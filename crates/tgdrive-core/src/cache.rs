use std::{
    fs,
    path::{Path, PathBuf},
};

use rusqlite::{params, Connection, OptionalExtension};
use time::OffsetDateTime;

use crate::{backend::RemoteObjectRef, format::Manifest};

#[derive(Debug)]
pub struct LocalStore {
    conn: Connection,
    cache_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct DirtyObject {
    pub object_id: u64,
    pub generation: u64,
    pub cache_path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct LocalStats {
    pub dirty_objects: usize,
    pub cache_entries: usize,
    pub wal_entries: usize,
}

impl LocalStore {
    pub fn open(
        sqlite_path: impl AsRef<Path>,
        cache_dir: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let cache_dir = cache_dir.as_ref().to_path_buf();
        fs::create_dir_all(cache_dir.join("objects"))?;
        if let Some(parent) = sqlite_path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(sqlite_path)?;
        let store = Self { conn, cache_dir };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS manifest_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                device_uuid TEXT NOT NULL,
                generation INTEGER NOT NULL,
                manifest_hash TEXT NOT NULL,
                manifest_json BLOB NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS objects (
                object_id INTEGER PRIMARY KEY,
                generation INTEGER NOT NULL,
                sha256 TEXT,
                zero INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS remote_refs (
                object_id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                generation INTEGER NOT NULL,
                sha256 TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dirty_objects (
                object_id INTEGER PRIMARY KEY,
                generation INTEGER NOT NULL,
                cache_path TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS wal_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_id INTEGER NOT NULL,
                offset INTEGER NOT NULL,
                len INTEGER NOT NULL,
                generation INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cache_entries (
                object_id INTEGER NOT NULL,
                generation INTEGER NOT NULL,
                cache_path TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                dirty INTEGER NOT NULL DEFAULT 0,
                last_access_at TEXT NOT NULL,
                PRIMARY KEY (object_id, generation)
            );
            ",
        )?;
        Ok(())
    }

    pub fn replace_from_manifest(&mut self, manifest: &Manifest) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM objects", [])?;
        tx.execute("DELETE FROM remote_refs", [])?;
        for object in &manifest.objects {
            tx.execute(
                "INSERT INTO objects (object_id, generation, sha256, zero) VALUES (?1, ?2, ?3, ?4)",
                params![
                    object.object_id as i64,
                    object.generation as i64,
                    object.sha256,
                    i64::from(object.zero)
                ],
            )?;
            if let Some(remote) = &object.remote {
                tx.execute(
                    "INSERT INTO remote_refs (object_id, chat_id, message_id, generation, sha256)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        object.object_id as i64,
                        remote.chat_id,
                        remote.message_id,
                        remote.generation as i64,
                        remote.sha256
                    ],
                )?;
            }
        }
        tx.execute(
            "INSERT OR REPLACE INTO manifest_state
             (id, device_uuid, generation, manifest_hash, manifest_json, updated_at)
             VALUES (1, ?1, ?2, ?3, ?4, ?5)",
            params![
                manifest.device_uuid.to_string(),
                manifest.generation as i64,
                manifest.hash_hex()?,
                manifest.encode_json()?,
                OffsetDateTime::now_utc().to_string()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn load_manifest(&self) -> anyhow::Result<Option<Manifest>> {
        let bytes: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT manifest_json FROM manifest_state WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        bytes.map(|bytes| Manifest::decode_json(&bytes)).transpose()
    }

    pub fn remote_ref(&self, object_id: u64) -> anyhow::Result<Option<RemoteObjectRef>> {
        self.conn
            .query_row(
                "SELECT chat_id, message_id, generation, sha256 FROM remote_refs WHERE object_id = ?1",
                [object_id as i64],
                |row| {
                    Ok(RemoteObjectRef {
                        chat_id: row.get(0)?,
                        message_id: row.get(1)?,
                        object_id,
                        generation: row.get::<_, i64>(2)? as u64,
                        sha256: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn cache_path(&self, object_id: u64, generation: u64) -> PathBuf {
        self.cache_dir
            .join("objects")
            .join(format!("{object_id:016x}-{generation:016x}.bin"))
    }

    pub fn read_cache(
        &self,
        object_id: u64,
        generation: u64,
        expected_sha: &str,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let path = self.cache_path(object_id, generation);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path)?;
        let actual = crate::format::sha256_hex(&bytes);
        anyhow::ensure!(
            actual == expected_sha,
            "cache checksum mismatch for object {object_id}"
        );
        self.conn.execute(
            "INSERT OR REPLACE INTO cache_entries
             (object_id, generation, cache_path, sha256, dirty, last_access_at)
             VALUES (?1, ?2, ?3, ?4, COALESCE((SELECT dirty FROM cache_entries WHERE object_id = ?1 AND generation = ?2), 0), ?5)",
            params![
                object_id as i64,
                generation as i64,
                path.to_string_lossy(),
                expected_sha,
                OffsetDateTime::now_utc().to_string()
            ],
        )?;
        Ok(Some(bytes))
    }

    pub fn write_cache(
        &self,
        object_id: u64,
        generation: u64,
        bytes: &[u8],
        dirty: bool,
    ) -> anyhow::Result<String> {
        let path = self.cache_path(object_id, generation);
        fs::write(&path, bytes)?;
        let sha = crate::format::sha256_hex(bytes);
        self.conn.execute(
            "INSERT OR REPLACE INTO cache_entries
             (object_id, generation, cache_path, sha256, dirty, last_access_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                object_id as i64,
                generation as i64,
                path.to_string_lossy(),
                sha,
                i64::from(dirty),
                OffsetDateTime::now_utc().to_string()
            ],
        )?;
        Ok(sha)
    }

    pub fn mark_dirty(
        &self,
        object_id: u64,
        generation: u64,
        cache_path: &Path,
        sha256: &str,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<()> {
        let now = OffsetDateTime::now_utc().to_string();
        self.conn.execute(
            "INSERT OR REPLACE INTO dirty_objects
             (object_id, generation, cache_path, sha256, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                object_id as i64,
                generation as i64,
                cache_path.to_string_lossy(),
                sha256,
                now
            ],
        )?;

        let existing = self.conn.query_row(
            "SELECT MIN(offset), MAX(offset + len)
                 FROM wal_entries
                 WHERE object_id = ?1
                   AND generation = ?2
                   AND offset <= ?4
                   AND offset + len >= ?3",
            params![
                object_id as i64,
                generation as i64,
                offset as i64,
                (offset + len) as i64
            ],
            |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
        )?;
        let merged = match existing {
            (Some(start), Some(end)) => (offset.min(start as u64), (offset + len).max(end as u64)),
            _ => (offset, offset + len),
        };
        self.conn.execute(
            "DELETE FROM wal_entries
             WHERE object_id = ?1
               AND generation = ?2
               AND offset <= ?4
               AND offset + len >= ?3",
            params![
                object_id as i64,
                generation as i64,
                merged.0 as i64,
                merged.1 as i64
            ],
        )?;
        self.conn.execute(
            "INSERT INTO wal_entries (object_id, offset, len, generation, sha256, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                object_id as i64,
                merged.0 as i64,
                (merged.1 - merged.0) as i64,
                generation as i64,
                sha256,
                now
            ],
        )?;
        self.flush()?;
        Ok(())
    }

    pub fn dirty_objects(&self) -> anyhow::Result<Vec<DirtyObject>> {
        let mut stmt = self.conn.prepare(
            "SELECT object_id, generation, cache_path, sha256 FROM dirty_objects ORDER BY object_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(DirtyObject {
                object_id: row.get::<_, i64>(0)? as u64,
                generation: row.get::<_, i64>(1)? as u64,
                cache_path: PathBuf::from(row.get::<_, String>(2)?),
                sha256: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn stats(&self) -> anyhow::Result<LocalStats> {
        Ok(LocalStats {
            dirty_objects: self.conn.query_row(
                "SELECT COUNT(*) FROM dirty_objects",
                [],
                |row| row.get(0),
            )?,
            cache_entries: self.conn.query_row(
                "SELECT COUNT(*) FROM cache_entries",
                [],
                |row| row.get(0),
            )?,
            wal_entries: self
                .conn
                .query_row("SELECT COUNT(*) FROM wal_entries", [], |row| row.get(0))?,
        })
    }

    #[cfg(test)]
    fn wal_ranges(&self) -> anyhow::Result<Vec<(u64, u64)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT offset, len FROM wal_entries ORDER BY offset")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn dirty_object(&self, object_id: u64) -> anyhow::Result<Option<DirtyObject>> {
        self.conn
            .query_row(
                "SELECT object_id, generation, cache_path, sha256 FROM dirty_objects WHERE object_id = ?1",
                [object_id as i64],
                |row| {
                    Ok(DirtyObject {
                        object_id: row.get::<_, i64>(0)? as u64,
                        generation: row.get::<_, i64>(1)? as u64,
                        cache_path: PathBuf::from(row.get::<_, String>(2)?),
                        sha256: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn clear_dirty_and_wal(&mut self) -> anyhow::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM dirty_objects", [])?;
        tx.execute("DELETE FROM wal_entries", [])?;
        tx.execute("UPDATE cache_entries SET dirty = 0", [])?;
        tx.commit()?;
        self.flush()?;
        Ok(())
    }

    pub fn evict_clean_lru(&self, max_entries: usize) -> anyhow::Result<usize> {
        let count: usize = self.conn.query_row(
            "SELECT COUNT(*) FROM cache_entries WHERE dirty = 0",
            [],
            |row| row.get(0),
        )?;
        if count <= max_entries {
            return Ok(0);
        }
        let to_remove = count - max_entries;
        let mut stmt = self.conn.prepare(
            "SELECT cache_path FROM cache_entries
             WHERE dirty = 0 ORDER BY last_access_at ASC LIMIT ?1",
        )?;
        let paths = stmt
            .query_map([to_remove as i64], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        for path in &paths {
            let _ = fs::remove_file(path);
        }
        self.conn.execute(
            "DELETE FROM cache_entries
             WHERE rowid IN (
                 SELECT rowid FROM cache_entries
                 WHERE dirty = 0 ORDER BY last_access_at ASC LIMIT ?1
             )",
            [to_remove as i64],
        )?;
        Ok(to_remove)
    }

    pub fn flush(&self) -> anyhow::Result<()> {
        self.conn.execute_batch("PRAGMA wal_checkpoint(FULL);")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn sqlite_rebuilds_from_manifest() {
        let temp = tempdir().unwrap();
        let mut store = LocalStore::open(temp.path().join("db.sqlite3"), temp.path()).unwrap();
        let manifest = Manifest::empty(Uuid::new_v4(), 256 * 1024, 2);
        store.replace_from_manifest(&manifest).unwrap();
        assert_eq!(store.load_manifest().unwrap().unwrap().object_count, 2);
    }

    #[test]
    fn does_not_evict_dirty_objects() {
        let temp = tempdir().unwrap();
        let store = LocalStore::open(temp.path().join("db.sqlite3"), temp.path()).unwrap();
        let sha = store.write_cache(1, 1, b"dirty", true).unwrap();
        let path = store.cache_path(1, 1);
        store.mark_dirty(1, 1, &path, &sha, 0, 5).unwrap();
        assert_eq!(store.evict_clean_lru(0).unwrap(), 0);
        assert!(path.exists());
    }

    #[test]
    fn coalesces_adjacent_wal_ranges() {
        let temp = tempdir().unwrap();
        let store = LocalStore::open(temp.path().join("db.sqlite3"), temp.path()).unwrap();
        let sha = store.write_cache(1, 1, b"dirty", true).unwrap();
        let path = store.cache_path(1, 1);
        store.mark_dirty(1, 1, &path, &sha, 0, 4).unwrap();
        store.mark_dirty(1, 1, &path, &sha, 4, 4).unwrap();
        store.mark_dirty(1, 1, &path, &sha, 16, 4).unwrap();
        assert_eq!(store.wal_ranges().unwrap(), vec![(0, 8), (16, 4)]);
    }
}
