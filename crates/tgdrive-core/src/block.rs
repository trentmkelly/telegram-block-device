use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use tokio::sync::Mutex;

use crate::{
    backend::{ManifestCommitter, RemoteBackend},
    cache::LocalStore,
    format::{Manifest, ObjectEnvelope},
};

#[derive(Debug, Clone)]
pub struct BlockMap {
    pub device_size: u64,
    pub logical_sector_size: u32,
    pub object_size: u32,
}

impl BlockMap {
    pub fn new(
        device_size: u64,
        logical_sector_size: u32,
        object_size: u32,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            device_size.is_multiple_of(u64::from(logical_sector_size)),
            "device size must be sector aligned"
        );
        anyhow::ensure!(
            object_size.is_multiple_of(logical_sector_size),
            "object size must be sector aligned"
        );
        Ok(Self {
            device_size,
            logical_sector_size,
            object_size,
        })
    }

    pub fn object_count(&self) -> u64 {
        self.device_size.div_ceil(u64::from(self.object_size))
    }

    pub fn locate(&self, offset: u64) -> anyhow::Result<(u64, usize)> {
        anyhow::ensure!(offset < self.device_size, "offset out of bounds");
        Ok((
            offset / u64::from(self.object_size),
            (offset % u64::from(self.object_size)) as usize,
        ))
    }
}

pub struct BlockDevice<B: RemoteBackend> {
    pub map: BlockMap,
    pub backend: Arc<B>,
    pub store: Arc<Mutex<LocalStore>>,
    pub manifest: Arc<Mutex<Manifest>>,
    metrics: Arc<DeviceMetrics>,
    committer: Option<Arc<dyn ManifestCommitter>>,
    max_dirty_objects: usize,
    read_ahead_objects: u64,
}

impl<B: RemoteBackend> BlockDevice<B> {
    pub fn new(map: BlockMap, backend: Arc<B>, store: LocalStore, manifest: Manifest) -> Self {
        Self {
            map,
            backend,
            store: Arc::new(Mutex::new(store)),
            manifest: Arc::new(Mutex::new(manifest)),
            metrics: Arc::new(DeviceMetrics::default()),
            committer: None,
            max_dirty_objects: 4096,
            read_ahead_objects: 4,
        }
    }

    pub fn metrics(&self) -> DeviceMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn with_committer(mut self, committer: Arc<dyn ManifestCommitter>) -> Self {
        self.committer = Some(committer);
        self
    }

    pub async fn read_at(&self, offset: u64, len: usize) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(
            offset + len as u64 <= self.map.device_size,
            "read out of bounds"
        );
        let mut remaining = len;
        let mut cursor = offset;
        let mut out = Vec::with_capacity(len);
        while remaining > 0 {
            let (object_id, object_offset) = self.map.locate(cursor)?;
            let take = remaining.min(self.map.object_size as usize - object_offset);
            let object = self.read_object(object_id).await?;
            out.extend_from_slice(&object[object_offset..object_offset + take]);
            remaining -= take;
            cursor += take as u64;
        }
        if cursor < self.map.device_size {
            if let Ok((next_object_id, 0)) = self.map.locate(cursor) {
                let _ = self
                    .prefetch_objects(next_object_id, self.read_ahead_objects)
                    .await;
            }
        }
        Ok(out)
    }

    pub async fn write_at(&self, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(
            offset + bytes.len() as u64 <= self.map.device_size,
            "write out of bounds"
        );
        let mut remaining = bytes.len();
        let mut cursor = offset;
        let mut input_offset = 0;
        while remaining > 0 {
            let (object_id, object_offset) = self.map.locate(cursor)?;
            let take = remaining.min(self.map.object_size as usize - object_offset);
            let full_object = object_offset == 0 && take == self.map.object_size as usize;
            let mut object = if full_object {
                vec![0; self.map.object_size as usize]
            } else {
                self.read_object(object_id).await?
            };
            object[object_offset..object_offset + take]
                .copy_from_slice(&bytes[input_offset..input_offset + take]);
            self.write_dirty_object(object_id, object, object_offset as u64, take as u64)
                .await?;
            remaining -= take;
            input_offset += take;
            cursor += take as u64;
        }
        Ok(())
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let dirty = { self.store.lock().await.dirty_objects()? };
        if dirty.is_empty() {
            return Ok(());
        }

        let mut manifest = self.manifest.lock().await;
        let new_generation = dirty
            .iter()
            .map(|object| object.generation)
            .max()
            .unwrap_or(manifest.generation + 1);
        manifest.generation = new_generation;

        for dirty in dirty {
            let encoded = tokio::fs::read(&dirty.cache_path).await?;
            ObjectEnvelope::decode(&encoded, dirty.object_id, dirty.generation)?;
            let sha = crate::format::sha256_hex(&encoded);
            anyhow::ensure!(sha == dirty.sha256, "dirty cache checksum mismatch");
            let started = Instant::now();
            let remote = self
                .backend
                .send_object(dirty.object_id, dirty.generation, encoded, sha.clone())
                .await?;
            self.metrics
                .record_upload(started.elapsed().as_millis() as u64);
            let idx = manifest
                .objects
                .iter()
                .position(|object| object.object_id == dirty.object_id)
                .ok_or_else(|| anyhow::anyhow!("manifest missing object {}", dirty.object_id))?;
            if let Some(old) = manifest.objects[idx].remote.take() {
                manifest.garbage.push(old);
            }
            let entry = &mut manifest.objects[idx];
            entry.generation = dirty.generation;
            entry.remote = Some(remote);
            entry.sha256 = Some(sha);
            entry.zero = false;
        }

        if let Some(committer) = &self.committer {
            committer
                .commit_manifest(
                    &manifest,
                    self.map.device_size,
                    self.map.logical_sector_size,
                    self.map.object_size,
                )
                .await?;
        }

        let mut store = self.store.lock().await;
        store.replace_from_manifest(&manifest)?;
        store.clear_dirty_and_wal()?;
        Ok(())
    }

    async fn read_object(&self, object_id: u64) -> anyhow::Result<Vec<u8>> {
        let object_size = self.map.object_size as usize;
        if let Some(dirty) = self.store.lock().await.dirty_object(object_id)? {
            let encoded = tokio::fs::read(&dirty.cache_path).await?;
            anyhow::ensure!(
                crate::format::sha256_hex(&encoded) == dirty.sha256,
                "dirty cache checksum mismatch for object {object_id}"
            );
            let envelope = ObjectEnvelope::decode(&encoded, object_id, dirty.generation)?;
            return Ok(pad_object(envelope.payload, object_size));
        }

        let manifest = self.manifest.lock().await;
        let entry = manifest
            .objects
            .iter()
            .find(|object| object.object_id == object_id)
            .ok_or_else(|| anyhow::anyhow!("object {object_id} out of manifest range"))?
            .clone();
        drop(manifest);

        if entry.zero {
            return Ok(vec![0; object_size]);
        }
        let remote = entry
            .remote
            .ok_or_else(|| anyhow::anyhow!("object {object_id} has no remote ref"))?;
        let sha = entry
            .sha256
            .ok_or_else(|| anyhow::anyhow!("object {object_id} has no checksum"))?;

        if let Some(cached) =
            self.store
                .lock()
                .await
                .read_cache(object_id, entry.generation, &sha)?
        {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            let envelope = ObjectEnvelope::decode(&cached, object_id, entry.generation)?;
            return Ok(pad_object(envelope.payload, object_size));
        }

        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let encoded = self.backend.fetch_object(&remote).await?;
        self.metrics
            .record_download(started.elapsed().as_millis() as u64);
        anyhow::ensure!(
            crate::format::sha256_hex(&encoded) == sha,
            "remote checksum mismatch for object {object_id}"
        );
        self.store
            .lock()
            .await
            .write_cache(object_id, entry.generation, &encoded, false)?;
        let envelope = ObjectEnvelope::decode(&encoded, object_id, entry.generation)?;
        Ok(pad_object(envelope.payload, object_size))
    }

    async fn prefetch_objects(&self, start_object_id: u64, count: u64) -> anyhow::Result<()> {
        let dirty_ids = {
            let store = self.store.lock().await;
            store
                .dirty_objects()?
                .into_iter()
                .map(|dirty| dirty.object_id)
                .collect::<BTreeSet<_>>()
        };
        let manifest = self.manifest.lock().await;
        let mut entries = Vec::new();
        for object_id in start_object_id..start_object_id.saturating_add(count) {
            if dirty_ids.contains(&object_id) {
                continue;
            }
            let Some(entry) = manifest
                .objects
                .iter()
                .find(|object| object.object_id == object_id)
                .cloned()
            else {
                break;
            };
            if entry.zero {
                continue;
            }
            let (Some(remote), Some(sha)) = (entry.remote.clone(), entry.sha256.clone()) else {
                continue;
            };
            if self
                .store
                .lock()
                .await
                .read_cache(object_id, entry.generation, &sha)?
                .is_some()
            {
                self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            entries.push((object_id, entry.generation, sha, remote));
        }
        drop(manifest);

        if entries.is_empty() {
            return Ok(());
        }

        self.metrics
            .cache_misses
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let ids = entries
            .iter()
            .map(|(_, _, _, remote)| remote.message_id)
            .collect::<Vec<_>>();
        let started = Instant::now();
        let messages = self.backend.get_messages_by_id(&ids).await?;
        self.metrics
            .record_download(started.elapsed().as_millis() as u64);

        for ((object_id, generation, sha, _), encoded) in entries.into_iter().zip(messages) {
            let Some(encoded) = encoded else {
                continue;
            };
            if crate::format::sha256_hex(&encoded) != sha {
                continue;
            }
            if ObjectEnvelope::decode(&encoded, object_id, generation).is_ok() {
                self.store
                    .lock()
                    .await
                    .write_cache(object_id, generation, &encoded, false)?;
            }
        }
        Ok(())
    }

    async fn write_dirty_object(
        &self,
        object_id: u64,
        mut object: Vec<u8>,
        offset: u64,
        len: u64,
    ) -> anyhow::Result<()> {
        object.resize(self.map.object_size as usize, 0);
        let dirty_count = self.store.lock().await.stats()?.dirty_objects;
        anyhow::ensure!(
            dirty_count < self.max_dirty_objects,
            "dirty object backpressure limit reached"
        );
        let generation = {
            let manifest = self.manifest.lock().await;
            manifest.generation + 1
        };
        let envelope = ObjectEnvelope {
            object_id,
            generation,
            object_size: self.map.object_size,
            flags: 0,
            payload: object,
        };
        let encoded = envelope.encode();
        let store = self.store.lock().await;
        let sha = store.write_cache(object_id, generation, &encoded, true)?;
        let path = store.cache_path(object_id, generation);
        store.mark_dirty(object_id, generation, &path, &sha, offset, len)?;
        Ok(())
    }
}

fn pad_object(mut payload: Vec<u8>, object_size: usize) -> Vec<u8> {
    payload.resize(object_size, 0);
    payload
}

#[derive(Debug, Default)]
struct DeviceMetrics {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    uploads: AtomicU64,
    upload_ms_total: AtomicU64,
    downloads: AtomicU64,
    download_ms_total: AtomicU64,
}

impl DeviceMetrics {
    fn record_upload(&self, ms: u64) {
        self.uploads.fetch_add(1, Ordering::Relaxed);
        self.upload_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    fn record_download(&self, ms: u64) {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        self.download_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    fn snapshot(&self) -> DeviceMetricsSnapshot {
        let uploads = self.uploads.load(Ordering::Relaxed);
        let downloads = self.downloads.load(Ordering::Relaxed);
        DeviceMetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            uploads,
            avg_upload_ms: avg(self.upload_ms_total.load(Ordering::Relaxed), uploads),
            downloads,
            avg_download_ms: avg(self.download_ms_total.load(Ordering::Relaxed), downloads),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeviceMetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub uploads: u64,
    pub avg_upload_ms: f64,
    pub downloads: u64,
    pub avg_download_ms: f64,
}

fn avg(total: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use tempfile::tempdir;
    use uuid::Uuid;

    use crate::{Manifest, ManifestCommitter, MemoryBackend};

    use super::*;

    #[test]
    fn maps_offsets_to_objects() {
        let map = BlockMap::new(1024 * 1024, 4096, 256 * 1024).unwrap();
        assert_eq!(map.locate(0).unwrap(), (0, 0));
        assert_eq!(map.locate(256 * 1024).unwrap(), (1, 0));
        assert_eq!(map.locate(256 * 1024 + 7).unwrap(), (1, 7));
    }

    #[tokio::test]
    async fn partial_write_round_trip_across_objects() {
        let temp = tempdir().unwrap();
        let store = LocalStore::open(temp.path().join("db.sqlite3"), temp.path()).unwrap();
        let map = BlockMap::new(512 * 1024, 4096, 256 * 1024).unwrap();
        let manifest = Manifest::empty(Uuid::new_v4(), 256 * 1024, 2);
        let device = BlockDevice::new(map, Arc::new(MemoryBackend::new(-100)), store, manifest);

        let offset = 256 * 1024 - 2;
        device.write_at(offset, b"abcd").await.unwrap();
        assert_eq!(device.read_at(offset, 4).await.unwrap(), b"abcd");
        device.flush().await.unwrap();
        assert_eq!(device.read_at(offset, 4).await.unwrap(), b"abcd");
    }

    #[tokio::test]
    async fn full_object_write_does_not_need_read_before_write() {
        let temp = tempdir().unwrap();
        let store = LocalStore::open(temp.path().join("db.sqlite3"), temp.path()).unwrap();
        let map = BlockMap::new(256 * 1024, 4096, 256 * 1024).unwrap();
        let manifest = Manifest::empty(Uuid::new_v4(), 256 * 1024, 1);
        let device = BlockDevice::new(map, Arc::new(MemoryBackend::new(-100)), store, manifest);
        let bytes = vec![7u8; 256 * 1024];
        device.write_at(0, &bytes).await.unwrap();
        assert_eq!(device.read_at(0, bytes.len()).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn interrupted_manifest_commit_can_retry() {
        let temp = tempdir().unwrap();
        let store = LocalStore::open(temp.path().join("db.sqlite3"), temp.path()).unwrap();
        let map = BlockMap::new(256 * 1024, 4096, 256 * 1024).unwrap();
        let manifest = Manifest::empty(Uuid::new_v4(), 256 * 1024, 1);
        let committer = Arc::new(FailOnceCommitter::default());
        let device = BlockDevice::new(map, Arc::new(MemoryBackend::new(-100)), store, manifest)
            .with_committer(committer);

        device.write_at(0, b"survives").await.unwrap();
        assert!(device.flush().await.is_err());
        assert_eq!(device.store.lock().await.stats().unwrap().dirty_objects, 1);

        device.flush().await.unwrap();
        assert_eq!(device.store.lock().await.stats().unwrap().dirty_objects, 0);
        assert_eq!(device.read_at(0, 8).await.unwrap(), b"survives");
    }

    #[derive(Default)]
    struct FailOnceCommitter {
        failed: AtomicBool,
    }

    #[async_trait]
    impl ManifestCommitter for FailOnceCommitter {
        async fn commit_manifest(
            &self,
            _manifest: &Manifest,
            _device_size: u64,
            _logical_sector_size: u32,
            _object_size: u32,
        ) -> anyhow::Result<()> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                anyhow::bail!("simulated interrupted commit");
            }
            Ok(())
        }
    }
}
