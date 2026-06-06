use std::{env, fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required configuration: {0}")]
    Missing(&'static str),
    #[error("device size must be a multiple of logical sector size")]
    DeviceSizeAlignment,
    #[error("object size must be a multiple of logical sector size")]
    ObjectSizeAlignment,
    #[error("object size must be at least 64 KiB")]
    ObjectSizeTooSmall,
    #[error("local cache directory is required for read-write mode")]
    CacheRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TgDriveConfigFile {
    pub device_size: Option<u64>,
    pub logical_sector_size: Option<u32>,
    pub object_size: Option<u32>,
    pub cache_dir: Option<PathBuf>,
    pub sqlite_path: Option<PathBuf>,
    pub telegram_api_id: Option<i32>,
    pub telegram_api_hash: Option<String>,
    pub telegram_session_path: Option<PathBuf>,
    pub telegram_channel_id: Option<i64>,
    pub telegram_channel_access_hash: Option<i64>,
    pub telegram_channel_title: Option<String>,
    pub read_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TgDriveConfig {
    pub device_size: u64,
    pub logical_sector_size: u32,
    pub object_size: u32,
    pub cache_dir: PathBuf,
    pub sqlite_path: PathBuf,
    pub telegram_api_id: Option<i32>,
    pub telegram_api_hash: Option<String>,
    pub telegram_session_path: PathBuf,
    pub telegram_channel_id: Option<i64>,
    pub telegram_channel_access_hash: Option<i64>,
    pub telegram_channel_title: Option<String>,
    pub read_only: bool,
}

impl TgDriveConfig {
    pub fn load(
        config_path: Option<PathBuf>,
        read_only_override: Option<bool>,
    ) -> anyhow::Result<Self> {
        let mut file = if let Some(path) = config_path {
            let text = fs::read_to_string(path)?;
            toml::from_str::<TgDriveConfigFile>(&text)?
        } else {
            TgDriveConfigFile::default()
        };

        merge_env(&mut file)?;

        let cache_dir = file
            .cache_dir
            .or_else(default_cache_dir)
            .ok_or(ConfigError::Missing("cache_dir"))?;
        let sqlite_path = file
            .sqlite_path
            .unwrap_or_else(|| cache_dir.join("metadata.sqlite3"));
        let telegram_session_path = file
            .telegram_session_path
            .unwrap_or_else(|| cache_dir.join("telegram.session"));

        let cfg = TgDriveConfig {
            device_size: file.device_size.unwrap_or(64 * 1024 * 1024),
            logical_sector_size: file.logical_sector_size.unwrap_or(4096),
            object_size: file.object_size.unwrap_or(256 * 1024),
            cache_dir,
            sqlite_path,
            telegram_api_id: file.telegram_api_id,
            telegram_api_hash: file.telegram_api_hash,
            telegram_session_path,
            telegram_channel_id: file.telegram_channel_id,
            telegram_channel_access_hash: file.telegram_channel_access_hash,
            telegram_channel_title: file.telegram_channel_title,
            read_only: read_only_override.unwrap_or(file.read_only.unwrap_or(false)),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self
            .device_size
            .is_multiple_of(u64::from(self.logical_sector_size))
        {
            return Err(ConfigError::DeviceSizeAlignment);
        }
        if !self.object_size.is_multiple_of(self.logical_sector_size) {
            return Err(ConfigError::ObjectSizeAlignment);
        }
        if self.object_size < 64 * 1024 {
            return Err(ConfigError::ObjectSizeTooSmall);
        }
        if !self.read_only && self.cache_dir.as_os_str().is_empty() {
            return Err(ConfigError::CacheRequired);
        }
        Ok(())
    }

    pub fn object_count(&self) -> u64 {
        self.device_size.div_ceil(u64::from(self.object_size))
    }
}

fn merge_env(file: &mut TgDriveConfigFile) -> anyhow::Result<()> {
    file.device_size = env_u64("TGDRIVE_DEVICE_SIZE")?.or(file.device_size);
    file.logical_sector_size = env_u32("TGDRIVE_LOGICAL_SECTOR_SIZE")?.or(file.logical_sector_size);
    file.object_size = env_u32("TGDRIVE_OBJECT_SIZE")?.or(file.object_size);
    file.cache_dir = env_path("TGDRIVE_CACHE_DIR").or(file.cache_dir.take());
    file.sqlite_path = env_path("TGDRIVE_SQLITE_PATH").or(file.sqlite_path.take());
    file.telegram_api_id = env_i32("TELEGRAM_API_ID")?.or(file.telegram_api_id);
    file.telegram_api_hash = env::var("TELEGRAM_API_HASH")
        .ok()
        .or(file.telegram_api_hash.take());
    file.telegram_session_path =
        env_path("TELEGRAM_SESSION_PATH").or(file.telegram_session_path.take());
    file.telegram_channel_id = env_i64("TELEGRAM_CHANNEL_ID")?.or(file.telegram_channel_id);
    file.telegram_channel_access_hash =
        env_i64("TELEGRAM_CHANNEL_ACCESS_HASH")?.or(file.telegram_channel_access_hash);
    file.telegram_channel_title = env::var("TELEGRAM_CHANNEL_TITLE")
        .ok()
        .or(file.telegram_channel_title.take());
    file.read_only = env_bool("TGDRIVE_READ_ONLY")?.or(file.read_only);
    Ok(())
}

fn default_cache_dir() -> Option<PathBuf> {
    dirs::cache_dir().map(|dir| dir.join("tgdrive"))
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name).map(PathBuf::from)
}

fn env_bool(name: &str) -> anyhow::Result<Option<bool>> {
    Ok(match env::var(name) {
        Ok(value) => Some(matches!(
            value.as_str(),
            "1" | "true" | "TRUE" | "yes" | "YES"
        )),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err.into()),
    })
}

fn env_u64(name: &str) -> anyhow::Result<Option<u64>> {
    Ok(match env::var(name) {
        Ok(value) => Some(value.parse()?),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err.into()),
    })
}

fn env_u32(name: &str) -> anyhow::Result<Option<u32>> {
    Ok(match env::var(name) {
        Ok(value) => Some(value.parse()?),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err.into()),
    })
}

fn env_i32(name: &str) -> anyhow::Result<Option<i32>> {
    Ok(match env::var(name) {
        Ok(value) => Some(value.parse()?),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err.into()),
    })
}

fn env_i64(name: &str) -> anyhow::Result<Option<i64>> {
    Ok(match env::var(name) {
        Ok(value) => Some(value.parse()?),
        Err(env::VarError::NotPresent) => None,
        Err(err) => return Err(err.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_misaligned_device_size() {
        let cfg = TgDriveConfig {
            device_size: 1000,
            logical_sector_size: 4096,
            object_size: 256 * 1024,
            cache_dir: PathBuf::from("cache"),
            sqlite_path: PathBuf::from("cache/db.sqlite3"),
            telegram_api_id: None,
            telegram_api_hash: None,
            telegram_session_path: PathBuf::from("cache/session"),
            telegram_channel_id: None,
            telegram_channel_access_hash: None,
            telegram_channel_title: None,
            read_only: false,
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::DeviceSizeAlignment)
        ));
    }
}
