use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand};
use tgdrive_core::{
    format::MANIFEST_OBJECT_ID, BlockDevice, BlockMap, LocalStore, Manifest, ManifestCommitter,
    MemoryBackend, RemoteObjectRef, Superblock, TgDriveConfig,
};
use tgdrive_nbd::{NbdExport, NbdServerOptions};
use tgdrive_telegram::{SignInError, TelegramClientConfig, TelegramStorage};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "tgdrive")]
#[command(about = "Expose a Linux NBD block device backed by Telegram object storage.")]
struct Cli {
    #[arg(long, env = "TGDRIVE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long)]
    read_only: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Login,
    ResolveChannel,
    RecoverFromRemote,
    Format {
        #[arg(long)]
        size: Option<String>,
        #[arg(long)]
        object_size: Option<String>,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        local_only: bool,
    },
    ServeNbd {
        #[arg(long, default_value = "127.0.0.1:10809")]
        bind: String,
        #[arg(long, default_value = "tgdrive")]
        export_name: String,
        #[arg(long, default_value = "telegram")]
        backend: BackendChoice,
    },
    Status,
    Flush,
    Fsck {
        #[arg(long)]
        all: bool,
    },
    Gc {
        #[arg(long)]
        yes: bool,
    },
    Recover,
    Bench {
        #[arg(long, default_value = "64MiB")]
        size: String,
        #[arg(long, default_value = "256KiB")]
        object_size: String,
        #[arg(long, default_value_t = 32)]
        ops: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let mut cfg = TgDriveConfig::load(cli.config, Some(cli.read_only))?;

    match cli.command {
        Command::Login => {
            let login = TelegramStorage::connect_for_login(telegram_config(&cfg)?).await?;
            if login.is_authorized().await? {
                println!("already authorized");
                return Ok(());
            }
            let phone = prompt("phone number")?;
            let token = login.request_login_code(&phone).await?;
            let code = prompt("login code")?;
            match login.sign_in(&token, &code).await {
                Ok(()) => println!("login complete"),
                Err(SignInError::PasswordRequired(password_token)) => {
                    let password = prompt("2FA password")?;
                    login.check_password(password_token, &password).await?;
                    println!("login complete");
                }
                Err(err) => return Err(err.into()),
            }
        }
        Command::ResolveChannel => {
            let storage = TelegramStorage::connect(telegram_config(&cfg)?).await?;
            let resolved = storage.resolve_channel().await?;
            upsert_env(
                ".env",
                "TELEGRAM_CHANNEL_ID",
                &resolved.bot_api_chat_id.to_string(),
            )?;
            upsert_env(
                ".env",
                "TELEGRAM_CHANNEL_ACCESS_HASH",
                &resolved.access_hash.to_string(),
            )?;
            if let Some(title) = resolved.title {
                upsert_env(".env", "TELEGRAM_CHANNEL_TITLE", &title)?;
            }
            println!(
                "resolved channel: bot_api_chat_id={} bare_channel_id={}",
                resolved.bot_api_chat_id, resolved.bare_channel_id
            );
        }
        Command::RecoverFromRemote => {
            let storage = TelegramStorage::connect(telegram_config(&cfg)?).await?;
            let (_superblock, manifest) = recover_remote_manifest(&storage, &cfg).await?;
            let mut store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            store.replace_from_manifest(&manifest)?;
            println!(
                "recovered remote manifest: uuid={} generation={} objects={}",
                manifest.device_uuid, manifest.generation, manifest.object_count
            );
        }
        Command::Format {
            size,
            object_size,
            force,
            local_only,
        } => {
            if let Some(size) = size {
                cfg.device_size = parse_size(&size)?;
            }
            if let Some(object_size) = object_size {
                cfg.object_size = parse_size(&object_size)? as u32;
            }
            cfg.validate()?;
            if cfg.sqlite_path.exists() && !force {
                anyhow::bail!(
                    "{} already exists; pass --force to replace local metadata",
                    cfg.sqlite_path.display()
                );
            }
            let mut store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            let manifest = Manifest::empty(Uuid::new_v4(), cfg.object_size, cfg.object_count());
            if !local_only {
                let storage = TelegramStorage::connect(telegram_config(&cfg)?).await?;
                storage
                    .commit_manifest(
                        &manifest,
                        cfg.device_size,
                        cfg.logical_sector_size,
                        cfg.object_size,
                    )
                    .await?;
            }
            store.replace_from_manifest(&manifest)?;
            println!(
                "formatted local TGDrive metadata: uuid={} size={} object_size={} objects={}",
                manifest.device_uuid, cfg.device_size, cfg.object_size, manifest.object_count
            );
        }
        Command::ServeNbd {
            bind,
            export_name,
            backend,
        } => {
            let _lock = MountLock::acquire(&cfg.cache_dir)?;
            match backend {
                BackendChoice::Local => {
                    let store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
                    let manifest = store.load_manifest()?.ok_or_else(|| {
                        anyhow::anyhow!("no local manifest; run tgdrive format first")
                    })?;
                    let map =
                        BlockMap::new(cfg.device_size, cfg.logical_sector_size, cfg.object_size)?;
                    let backend =
                        Arc::new(MemoryBackend::new(cfg.telegram_channel_id.unwrap_or(-100)));
                    let device = Arc::new(BlockDevice::new(map, backend, store, manifest));
                    let nbd = NbdExport::new(nbd_options(bind, export_name, &cfg), device);
                    nbd.serve().await?;
                }
                BackendChoice::Telegram => {
                    let backend = Arc::new(TelegramStorage::connect(telegram_config(&cfg)?).await?);
                    let mut store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
                    let manifest = if let Some(manifest) = store.load_manifest()? {
                        if let Some(text) = backend.recover_superblock_text(200).await? {
                            let remote_superblock = Superblock::decode_text(&text)?;
                            anyhow::ensure!(
                                remote_superblock.device_uuid == manifest.device_uuid,
                                "local manifest UUID does not match remote channel superblock"
                            );
                        }
                        manifest
                    } else {
                        let (_superblock, manifest) =
                            recover_remote_manifest(&backend, &cfg).await?;
                        store.replace_from_manifest(&manifest)?;
                        manifest
                    };
                    let map =
                        BlockMap::new(cfg.device_size, cfg.logical_sector_size, cfg.object_size)?;
                    let committer: Arc<dyn ManifestCommitter> = backend.clone();
                    let device = Arc::new(
                        BlockDevice::new(map, backend, store, manifest).with_committer(committer),
                    );
                    let nbd = NbdExport::new(nbd_options(bind, export_name, &cfg), device);
                    nbd.serve().await?;
                }
            }
        }
        Command::Status => {
            let store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            let stats = store.stats()?;
            let manifest = store.load_manifest()?;
            println!("cache_dir={}", cfg.cache_dir.display());
            println!("sqlite_path={}", cfg.sqlite_path.display());
            println!("device_size={}", cfg.device_size);
            println!("object_size={}", cfg.object_size);
            println!("dirty_objects={}", stats.dirty_objects);
            println!("cache_entries={}", stats.cache_entries);
            println!("wal_entries={}", stats.wal_entries);
            if let Some(manifest) = manifest {
                println!("manifest_generation={}", manifest.generation);
                println!("device_uuid={}", manifest.device_uuid);
            } else {
                println!("manifest=missing");
            }
        }
        Command::Flush => {
            let mut store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            let manifest = store
                .load_manifest()?
                .ok_or_else(|| anyhow::anyhow!("no local manifest; run tgdrive format first"))?;
            let map = BlockMap::new(cfg.device_size, cfg.logical_sector_size, cfg.object_size)?;
            let backend = Arc::new(MemoryBackend::new(cfg.telegram_channel_id.unwrap_or(-100)));
            let device = BlockDevice::new(map, backend, store, manifest);
            device.flush().await?;
            store = Arc::try_unwrap(device.store)
                .map_err(|_| anyhow::anyhow!("store still shared"))?
                .into_inner();
            store.flush()?;
            println!("flushed local dirty objects through configured backend");
        }
        Command::Fsck { all: _ } => {
            let store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            let manifest = store
                .load_manifest()?
                .ok_or_else(|| anyhow::anyhow!("no local manifest; run tgdrive format first"))?;
            let hash = manifest.hash_hex()?;
            manifest.verify_hash(&hash)?;
            anyhow::ensure!(
                manifest.object_count == cfg.object_count(),
                "manifest object count does not match config"
            );
            println!(
                "fsck ok: uuid={} generation={} objects={} hash={}",
                manifest.device_uuid, manifest.generation, manifest.object_count, hash
            );
        }
        Command::Gc { yes } => {
            if !yes {
                anyhow::bail!(
                    "live GC deletes Telegram messages; rerun with --yes after reviewing garbage"
                )
            }
            let store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            let manifest = store
                .load_manifest()?
                .ok_or_else(|| anyhow::anyhow!("no local manifest; run tgdrive format first"))?;
            let ids = manifest
                .garbage
                .iter()
                .map(|reference| reference.message_id)
                .collect::<Vec<_>>();
            if ids.is_empty() {
                println!("no remote garbage in current manifest");
                return Ok(());
            }
            let storage = TelegramStorage::connect(telegram_config(&cfg)?).await?;
            let deleted = storage.delete_messages(&ids).await?;
            println!("deleted {deleted} remote garbage messages");
        }
        Command::Recover => {
            let storage = TelegramStorage::connect(telegram_config(&cfg)?).await?;
            let text = storage
                .recover_superblock_text(200)
                .await?
                .ok_or_else(|| anyhow::anyhow!("no TGDrive superblock found"))?;
            let current = Superblock::decode_text(&text)?;
            let previous_message_id = current
                .previous_manifest_message_id
                .ok_or_else(|| anyhow::anyhow!("superblock has no previous manifest"))?;
            let previous_hash = current
                .previous_manifest_hash
                .clone()
                .ok_or_else(|| anyhow::anyhow!("superblock has no previous manifest hash"))?;
            let previous_generation = current
                .previous_manifest_generation
                .ok_or_else(|| anyhow::anyhow!("superblock has no previous manifest generation"))?;
            let previous_ref = RemoteObjectRef {
                chat_id: cfg.telegram_channel_id.unwrap_or_default(),
                message_id: previous_message_id,
                object_id: MANIFEST_OBJECT_ID,
                generation: previous_generation,
                sha256: previous_hash,
            };
            let previous_superblock = Superblock {
                current_manifest_message_id: previous_ref.message_id,
                manifest_hash: previous_ref.sha256.clone(),
                manifest_generation: previous_ref.generation,
                previous_manifest_message_id: None,
                previous_manifest_hash: None,
                previous_manifest_generation: None,
                ..current.clone()
            };
            let manifest = storage
                .fetch_manifest_from_superblock(&previous_superblock)
                .await?;
            let rollback_superblock = Superblock::for_manifest(
                manifest.device_uuid,
                current.device_size,
                current.logical_sector_size,
                current.object_size,
                &previous_ref,
                Some(&current),
            );
            storage
                .upsert_pinned_superblock(&rollback_superblock.encode_text()?)
                .await?;
            let mut store = LocalStore::open(&cfg.sqlite_path, &cfg.cache_dir)?;
            store.replace_from_manifest(&manifest)?;
            println!("rolled back to manifest generation {}", manifest.generation);
        }
        Command::Bench {
            size,
            object_size,
            ops,
        } => {
            run_bench(&cfg, &size, &object_size, ops).await?;
        }
    }
    Ok(())
}

fn parse_size(input: &str) -> anyhow::Result<u64> {
    let trimmed = input.trim();
    let (number, multiplier) = if let Some(number) = trimmed.strip_suffix("MiB") {
        (number, 1024 * 1024)
    } else if let Some(number) = trimmed.strip_suffix("GiB") {
        (number, 1024 * 1024 * 1024)
    } else if let Some(number) = trimmed.strip_suffix("KiB") {
        (number, 1024)
    } else if let Some(number) = trimmed.strip_suffix('M') {
        (number, 1000 * 1000)
    } else if let Some(number) = trimmed.strip_suffix('G') {
        (number, 1000 * 1000 * 1000)
    } else if let Some(number) = trimmed.strip_suffix('K') {
        (number, 1000)
    } else {
        (trimmed, 1)
    };
    Ok(number.trim().parse::<u64>()? * multiplier)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum BackendChoice {
    Telegram,
    Local,
}

async fn recover_remote_manifest(
    storage: &TelegramStorage,
    cfg: &TgDriveConfig,
) -> anyhow::Result<(Superblock, Manifest)> {
    let superblock_text = storage
        .recover_superblock_text(200)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no TGDrive superblock found"))?;
    let superblock = Superblock::decode_text(&superblock_text)?;
    anyhow::ensure!(
        superblock.device_size == cfg.device_size,
        "remote device size does not match local config"
    );
    anyhow::ensure!(
        superblock.object_size == cfg.object_size,
        "remote object size does not match local config"
    );
    let manifest = match storage.fetch_manifest_from_superblock(&superblock).await {
        Ok(manifest) => manifest,
        Err(current_error) => {
            let Some(previous_message_id) = superblock.previous_manifest_message_id else {
                return Err(current_error.context("latest manifest verification failed"));
            };
            let previous_hash = superblock
                .previous_manifest_hash
                .clone()
                .ok_or_else(|| anyhow::anyhow!("previous manifest hash missing"))?;
            let previous_generation = superblock
                .previous_manifest_generation
                .ok_or_else(|| anyhow::anyhow!("previous manifest generation missing"))?;
            eprintln!(
                "latest manifest verification failed; recovering previous generation {}",
                previous_generation
            );
            let previous_superblock = Superblock {
                current_manifest_message_id: previous_message_id,
                manifest_hash: previous_hash,
                manifest_generation: previous_generation,
                previous_manifest_message_id: None,
                previous_manifest_hash: None,
                previous_manifest_generation: None,
                ..superblock.clone()
            };
            storage
                .fetch_manifest_from_superblock(&previous_superblock)
                .await?
        }
    };
    anyhow::ensure!(
        manifest.device_uuid == superblock.device_uuid,
        "remote manifest UUID does not match superblock"
    );
    Ok((superblock, manifest))
}

fn telegram_config(cfg: &TgDriveConfig) -> anyhow::Result<TelegramClientConfig> {
    Ok(TelegramClientConfig {
        api_id: cfg
            .telegram_api_id
            .ok_or_else(|| anyhow::anyhow!("missing TELEGRAM_API_ID"))?,
        api_hash: cfg
            .telegram_api_hash
            .clone()
            .ok_or_else(|| anyhow::anyhow!("missing TELEGRAM_API_HASH"))?,
        session_path: cfg.telegram_session_path.clone(),
        channel_id: cfg
            .telegram_channel_id
            .ok_or_else(|| anyhow::anyhow!("missing TELEGRAM_CHANNEL_ID"))?,
        channel_access_hash: cfg.telegram_channel_access_hash,
        channel_title: cfg.telegram_channel_title.clone(),
    })
}

fn nbd_options(bind: String, export_name: String, cfg: &TgDriveConfig) -> NbdServerOptions {
    NbdServerOptions {
        bind,
        export_name,
        device_size: cfg.device_size,
        logical_block_size: cfg.logical_sector_size,
        max_inflight: 64,
    }
}

fn prompt(label: &str) -> anyhow::Result<String> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

async fn run_bench(
    cfg: &TgDriveConfig,
    size: &str,
    object_size: &str,
    ops: usize,
) -> anyhow::Result<()> {
    anyhow::ensure!(ops > 0, "--ops must be greater than zero");
    let device_size = parse_size(size)?;
    let object_size = parse_size(object_size)? as u32;
    let map = BlockMap::new(device_size, cfg.logical_sector_size, object_size)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let bench_dir =
        std::env::temp_dir().join(format!("tgdrive-bench-{}-{now}", std::process::id()));
    fs::create_dir_all(&bench_dir)?;

    let result = async {
        let store = LocalStore::open(bench_dir.join("bench.sqlite3"), &bench_dir)?;
        let manifest = Manifest::empty(Uuid::new_v4(), object_size, map.object_count());
        let device = BlockDevice::new(
            map.clone(),
            Arc::new(MemoryBackend::new(-100)),
            store,
            manifest,
        );
        let object = deterministic_bytes(object_size as usize, 0x54474452495645);
        let random_write = deterministic_bytes(cfg.logical_sector_size as usize, 0x52414e444f4d);

        let seq_write = time_async(async {
            let mut offset = 0;
            while offset < device_size {
                let take = (device_size - offset).min(u64::from(object_size)) as usize;
                device.write_at(offset, &object[..take]).await?;
                offset += take as u64;
            }
            anyhow::Ok(())
        })
        .await?;

        let flush = time_async(device.flush()).await?;

        let seq_read = time_async(async {
            let mut offset = 0;
            while offset < device_size {
                let take = (device_size - offset).min(u64::from(object_size)) as usize;
                let _ = device.read_at(offset, take).await?;
                offset += take as u64;
            }
            anyhow::Ok(())
        })
        .await?;

        let max_sector = device_size / u64::from(cfg.logical_sector_size);
        let random_read = time_async(async {
            for index in 0..ops {
                let sector = pseudo_random_index(index as u64, max_sector);
                let _ = device
                    .read_at(
                        sector * u64::from(cfg.logical_sector_size),
                        cfg.logical_sector_size as usize,
                    )
                    .await?;
            }
            anyhow::Ok(())
        })
        .await?;

        let random_write_elapsed = time_async(async {
            for index in 0..ops {
                let sector = pseudo_random_index(index as u64 + 0x1000, max_sector);
                device
                    .write_at(sector * u64::from(cfg.logical_sector_size), &random_write)
                    .await?;
            }
            anyhow::Ok(())
        })
        .await?;

        let final_flush = time_async(device.flush()).await?;
        let metrics = device.metrics();
        println!("bench backend=memory size={device_size} object_size={object_size} ops={ops}");
        println!("sequential_write_ms={}", seq_write.as_millis());
        println!("first_flush_ms={}", flush.as_millis());
        println!("sequential_read_ms={}", seq_read.as_millis());
        println!("random_read_ms={}", random_read.as_millis());
        println!("random_write_ms={}", random_write_elapsed.as_millis());
        println!("final_flush_ms={}", final_flush.as_millis());
        println!(
            "cache_hits={} cache_misses={} uploads={} downloads={}",
            metrics.cache_hits, metrics.cache_misses, metrics.uploads, metrics.downloads
        );
        anyhow::Ok(())
    }
    .await;

    let cleanup = fs::remove_dir_all(&bench_dir);
    if let Err(err) = cleanup {
        eprintln!("warning: failed to remove {}: {err}", bench_dir.display());
    }
    result
}

async fn time_async<F, T>(future: F) -> anyhow::Result<std::time::Duration>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    let started = Instant::now();
    future.await?;
    Ok(started.elapsed())
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn pseudo_random_index(index: u64, modulus: u64) -> u64 {
    if modulus == 0 {
        return 0;
    }
    index.wrapping_mul(6364136223846793005).wrapping_add(1) % modulus
}

fn upsert_env(path: &str, key: &str, value: &str) -> anyhow::Result<()> {
    let existing = fs::read_to_string(path).unwrap_or_default();
    let mut found = false;
    let mut lines = Vec::new();
    for line in existing.lines() {
        if line.starts_with(&format!("{key}=")) {
            lines.push(format!("{key}={value}"));
            found = true;
        } else {
            lines.push(line.to_string());
        }
    }
    if !found {
        lines.push(format!("{key}={value}"));
    }
    fs::write(path, format!("{}\n", lines.join("\n")))?;
    Ok(())
}

struct MountLock {
    path: PathBuf,
    _file: File,
}

impl MountLock {
    fn acquire(cache_dir: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(cache_dir)?;
        let path = cache_dir.join("mount.lock");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to acquire mount lock at {}: {err}; another tgdrive may be running",
                    path.display()
                )
            })?;
        Ok(Self { path, _file: file })
    }
}

impl Drop for MountLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
