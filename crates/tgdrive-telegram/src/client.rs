use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::future::BoxFuture;
use grammers_client::{
    client::{AutoSleep, ClientConfiguration, LoginToken, PasswordToken},
    message::{InputMessage, Message},
    Client, InvocationError, SignInError,
};
use grammers_mtsender::{SenderPool, SenderPoolFatHandle};
use grammers_session::{
    types::{
        ChannelState, DcOption, PeerAuth, PeerId, PeerInfo, PeerRef, UpdateState, UpdatesState,
    },
    Session, SessionData,
};
use tgdrive_core::{
    format::{
        ManifestTreeLeaf, ManifestTreeLeafRef, ManifestTreeRoot, ObjectEnvelope,
        MANIFEST_OBJECT_ID, MANIFEST_TREE_LEAF_SPAN,
    },
    Manifest, ManifestCommitter, ManifestEncoding, RemoteBackend, RemoteObjectRef, Superblock,
};
use tokio::{sync::Semaphore, task::JoinHandle};
use tracing::{debug, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramErrorKind {
    Retryable,
    RateLimited,
    Authentication,
    Corruption,
    NotFound,
    Permanent,
}

pub fn classify_error(error: &anyhow::Error) -> TelegramErrorKind {
    for cause in error.chain() {
        if let Some(invocation) = cause.downcast_ref::<InvocationError>() {
            return classify_invocation_error(invocation);
        }
        if cause
            .downcast_ref::<tgdrive_core::format::FormatError>()
            .is_some()
        {
            return TelegramErrorKind::Corruption;
        }
    }
    let text = error.to_string();
    if text.contains("checksum")
        || text.contains("wrong object")
        || text.contains("wrong generation")
        || text.contains("has no media")
    {
        TelegramErrorKind::Corruption
    } else if text.contains("not found") {
        TelegramErrorKind::NotFound
    } else {
        TelegramErrorKind::Permanent
    }
}

fn classify_invocation_error(error: &InvocationError) -> TelegramErrorKind {
    match error {
        InvocationError::Rpc(rpc) if rpc.code == 420 => TelegramErrorKind::RateLimited,
        InvocationError::Rpc(rpc)
            if rpc.is("AUTH_KEY_*")
                || rpc.is("SESSION_*")
                || rpc.is("USER_DEACTIVATED*")
                || rpc.is("PHONE_*") =>
        {
            TelegramErrorKind::Authentication
        }
        InvocationError::Rpc(rpc) if rpc.code >= 500 => TelegramErrorKind::Retryable,
        InvocationError::Io(_) | InvocationError::Dropped => TelegramErrorKind::Retryable,
        InvocationError::Transport(_) => TelegramErrorKind::Retryable,
        InvocationError::Authentication(_) | InvocationError::InvalidDc => {
            TelegramErrorKind::Authentication
        }
        InvocationError::Deserialize(_) => TelegramErrorKind::Corruption,
        InvocationError::Rpc(_) => TelegramErrorKind::Permanent,
    }
}

#[derive(Debug, Clone)]
pub struct TelegramClientConfig {
    pub api_id: i32,
    pub api_hash: String,
    pub session_path: PathBuf,
    pub channel_id: i64,
    pub channel_access_hash: Option<i64>,
    pub channel_title: Option<String>,
}

pub struct TelegramStorage {
    config: TelegramClientConfig,
    client: Client,
    peer: PeerRef,
    handle: SenderPoolFatHandle,
    runner: JoinHandle<()>,
    upload_limit: Arc<Semaphore>,
    download_limit: Arc<Semaphore>,
}

impl Drop for TelegramStorage {
    fn drop(&mut self) {
        self.handle.quit();
        self.runner.abort();
    }
}

impl TelegramStorage {
    pub async fn connect(config: TelegramClientConfig) -> anyhow::Result<Self> {
        let (client, handle, runner) = connect_client(&config).await?;
        let peer = resolve_storage_peer(&client, &config).await?;
        Ok(Self {
            config,
            client,
            peer,
            handle,
            runner,
            upload_limit: Arc::new(Semaphore::new(4)),
            download_limit: Arc::new(Semaphore::new(8)),
        })
    }

    pub async fn connect_for_login(config: TelegramClientConfig) -> anyhow::Result<LoginClient> {
        let (client, handle, runner) = connect_client(&config).await?;
        Ok(LoginClient {
            api_hash: config.api_hash,
            client,
            handle,
            runner,
        })
    }

    pub async fn resolve_channel(&self) -> anyhow::Result<ResolvedChannel> {
        Ok(ResolvedChannel {
            bot_api_chat_id: self.peer.id.bot_api_dialog_id_unchecked(),
            bare_channel_id: self.peer.id.bare_id_unchecked(),
            access_hash: self.peer.auth.hash(),
            title: self.config.channel_title.clone(),
        })
    }

    pub async fn upsert_pinned_superblock(&self, text: &str) -> anyhow::Result<i32> {
        if let Some(message) = self.pinned_message().await? {
            if message.text().starts_with("TGDRIVE_SUPERBLOCK_V1\n") {
                self.send_superblock_backup(text).await?;
                return Ok(message.id());
            }
        }
        let message = self
            .client
            .send_message(self.peer, InputMessage::new().text(text).silent(true))
            .await?;
        self.client.pin_message(self.peer, message.id()).await?;
        self.send_superblock_backup(text).await?;
        Ok(message.id())
    }

    pub async fn pinned_superblock_text(&self) -> anyhow::Result<Option<String>> {
        Ok(self
            .pinned_message()
            .await?
            .map(|message| message.text().to_string()))
    }

    pub async fn recover_superblock_text(
        &self,
        scan_limit: usize,
    ) -> anyhow::Result<Option<String>> {
        let mut best: Option<(u64, String)> = None;
        let mut messages = self.client.iter_messages(self.peer).limit(scan_limit);
        while let Some(message) = messages.next().await? {
            let text = message.text();
            if let Ok(superblock) = Superblock::decode_text(text) {
                if best
                    .as_ref()
                    .map(|(generation, _)| superblock.manifest_generation > *generation)
                    .unwrap_or(true)
                {
                    best = Some((superblock.manifest_generation, text.to_string()));
                }
            }
        }
        if best.is_some() {
            return Ok(best.map(|(_, text)| text));
        }
        Ok(self.pinned_superblock_text().await?.and_then(|text| {
            Superblock::decode_text(&text).ok()?;
            Some(text)
        }))
    }

    pub async fn delete_messages(&self, message_ids: &[i32]) -> anyhow::Result<usize> {
        let mut deleted = 0;
        for chunk in message_ids.chunks(100) {
            deleted += self.client.delete_messages(self.peer, chunk).await?;
        }
        Ok(deleted)
    }

    pub async fn fetch_manifest_from_superblock(
        &self,
        superblock: &Superblock,
    ) -> anyhow::Result<Manifest> {
        let root_ref = RemoteObjectRef {
            chat_id: self.peer.id.bot_api_dialog_id_unchecked(),
            message_id: superblock.current_manifest_message_id,
            object_id: MANIFEST_OBJECT_ID,
            generation: superblock.manifest_generation,
            sha256: superblock.manifest_hash.clone(),
        };
        match superblock.manifest_encoding {
            ManifestEncoding::FlatJson => {
                let encoded = self.fetch_object(&root_ref).await?;
                let envelope =
                    ObjectEnvelope::decode(&encoded, MANIFEST_OBJECT_ID, root_ref.generation)?;
                Manifest::decode_json(&envelope.payload)
            }
            ManifestEncoding::Tree => {
                let encoded = self.fetch_object(&root_ref).await?;
                let envelope =
                    ObjectEnvelope::decode(&encoded, MANIFEST_OBJECT_ID, root_ref.generation)?;
                let root = ManifestTreeRoot::decode_json(&envelope.payload)?;
                let leaves = self.fetch_manifest_tree_leaves(&root).await?;
                Manifest::from_tree_root_and_leaves(root, leaves)
            }
        }
    }

    async fn send_superblock_backup(&self, text: &str) -> anyhow::Result<i32> {
        let message = self
            .client
            .send_message(self.peer, InputMessage::new().text(text).silent(true))
            .await?;
        Ok(message.id())
    }

    async fn pinned_message(&self) -> anyhow::Result<Option<Message>> {
        match self.client.get_pinned_message(self.peer).await {
            Ok(message) => Ok(message),
            Err(InvocationError::Rpc(rpc)) if rpc.is("MESSAGE_IDS_EMPTY") => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn fetch_manifest_tree_leaves(
        &self,
        root: &ManifestTreeRoot,
    ) -> anyhow::Result<Vec<ManifestTreeLeaf>> {
        let ids = root
            .leaves
            .iter()
            .map(|leaf| leaf.remote.message_id)
            .collect::<Vec<_>>();
        let messages = self.get_messages_by_id(&ids).await?;
        let mut leaves = Vec::with_capacity(root.leaves.len());
        for (leaf_ref, encoded) in root.leaves.iter().zip(messages) {
            let encoded = encoded
                .ok_or_else(|| anyhow::anyhow!("manifest leaf {} missing", leaf_ref.leaf_index))?;
            let envelope = ObjectEnvelope::decode(
                &encoded,
                leaf_ref.remote.object_id,
                leaf_ref.remote.generation,
            )?;
            anyhow::ensure!(
                tgdrive_core::format::sha256_hex(&encoded) == leaf_ref.remote.sha256,
                "manifest leaf checksum mismatch"
            );
            leaves.push(ManifestTreeLeaf::decode_json(&envelope.payload)?);
        }
        Ok(leaves)
    }

    async fn send_manifest_tree(
        &self,
        manifest: &Manifest,
        object_size: u32,
    ) -> anyhow::Result<RemoteObjectRef> {
        let mut refs = Vec::new();
        for leaf in manifest.tree_leaves(MANIFEST_TREE_LEAF_SPAN) {
            let payload = leaf.encode_json()?;
            let object_id = ManifestTreeLeaf::object_id_for_index(leaf.leaf_index);
            let envelope = ObjectEnvelope {
                object_id,
                generation: manifest.generation,
                object_size,
                flags: 0,
                payload,
            };
            let encoded = envelope.encode();
            let remote = self
                .send_object(
                    object_id,
                    manifest.generation,
                    encoded.clone(),
                    tgdrive_core::format::sha256_hex(&encoded),
                )
                .await?;
            refs.push(ManifestTreeLeafRef {
                leaf_index: leaf.leaf_index,
                start_object_id: leaf.start_object_id,
                object_count: leaf.objects.len() as u64,
                remote,
            });
        }

        let root = ManifestTreeRoot {
            magic: "TGDRIVE_MANIFEST_ROOT".to_string(),
            format_version: 1,
            device_uuid: manifest.device_uuid,
            generation: manifest.generation,
            object_size: manifest.object_size,
            object_count: manifest.object_count,
            leaf_span: MANIFEST_TREE_LEAF_SPAN,
            leaves: refs,
            garbage: manifest.garbage.clone(),
        };
        let envelope = ObjectEnvelope {
            object_id: MANIFEST_OBJECT_ID,
            generation: manifest.generation,
            object_size,
            flags: 0,
            payload: root.encode_json()?,
        };
        let encoded = envelope.encode();
        let root_ref = self
            .send_object(
                MANIFEST_OBJECT_ID,
                manifest.generation,
                encoded.clone(),
                tgdrive_core::format::sha256_hex(&encoded),
            )
            .await?;
        self.fetch_object(&root_ref).await?;
        Ok(root_ref)
    }
}

pub struct LoginClient {
    api_hash: String,
    client: Client,
    handle: SenderPoolFatHandle,
    runner: JoinHandle<()>,
}

impl Drop for LoginClient {
    fn drop(&mut self) {
        self.handle.quit();
        self.runner.abort();
    }
}

impl LoginClient {
    pub async fn is_authorized(&self) -> anyhow::Result<bool> {
        Ok(self.client.is_authorized().await?)
    }

    pub async fn request_login_code(&self, phone: &str) -> anyhow::Result<LoginToken> {
        Ok(self
            .client
            .request_login_code(phone, &self.api_hash)
            .await?)
    }

    pub async fn sign_in(&self, token: &LoginToken, code: &str) -> Result<(), SignInError> {
        self.client.sign_in(token, code).await.map(drop)
    }

    pub async fn check_password(
        &self,
        password_token: PasswordToken,
        password: &str,
    ) -> Result<(), SignInError> {
        self.client
            .check_password(password_token, password)
            .await
            .map(drop)
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedChannel {
    pub bot_api_chat_id: i64,
    pub bare_channel_id: i64,
    pub access_hash: i64,
    pub title: Option<String>,
}

#[async_trait]
impl RemoteBackend for TelegramStorage {
    async fn send_object(
        &self,
        object_id: u64,
        generation: u64,
        payload: Vec<u8>,
        sha256: String,
    ) -> anyhow::Result<RemoteObjectRef> {
        ObjectEnvelope::decode(&payload, object_id, generation)?;
        let _permit = self.upload_limit.acquire().await?;
        let name = format!("tgdrive-{object_id:016x}-{generation:016x}.bin");
        let mut stream = Cursor::new(payload);
        let len = stream.get_ref().len();
        let uploaded = self.client.upload_stream(&mut stream, len, name).await?;
        let caption = format!("TGDRIVE object={object_id} generation={generation} sha256={sha256}");
        let message = self
            .client
            .send_message(
                self.peer,
                InputMessage::new()
                    .text(caption)
                    .document(uploaded)
                    .silent(true),
            )
            .await?;
        Ok(RemoteObjectRef {
            chat_id: self.peer.id.bot_api_dialog_id_unchecked(),
            message_id: message.id(),
            object_id,
            generation,
            sha256,
        })
    }

    async fn fetch_object(&self, reference: &RemoteObjectRef) -> anyhow::Result<Vec<u8>> {
        let messages = self.get_messages_by_id(&[reference.message_id]).await?;
        let payload = messages
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| anyhow::anyhow!("message {} not found", reference.message_id))?;
        ObjectEnvelope::decode(&payload, reference.object_id, reference.generation)?;
        anyhow::ensure!(
            tgdrive_core::format::sha256_hex(&payload) == reference.sha256,
            "remote payload checksum mismatch"
        );
        Ok(payload)
    }

    async fn get_messages_by_id(&self, ids: &[i32]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(100) {
            let _permit = self.download_limit.acquire().await?;
            let messages = self.client.get_messages_by_id(self.peer, chunk).await?;
            for message in messages {
                out.push(match message {
                    Some(message) => Some(download_message_media(&self.client, message).await?),
                    None => None,
                });
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl ManifestCommitter for TelegramStorage {
    async fn commit_manifest(
        &self,
        manifest: &Manifest,
        device_size: u64,
        logical_sector_size: u32,
        object_size: u32,
    ) -> anyhow::Result<()> {
        let use_tree = manifest.objects.len() > MANIFEST_TREE_LEAF_SPAN;
        let manifest_ref = if use_tree {
            self.send_manifest_tree(manifest, object_size).await?
        } else {
            let envelope = ObjectEnvelope {
                object_id: MANIFEST_OBJECT_ID,
                generation: manifest.generation,
                object_size,
                flags: 0,
                payload: manifest.encode_json()?,
            };
            let encoded = envelope.encode();
            let manifest_ref = self
                .send_object(
                    MANIFEST_OBJECT_ID,
                    manifest.generation,
                    encoded.clone(),
                    tgdrive_core::format::sha256_hex(&encoded),
                )
                .await?;
            self.fetch_object(&manifest_ref).await?;
            manifest_ref
        };
        if std::env::var("TGDRIVE_CRASH_AFTER_MANIFEST_UPLOAD").as_deref() == Ok("1") {
            warn!("TGDRIVE_CRASH_AFTER_MANIFEST_UPLOAD set; aborting after manifest upload");
            std::process::abort();
        }
        let previous = self
            .pinned_superblock_text()
            .await?
            .and_then(|text| Superblock::decode_text(&text).ok());
        let superblock = if use_tree {
            Superblock::for_tree_manifest(
                manifest.device_uuid,
                device_size,
                logical_sector_size,
                object_size,
                &manifest_ref,
                previous.as_ref(),
            )
        } else {
            Superblock::for_manifest(
                manifest.device_uuid,
                device_size,
                logical_sector_size,
                object_size,
                &manifest_ref,
                previous.as_ref(),
            )
        };
        self.upsert_pinned_superblock(&superblock.encode_text()?)
            .await?;
        Ok(())
    }
}

async fn connect_client(
    config: &TelegramClientConfig,
) -> anyhow::Result<(Client, SenderPoolFatHandle, JoinHandle<()>)> {
    if let Some(parent) = config.session_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let session = Arc::new(FileSession::open(&config.session_path)?);
    let SenderPool {
        runner,
        updates: _,
        handle,
    } = SenderPool::new(Arc::clone(&session), config.api_id);
    let client = Client::with_configuration(
        handle.clone(),
        ClientConfiguration {
            retry_policy: Box::new(AutoSleep {
                threshold: Duration::from_secs(10 * 60),
                io_errors_as_flood_of: Some(Duration::from_secs(1)),
            }),
            auto_cache_peers: true,
        },
    );
    let runner = tokio::spawn(async move {
        runner.run().await;
        warn!("Telegram sender runner stopped");
    });
    Ok((client, handle, runner))
}

struct FileSession {
    path: PathBuf,
    data: Mutex<SessionData>,
}

impl FileSession {
    fn open(path: &Path) -> anyhow::Result<Self> {
        let data = if path.exists() {
            serde_json::from_slice(&fs::read(path)?)?
        } else {
            SessionData::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            data: Mutex::new(data),
        })
    }

    fn save_locked(&self, data: &SessionData) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(data)?;
        fs::write(&self.path, bytes)?;
        Ok(())
    }
}

impl Session for FileSession {
    fn home_dc_id(&self) -> i32 {
        self.data.lock().unwrap().home_dc
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            data.home_dc = dc_id;
            let _ = self.save_locked(&data);
        })
    }

    fn dc_option(&self, dc_id: i32) -> Option<DcOption> {
        self.data.lock().unwrap().dc_options.get(&dc_id).cloned()
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, ()> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            data.dc_options.insert(dc_option.id, dc_option);
            let _ = self.save_locked(&data);
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Option<PeerInfo>> {
        Box::pin(async move { self.data.lock().unwrap().peer_infos.get(&peer).cloned() })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, ()> {
        let peer = peer.clone();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            data.peer_infos
                .entry(peer.id())
                .or_insert_with(|| peer.clone())
                .extend_info(&peer);
            let _ = self.save_locked(&data);
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, UpdatesState> {
        Box::pin(async move { self.data.lock().unwrap().updates_state.clone() })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            match update {
                UpdateState::All(updates_state) => {
                    data.updates_state = updates_state;
                }
                UpdateState::Primary { pts, date, seq } => {
                    data.updates_state.pts = pts;
                    data.updates_state.date = date;
                    data.updates_state.seq = seq;
                }
                UpdateState::Secondary { qts } => {
                    data.updates_state.qts = qts;
                }
                UpdateState::Channel { id, pts } => {
                    data.updates_state
                        .channels
                        .retain(|channel| channel.id != id);
                    data.updates_state.channels.push(ChannelState { id, pts });
                }
            }
            let _ = self.save_locked(&data);
        })
    }
}

async fn resolve_storage_peer(
    client: &Client,
    config: &TelegramClientConfig,
) -> anyhow::Result<PeerRef> {
    if let Some(access_hash) = config.channel_access_hash {
        let id = PeerId::from_bot_api_dialog_id(config.channel_id)
            .ok_or_else(|| anyhow::anyhow!("invalid Telegram channel id {}", config.channel_id))?;
        return Ok(PeerRef {
            id,
            auth: PeerAuth::from_hash(access_hash),
        });
    }

    if let Some(title) = &config.channel_title {
        let mut dialogs = client.iter_dialogs();
        while let Some(dialog) = dialogs.next().await? {
            let peer = dialog.peer();
            if peer.name() == Some(title.as_str()) {
                debug!(title, "resolved Telegram channel by title");
                return Ok(dialog.peer_ref());
            }
        }
    }

    let id = PeerId::from_bot_api_dialog_id(config.channel_id)
        .ok_or_else(|| anyhow::anyhow!("invalid Telegram channel id {}", config.channel_id))?;
    Ok(id.to_ambient_ref())
}

async fn download_message_media(client: &Client, message: Message) -> anyhow::Result<Vec<u8>> {
    let media = message
        .media()
        .ok_or_else(|| anyhow::anyhow!("message {} has no media", message.id()))?;
    let mut download = client.iter_download(&media);
    let mut bytes = Vec::new();
    while let Some(chunk) = download.next().await? {
        bytes.extend(chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod live_tests {
    use super::*;
    use grammers_mtsender::RpcError;

    fn live_config() -> Option<TelegramClientConfig> {
        if std::env::var("TGDRIVE_LIVE_TELEGRAM").ok().as_deref() != Some("1") {
            return None;
        }
        Some(TelegramClientConfig {
            api_id: std::env::var("TELEGRAM_API_ID").ok()?.parse().ok()?,
            api_hash: std::env::var("TELEGRAM_API_HASH").ok()?,
            session_path: std::env::var("TELEGRAM_SESSION_PATH").ok()?.into(),
            channel_id: std::env::var("TELEGRAM_CHANNEL_ID").ok()?.parse().ok()?,
            channel_access_hash: std::env::var("TELEGRAM_CHANNEL_ACCESS_HASH")
                .ok()
                .and_then(|value| value.parse().ok()),
            channel_title: std::env::var("TELEGRAM_CHANNEL_TITLE").ok(),
        })
    }

    #[tokio::test]
    async fn live_resolves_storage_channel_when_enabled() -> anyhow::Result<()> {
        let Some(config) = live_config() else {
            eprintln!("skipping live Telegram test; set TGDRIVE_LIVE_TELEGRAM=1");
            return Ok(());
        };
        let storage = TelegramStorage::connect(config).await?;
        storage.resolve_channel().await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_destructive_delete_gate_is_separate() -> anyhow::Result<()> {
        if std::env::var("TGDRIVE_LIVE_TELEGRAM_DESTRUCTIVE")
            .ok()
            .as_deref()
            != Some("1")
        {
            eprintln!(
                "skipping destructive live Telegram test; set TGDRIVE_LIVE_TELEGRAM_DESTRUCTIVE=1"
            );
            return Ok(());
        }
        anyhow::ensure!(
            live_config().is_some(),
            "destructive live tests also require TGDRIVE_LIVE_TELEGRAM=1 and Telegram env"
        );
        Ok(())
    }

    #[test]
    fn classifies_error_kinds() {
        let flood = anyhow::Error::new(InvocationError::Rpc(RpcError {
            code: 420,
            name: "FLOOD_WAIT".to_string(),
            value: Some(10),
            caused_by: None,
        }));
        assert_eq!(classify_error(&flood), TelegramErrorKind::RateLimited);

        let server = anyhow::Error::new(InvocationError::Rpc(RpcError {
            code: 500,
            name: "INTERNAL".to_string(),
            value: None,
            caused_by: None,
        }));
        assert_eq!(classify_error(&server), TelegramErrorKind::Retryable);

        let corrupt = anyhow::anyhow!("remote payload checksum mismatch");
        assert_eq!(classify_error(&corrupt), TelegramErrorKind::Corruption);
    }
}
