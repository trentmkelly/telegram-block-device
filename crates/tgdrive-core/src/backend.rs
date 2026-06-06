use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteObjectRef {
    pub chat_id: i64,
    pub message_id: i32,
    pub object_id: u64,
    pub generation: u64,
    pub sha256: String,
}

#[async_trait]
pub trait RemoteBackend: Send + Sync {
    async fn send_object(
        &self,
        object_id: u64,
        generation: u64,
        payload: Vec<u8>,
        sha256: String,
    ) -> anyhow::Result<RemoteObjectRef>;

    async fn fetch_object(&self, reference: &RemoteObjectRef) -> anyhow::Result<Vec<u8>>;

    async fn get_messages_by_id(&self, ids: &[i32]) -> anyhow::Result<Vec<Option<Vec<u8>>>>;
}

#[async_trait]
pub trait ManifestCommitter: Send + Sync {
    async fn commit_manifest(
        &self,
        manifest: &crate::format::Manifest,
        device_size: u64,
        logical_sector_size: u32,
        object_size: u32,
    ) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct MemoryBackend {
    chat_id: i64,
    next_message_id: Arc<RwLock<i32>>,
    messages: Arc<RwLock<HashMap<i32, Vec<u8>>>>,
}

impl MemoryBackend {
    pub fn new(chat_id: i64) -> Self {
        Self {
            chat_id,
            next_message_id: Arc::new(RwLock::new(1)),
            messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl RemoteBackend for MemoryBackend {
    async fn send_object(
        &self,
        object_id: u64,
        generation: u64,
        payload: Vec<u8>,
        sha256: String,
    ) -> anyhow::Result<RemoteObjectRef> {
        let mut next = self.next_message_id.write().await;
        let message_id = *next;
        *next += 1;
        self.messages.write().await.insert(message_id, payload);
        Ok(RemoteObjectRef {
            chat_id: self.chat_id,
            message_id,
            object_id,
            generation,
            sha256,
        })
    }

    async fn fetch_object(&self, reference: &RemoteObjectRef) -> anyhow::Result<Vec<u8>> {
        self.messages
            .read()
            .await
            .get(&reference.message_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("message {} not found", reference.message_id))
    }

    async fn get_messages_by_id(&self, ids: &[i32]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let messages = self.messages.read().await;
        Ok(ids.iter().map(|id| messages.get(id).cloned()).collect())
    }
}
