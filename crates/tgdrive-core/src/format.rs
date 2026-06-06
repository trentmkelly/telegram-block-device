use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::backend::RemoteObjectRef;

pub const TGDRIVE_MAGIC: &[u8; 8] = b"TGDRV001";
pub const MANIFEST_OBJECT_ID: u64 = u64::MAX - 1;
pub const MANIFEST_TREE_LEAF_BASE_OBJECT_ID: u64 = u64::MAX - 1_000_000;
pub const MANIFEST_TREE_LEAF_SPAN: usize = 4096;
const OBJECT_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8 + 8 + 4 + 32 + 4;

#[derive(Debug, Error)]
pub enum FormatError {
    #[error("invalid magic")]
    InvalidMagic,
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("truncated object")]
    Truncated,
    #[error("object id mismatch: expected {expected}, got {actual}")]
    ObjectIdMismatch { expected: u64, actual: u64 },
    #[error("generation mismatch: expected {expected}, got {actual}")]
    GenerationMismatch { expected: u64, actual: u64 },
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("payload length mismatch")]
    PayloadLengthMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEnvelope {
    pub object_id: u64,
    pub generation: u64,
    pub object_size: u32,
    pub flags: u32,
    pub payload: Vec<u8>,
}

impl ObjectEnvelope {
    pub const VERSION: u16 = 1;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(OBJECT_HEADER_LEN + self.payload.len());
        out.extend_from_slice(TGDRIVE_MAGIC);
        out.extend_from_slice(&Self::VERSION.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&self.object_id.to_be_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u64).to_be_bytes());
        out.extend_from_slice(&self.object_size.to_be_bytes());
        out.extend_from_slice(&sha256_bytes(&self.payload));
        out.extend_from_slice(&self.flags.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(
        bytes: &[u8],
        expected_object_id: u64,
        expected_generation: u64,
    ) -> Result<Self, FormatError> {
        if bytes.len() < OBJECT_HEADER_LEN {
            return Err(FormatError::Truncated);
        }
        if &bytes[0..8] != TGDRIVE_MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        let version = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
        if version != Self::VERSION {
            return Err(FormatError::UnsupportedVersion(version));
        }
        let object_id = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
        if object_id != expected_object_id {
            return Err(FormatError::ObjectIdMismatch {
                expected: expected_object_id,
                actual: object_id,
            });
        }
        let generation = u64::from_be_bytes(bytes[20..28].try_into().unwrap());
        if generation != expected_generation {
            return Err(FormatError::GenerationMismatch {
                expected: expected_generation,
                actual: generation,
            });
        }
        let payload_len = u64::from_be_bytes(bytes[28..36].try_into().unwrap()) as usize;
        let object_size = u32::from_be_bytes(bytes[36..40].try_into().unwrap());
        let checksum = &bytes[40..72];
        let flags = u32::from_be_bytes(bytes[72..76].try_into().unwrap());
        if bytes.len() != OBJECT_HEADER_LEN + payload_len {
            return Err(FormatError::PayloadLengthMismatch);
        }
        let payload = bytes[OBJECT_HEADER_LEN..].to_vec();
        if sha256_bytes(&payload).as_slice() != checksum {
            return Err(FormatError::ChecksumMismatch);
        }
        Ok(Self {
            object_id,
            generation,
            object_size,
            flags,
            payload,
        })
    }

    pub fn sha256_hex(&self) -> String {
        hex::encode(sha256_bytes(&self.encode()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Superblock {
    pub magic: String,
    pub format_version: u16,
    pub device_uuid: Uuid,
    pub device_size: u64,
    pub logical_sector_size: u32,
    pub object_size: u32,
    pub current_manifest_message_id: i32,
    pub manifest_generation: u64,
    pub manifest_hash: String,
    pub previous_manifest_message_id: Option<i32>,
    pub previous_manifest_hash: Option<String>,
    #[serde(default)]
    pub previous_manifest_generation: Option<u64>,
    pub manifest_encoding: ManifestEncoding,
    pub root_index_message_id: Option<i32>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub last_commit_at: OffsetDateTime,
}

impl Superblock {
    pub fn new(
        device_size: u64,
        logical_sector_size: u32,
        object_size: u32,
        manifest_ref: &RemoteObjectRef,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            magic: "TGDRIVE_SUPERBLOCK".to_string(),
            format_version: 1,
            device_uuid: Uuid::new_v4(),
            device_size,
            logical_sector_size,
            object_size,
            current_manifest_message_id: manifest_ref.message_id,
            manifest_generation: manifest_ref.generation,
            manifest_hash: manifest_ref.sha256.clone(),
            previous_manifest_message_id: None,
            previous_manifest_hash: None,
            previous_manifest_generation: None,
            manifest_encoding: ManifestEncoding::FlatJson,
            root_index_message_id: None,
            created_at: now,
            last_commit_at: now,
        }
    }

    pub fn for_manifest(
        device_uuid: Uuid,
        device_size: u64,
        logical_sector_size: u32,
        object_size: u32,
        manifest_ref: &RemoteObjectRef,
        previous: Option<&Superblock>,
    ) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            magic: "TGDRIVE_SUPERBLOCK".to_string(),
            format_version: 1,
            device_uuid,
            device_size,
            logical_sector_size,
            object_size,
            current_manifest_message_id: manifest_ref.message_id,
            manifest_generation: manifest_ref.generation,
            manifest_hash: manifest_ref.sha256.clone(),
            previous_manifest_message_id: previous.map(|s| s.current_manifest_message_id),
            previous_manifest_hash: previous.map(|s| s.manifest_hash.clone()),
            previous_manifest_generation: previous.map(|s| s.manifest_generation),
            manifest_encoding: ManifestEncoding::FlatJson,
            root_index_message_id: None,
            created_at: previous.map(|s| s.created_at).unwrap_or(now),
            last_commit_at: now,
        }
    }

    pub fn for_tree_manifest(
        device_uuid: Uuid,
        device_size: u64,
        logical_sector_size: u32,
        object_size: u32,
        root_ref: &RemoteObjectRef,
        previous: Option<&Superblock>,
    ) -> Self {
        let mut superblock = Self::for_manifest(
            device_uuid,
            device_size,
            logical_sector_size,
            object_size,
            root_ref,
            previous,
        );
        superblock.manifest_encoding = ManifestEncoding::Tree;
        superblock.root_index_message_id = Some(root_ref.message_id);
        superblock
    }

    pub fn encode_text(&self) -> anyhow::Result<String> {
        Ok(format!(
            "TGDRIVE_SUPERBLOCK_V1\n{}",
            serde_json::to_string_pretty(self)?
        ))
    }

    pub fn decode_text(text: &str) -> anyhow::Result<Self> {
        let json = text
            .strip_prefix("TGDRIVE_SUPERBLOCK_V1\n")
            .ok_or_else(|| anyhow::anyhow!("missing superblock prefix"))?;
        Ok(serde_json::from_str(json)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ManifestEncoding {
    FlatJson,
    Tree,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub magic: String,
    pub format_version: u16,
    pub device_uuid: Uuid,
    pub generation: u64,
    pub object_size: u32,
    pub object_count: u64,
    pub objects: Vec<ManifestObject>,
    pub garbage: Vec<RemoteObjectRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestObject {
    pub object_id: u64,
    pub generation: u64,
    pub remote: Option<RemoteObjectRef>,
    pub sha256: Option<String>,
    pub zero: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestTreeRoot {
    pub magic: String,
    pub format_version: u16,
    pub device_uuid: Uuid,
    pub generation: u64,
    pub object_size: u32,
    pub object_count: u64,
    pub leaf_span: usize,
    pub leaves: Vec<ManifestTreeLeafRef>,
    pub garbage: Vec<RemoteObjectRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestTreeLeafRef {
    pub leaf_index: u64,
    pub start_object_id: u64,
    pub object_count: u64,
    pub remote: RemoteObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestTreeLeaf {
    pub magic: String,
    pub format_version: u16,
    pub device_uuid: Uuid,
    pub generation: u64,
    pub leaf_index: u64,
    pub start_object_id: u64,
    pub objects: Vec<ManifestObject>,
}

impl Manifest {
    pub fn empty(device_uuid: Uuid, object_size: u32, object_count: u64) -> Self {
        Self {
            magic: "TGDRIVE_MANIFEST".to_string(),
            format_version: 1,
            device_uuid,
            generation: 0,
            object_size,
            object_count,
            objects: (0..object_count)
                .map(|object_id| ManifestObject {
                    object_id,
                    generation: 0,
                    remote: None,
                    sha256: None,
                    zero: true,
                })
                .collect(),
            garbage: Vec::new(),
        }
    }

    pub fn encode_json(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn hash_hex(&self) -> anyhow::Result<String> {
        Ok(hex::encode(sha256_bytes(&self.encode_json()?)))
    }

    pub fn verify_hash(&self, expected: &str) -> anyhow::Result<()> {
        let actual = self.hash_hex()?;
        anyhow::ensure!(actual == expected, "manifest hash mismatch");
        Ok(())
    }

    pub fn tree_leaves(&self, leaf_span: usize) -> Vec<ManifestTreeLeaf> {
        self.objects
            .chunks(leaf_span)
            .enumerate()
            .map(|(leaf_index, objects)| ManifestTreeLeaf {
                magic: "TGDRIVE_MANIFEST_LEAF".to_string(),
                format_version: 1,
                device_uuid: self.device_uuid,
                generation: self.generation,
                leaf_index: leaf_index as u64,
                start_object_id: objects
                    .first()
                    .map(|object| object.object_id)
                    .unwrap_or_default(),
                objects: objects.to_vec(),
            })
            .collect()
    }

    pub fn from_tree_root_and_leaves(
        root: ManifestTreeRoot,
        mut leaves: Vec<ManifestTreeLeaf>,
    ) -> anyhow::Result<Self> {
        leaves.sort_by_key(|leaf| leaf.leaf_index);
        let mut objects = Vec::with_capacity(root.object_count as usize);
        for (expected_index, leaf) in leaves.into_iter().enumerate() {
            anyhow::ensure!(
                leaf.magic == "TGDRIVE_MANIFEST_LEAF",
                "invalid manifest leaf magic"
            );
            anyhow::ensure!(
                leaf.format_version == 1,
                "unsupported manifest leaf version"
            );
            anyhow::ensure!(
                leaf.device_uuid == root.device_uuid,
                "manifest leaf UUID mismatch"
            );
            anyhow::ensure!(
                leaf.generation == root.generation,
                "manifest leaf generation mismatch"
            );
            anyhow::ensure!(
                leaf.leaf_index == expected_index as u64,
                "manifest leaf index mismatch"
            );
            objects.extend(leaf.objects);
        }
        anyhow::ensure!(
            objects.len() as u64 == root.object_count,
            "manifest tree object count mismatch"
        );
        for (expected_object_id, object) in objects.iter().enumerate() {
            anyhow::ensure!(
                object.object_id == expected_object_id as u64,
                "manifest tree object id mismatch"
            );
        }
        Ok(Self {
            magic: "TGDRIVE_MANIFEST".to_string(),
            format_version: 1,
            device_uuid: root.device_uuid,
            generation: root.generation,
            object_size: root.object_size,
            object_count: root.object_count,
            objects,
            garbage: root.garbage,
        })
    }
}

impl ManifestTreeRoot {
    pub fn encode_json(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

impl ManifestTreeLeaf {
    pub fn object_id_for_index(leaf_index: u64) -> u64 {
        MANIFEST_TREE_LEAF_BASE_OBJECT_ID - leaf_index
    }

    pub fn encode_json(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    pub fn decode_json(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(sha256_bytes(bytes))
}

fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_round_trip_is_strict() {
        let envelope = ObjectEnvelope {
            object_id: 7,
            generation: 3,
            object_size: 256 * 1024,
            flags: 0,
            payload: b"hello".to_vec(),
        };
        let encoded = envelope.encode();
        assert_eq!(ObjectEnvelope::decode(&encoded, 7, 3).unwrap(), envelope);
        assert!(matches!(
            ObjectEnvelope::decode(&encoded, 8, 3),
            Err(FormatError::ObjectIdMismatch { .. })
        ));
        assert!(matches!(
            ObjectEnvelope::decode(&encoded, 7, 4),
            Err(FormatError::GenerationMismatch { .. })
        ));

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 1;
        assert!(matches!(
            ObjectEnvelope::decode(&corrupted, 7, 3),
            Err(FormatError::ChecksumMismatch)
        ));

        assert!(matches!(
            ObjectEnvelope::decode(b"short", 7, 3),
            Err(FormatError::Truncated)
        ));

        let mut malformed = envelope.encode();
        malformed[0] = b'X';
        assert!(matches!(
            ObjectEnvelope::decode(&malformed, 7, 3),
            Err(FormatError::InvalidMagic)
        ));
    }

    #[test]
    fn manifest_hash_verifies() {
        let manifest = Manifest::empty(Uuid::new_v4(), 256 * 1024, 2);
        let hash = manifest.hash_hex().unwrap();
        manifest.verify_hash(&hash).unwrap();
        assert!(manifest.verify_hash("bad").is_err());
    }

    #[test]
    fn manifest_tree_round_trip_rebuilds_flat_manifest() {
        let mut manifest = Manifest::empty(Uuid::new_v4(), 256 * 1024, 5);
        manifest.generation = 3;
        manifest.objects[2].zero = false;
        manifest.objects[2].sha256 = Some("abc".to_string());
        let leaves = manifest.tree_leaves(2);
        assert_eq!(leaves.len(), 3);
        let refs = leaves
            .iter()
            .map(|leaf| ManifestTreeLeafRef {
                leaf_index: leaf.leaf_index,
                start_object_id: leaf.start_object_id,
                object_count: leaf.objects.len() as u64,
                remote: RemoteObjectRef {
                    chat_id: -100,
                    message_id: leaf.leaf_index as i32 + 10,
                    object_id: ManifestTreeLeaf::object_id_for_index(leaf.leaf_index),
                    generation: leaf.generation,
                    sha256: format!("sha{}", leaf.leaf_index),
                },
            })
            .collect();
        let root = ManifestTreeRoot {
            magic: "TGDRIVE_MANIFEST_ROOT".to_string(),
            format_version: 1,
            device_uuid: manifest.device_uuid,
            generation: manifest.generation,
            object_size: manifest.object_size,
            object_count: manifest.object_count,
            leaf_span: 2,
            leaves: refs,
            garbage: manifest.garbage.clone(),
        };
        assert_eq!(
            Manifest::from_tree_root_and_leaves(root, leaves).unwrap(),
            manifest
        );
    }
}
