use rkyv::{Archive, Deserialize, Serialize};

// Fixed length hash, suitable for zero-copy and database keys
pub type Hash = [u8; 32];

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[archive(check_bytes)]
#[repr(C)]
pub struct FileMetadata {
    pub size: u64,
    pub mtime: i64, // UNIX timestamp
    pub mode: u32,  // Permissions
    pub uid: u32,
    pub gid: u32,
    // File content hash (Content Addressable)
    pub content_hash: Hash,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(check_bytes)]
#[repr(C)]
pub struct TreeEntry {
    pub name: String,
    pub mode: u32,
    pub hash: Hash,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(check_bytes)]
#[repr(C)]
pub struct Commit {
    pub tree_hash: Hash,
    pub parent_hash: Option<Hash>,
    pub author: String,
    pub message: String,
    pub timestamp: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[archive(check_bytes)]
#[repr(C)]
pub struct BlobLocation {
    pub tape_id: u64,
    pub offset: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone, Copy)]
#[archive(check_bytes)]
#[repr(C)]
pub struct IndexEntry {
    pub mtime: i64,
    pub size: u64,
    pub hash: Hash,
}


impl FileMetadata {
    pub fn compute_hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();
        // We hash the metadata fields to form a unique identifier for the metadata state
        // Note: content_hash is part of the metadata, but usually we hash metadata *including* content hash
        // to get the "File Object" hash in a Git-like system?
        // Or is this just for the struct?
        // The spec says "FileMetadata... content_hash".
        // In Git, a blob hash is just content. A tree entry has mode, name, and blob hash.
        // Let's assume we just need a utility to hash this struct if needed, 
        // but for the Merkle Tree, the Tree Hash depends on TreeEntry(s).
        
        // For rkyv structs, we can hash the archived bytes for stability, 
        // or hash fields manually. Manual is safer for cross-platform/version stability if layout changes.
        hasher.update(&self.size.to_le_bytes());
        hasher.update(&self.mtime.to_le_bytes());
        hasher.update(&self.mode.to_le_bytes());
        hasher.update(&self.uid.to_le_bytes());
        hasher.update(&self.gid.to_le_bytes());
        hasher.update(&self.content_hash);
        *hasher.finalize().as_bytes()
    }
}

impl TreeEntry {
    pub fn compute_hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.name.as_bytes());
        hasher.update(&self.mode.to_le_bytes());
        hasher.update(&self.hash);
        *hasher.finalize().as_bytes()
    }
}
