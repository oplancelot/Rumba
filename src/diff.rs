use crate::db::BackupDb;
use crate::models::Hash;
use anyhow::Result;
use std::path::Path;

pub struct DiffEngine<'a> {
    db: &'a BackupDb,
}

impl<'a> DiffEngine<'a> {
    pub fn new(db: &'a BackupDb) -> Self {
        Self { db }
    }

    /// Checks if the file is unchanged based on the index.
    /// Returns Some(Hash) if the file is clean (unchanged).
    /// Returns None if the file is dirty (needs hashing).
    pub fn check_index(&self, path: &Path, mtime: i64, size: u64) -> Result<Option<Hash>> {
        let path_str = path.to_string_lossy();
        // Note: We use the string representation of the path as the key.
        // This assumes consistent path normalization.
        if let Some(entry) = self.db.get_index_entry(&path_str)? {
            if entry.mtime == mtime && entry.size == size {
                return Ok(Some(entry.hash));
            }
        }
        Ok(None)
    }

    /// Checks if the blob with the given hash already exists in the backup (Deduplication).
    /// Returns true if the blob should be backed up (it's new).
    /// Returns false if the blob already exists.
    pub fn should_backup_blob(&self, hash: &Hash) -> Result<bool> {
        Ok(self.db.get_blob(hash)?.is_none())
    }
}
