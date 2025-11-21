use redb::{Database, TableDefinition, WriteTransaction};
use std::path::Path;
use anyhow::Result;
use crate::models::{Hash, BlobLocation};
use rkyv::Deserialize;

// Table Definitions
pub const BLOBS_TABLE: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("blobs");
pub const TREES_TABLE: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("trees");
pub const COMMITS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("commits");
pub const INDEX_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("index");

use std::sync::Arc;
use std::path::PathBuf;

#[derive(Clone)]
pub struct BackupDb {
    db: Arc<Database>,
    path: PathBuf,
}

impl BackupDb {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let db = Database::create(&path_buf)?;
        // Create tables if they don't exist
        let write_txn = db.begin_write()?;
        {
            write_txn.open_table(BLOBS_TABLE)?;
            write_txn.open_table(TREES_TABLE)?;
            write_txn.open_table(COMMITS_TABLE)?;
            write_txn.open_table(INDEX_TABLE)?;
        }
        write_txn.commit()?;
        
        Ok(Self { db: Arc::new(db), path: path_buf })
    }
    
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn begin_write(&self) -> Result<WriteTransaction> {
        Ok(self.db.begin_write()?)
    }
    
    pub fn begin_read(&self) -> Result<redb::ReadTransaction> {
        Ok(self.db.begin_read()?)
    }


    pub fn get_blob(&self, hash: &Hash) -> Result<Option<BlobLocation>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(BLOBS_TABLE)?;
        if let Some(value) = table.get(hash)? {
            let value = value.value();
            // Copy to owned Vec to fix alignment issues
            let bytes = value.to_vec();
            let archived = unsafe { rkyv::archived_root::<BlobLocation>(&bytes) };
            let deserialized: BlobLocation = archived.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new()).unwrap();
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }

    pub fn get_index_entry(&self, path: &str) -> Result<Option<crate::models::IndexEntry>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(INDEX_TABLE)?;
        if let Some(value) = table.get(path)? {
            let value = value.value();
            // Copy to owned Vec to fix alignment issues
            let bytes = value.to_vec();
            let archived = unsafe { rkyv::archived_root::<crate::models::IndexEntry>(&bytes) };
            let deserialized: crate::models::IndexEntry = archived.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new()).unwrap();
            Ok(Some(deserialized))
        } else {
            Ok(None)
        }
    }

    pub fn insert_blob(&self, txn: &WriteTransaction, hash: &Hash, location: &BlobLocation) -> Result<()> {
        use rkyv::ser::Serializer;
        let mut serializer = rkyv::ser::serializers::AllocSerializer::<256>::default();
        serializer.serialize_value(location).unwrap();
        let bytes = serializer.into_serializer().into_inner();
        
        let mut table = txn.open_table(BLOBS_TABLE)?;
        table.insert(hash, bytes.as_slice())?;
        Ok(())
    }

    pub fn insert_tree(&self, txn: &WriteTransaction, hash: &Hash, entries: &Vec<crate::models::TreeEntry>) -> Result<()> {
        use rkyv::ser::Serializer;
        let mut serializer = rkyv::ser::serializers::AllocSerializer::<4096>::default();
        serializer.serialize_value(entries).unwrap();
        let bytes = serializer.into_serializer().into_inner();
        
        let mut table = txn.open_table(TREES_TABLE)?;
        table.insert(hash, bytes.as_slice())?;
        Ok(())
    }

    pub fn insert_commit(&self, txn: &WriteTransaction, timestamp: u64, commit: &crate::models::Commit) -> Result<()> {
        use rkyv::ser::Serializer;
        let mut serializer = rkyv::ser::serializers::AllocSerializer::<1024>::default();
        serializer.serialize_value(commit).unwrap();
        let bytes = serializer.into_serializer().into_inner();
        
        let mut table = txn.open_table(COMMITS_TABLE)?;
        table.insert(&timestamp, bytes.as_slice())?;
        Ok(())
    }

    pub fn insert_index(&self, txn: &WriteTransaction, path: &str, entry: &crate::models::IndexEntry) -> Result<()> {
        use rkyv::ser::Serializer;
        let mut serializer = rkyv::ser::serializers::AllocSerializer::<256>::default();
        serializer.serialize_value(entry).unwrap();
        let bytes = serializer.into_serializer().into_inner();
        
        let mut table = txn.open_table(INDEX_TABLE)?;
        table.insert(path, bytes.as_slice())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_operations() -> Result<()> {
        let temp_file = tempfile::NamedTempFile::new()?;
        let db = BackupDb::new(temp_file.path())?;

        // Test Blob Insert
        let hash = [1u8; 32];
        let location = BlobLocation { tape_id: 100, offset: 200 };

        let write_txn = db.begin_write()?;
        db.insert_blob(&write_txn, &hash, &location)?;
        write_txn.commit()?;

        // Test Blob Get
        let retrieved = db.get_blob(&hash)?;
        assert_eq!(retrieved, Some(location));

        Ok(())
    }
}
