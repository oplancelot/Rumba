use std::path::{Path, PathBuf};
use std::collections::HashMap;
use anyhow::Result;
use tracing::{info, debug};
use crate::scanner::{Scanner, ScannedDir};
use crate::db::BackupDb;
use crate::models::{Hash, FileMetadata, TreeEntry};
use crate::diff::DiffEngine;
use std::sync::mpsc;
use std::io::Read;

pub struct BackupPlan {
    pub new_files: Vec<(PathBuf, Hash)>,
    pub total_size: u64,
}

pub struct Pipeline {
    db: BackupDb,
    root: PathBuf,
}

impl Pipeline {
    pub fn new(db: BackupDb, root: PathBuf) -> Self {
        Self { db, root }
    }

    pub fn run(&self) -> Result<BackupPlan> {
        info!("Starting backup pipeline for {:?}", self.root);

        // 1. Scan
        let (tx, rx) = mpsc::channel();
        let scanner_root = self.root.clone();
        std::thread::spawn(move || {
            let scanner = Scanner::new(scanner_root);
            if let Err(e) = scanner.scan_parallel(tx) {
                tracing::error!("Scanner failed: {}", e);
            }
        });

        // 2. Ingest & Build Tree (Bottom-Up Strategy)
        let mut dir_map: HashMap<PathBuf, ScannedDir> = HashMap::new();
        
        for scanned_dir in rx {
            dir_map.insert(scanned_dir.path.clone(), scanned_dir);
        }

        // Sort paths by length descending (leaves first)
        let mut paths: Vec<PathBuf> = dir_map.keys().cloned().collect();
        paths.sort_by(|a, b| b.as_os_str().len().cmp(&a.as_os_str().len()));

        let mut new_files = Vec::new();
        let mut total_size = 0;

        // We need to store computed Tree Hashes for directories to use in their parents
        // Map<Path, Hash>
        let mut tree_hashes: HashMap<PathBuf, Hash> = HashMap::new();

        let diff_engine = DiffEngine::new(&self.db);

        for path in paths {
            if let Some(dir_info) = dir_map.get(&path) {
                let mut tree_entries = Vec::new();

                for entry in &dir_info.entries {
                    let entry_path = entry.path.clone();
                    
                    if entry.is_dir {
                        // It's a directory, look up its computed hash
                        if let Some(hash) = tree_hashes.get(&entry_path) {
                            tree_entries.push(TreeEntry {
                                name: entry.name.clone(),
                                mode: 0o040755, // Directory mode
                                hash: *hash,
                            });
                        } else {
                            debug!("Subdirectory hash not found for {:?}, assuming empty or error", entry_path);
                        }
                    } else {
                        // It's a file
                        match std::fs::metadata(&entry_path) {
                            Ok(fs_metadata) => {
                                let mtime = fs_metadata.modified()
                                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                                    .unwrap_or(0);
                                let size = fs_metadata.len();
                                
                                // 1. Check Index (Fast Path)
                                let content_hash = match diff_engine.check_index(&entry_path, mtime, size)? {
                                    Some(hash) => hash, // Clean
                                    None => {
                                        // Dirty: Compute Hash
                                        compute_file_hash(&entry_path)?
                                    }
                                };

                                // 2. Check Deduplication
                                // If we computed a new hash (or even if we got it from index, though less likely to be missing from blobs if in index),
                                // we should check if we need to back it up.
                                // Optimization: If check_index returned Some, we assume blob exists? 
                                // Safety: Always check blob existence if we want to be sure, or trust index implies blob existence.
                                // For robustness, let's check blob existence if it was dirty. 
                                // If it was clean, we trust the blob is there (unless GC happened, which is out of scope).
                                
                                // Actually, let's just check `should_backup_blob` if we computed the hash.
                                // If we got it from index, we assume it's already backed up.
                                let mut needs_backup = false;
                                if diff_engine.should_backup_blob(&content_hash)? {
                                    needs_backup = true;
                                }

                                let metadata = FileMetadata {
                                    size,
                                    mtime,
                                    mode: 0o100644, // TODO: Real mode
                                    uid: 0,
                                    gid: 0,
                                    content_hash,
                                };

                                if needs_backup {
                                    new_files.push((entry_path.clone(), content_hash));
                                    total_size += metadata.size;
                                }

                                tree_entries.push(TreeEntry {
                                    name: entry.name.clone(),
                                    mode: metadata.mode,
                                    hash: content_hash,
                                });
                            },
                            Err(e) => {
                                tracing::warn!("Failed to get metadata for {:?}: {}", entry_path, e);
                            }
                        }
                    }
                }

                // Compute Tree Hash
                tree_entries.sort_by(|a, b| a.name.cmp(&b.name));
                
                let mut hasher = blake3::Hasher::new();
                for entry in &tree_entries {
                    hasher.update(&entry.compute_hash());
                }
                let tree_hash = *hasher.finalize().as_bytes();
                
                tree_hashes.insert(path.clone(), tree_hash);
            }
        }

        Ok(BackupPlan {
            new_files,
            total_size,
        })
    }
}



fn compute_file_hash(path: &Path) -> Result<Hash> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 8192];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::BackupDb;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_pipeline_flow() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();
        let db_path = root.join("test.redb");
        let db = BackupDb::new(&db_path)?;

        // Create some files
        fs::write(root.join("file1.txt"), "content1")?;
        fs::create_dir(root.join("subdir"))?;
        fs::write(root.join("subdir").join("file2.txt"), "content2")?;

        let pipeline = Pipeline::new(db, root.to_path_buf());
        let plan = pipeline.run()?;

        // Verify plan
        // We expect 2 new files (since DB is empty)
        assert_eq!(plan.new_files.len(), 2);
        
        // Check if paths are correct (order might vary, so check existence)
        let paths: Vec<_> = plan.new_files.iter().map(|(p, _)| p.file_name().unwrap().to_str().unwrap()).collect();
        assert!(paths.contains(&"file1.txt"));
        assert!(paths.contains(&"file2.txt"));

        Ok(())
    }
}
