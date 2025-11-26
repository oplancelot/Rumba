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
use rayon::prelude::*;

pub struct BackupPlan {
    pub new_files: Vec<(PathBuf, Hash)>,
    pub total_size: u64,
}

pub struct Pipeline {
    db: BackupDb,
    root: PathBuf,
    excludes: Vec<String>,
}

impl Pipeline {
    pub fn new(db: BackupDb, root: PathBuf, excludes: Vec<String>) -> Self {
        Self { db, root, excludes }
    }

    pub fn run(&self) -> Result<BackupPlan> {
        info!("Starting backup pipeline for {:?}", self.root);

        // 1. Scan
        let (tx, rx) = mpsc::channel();
        let scanner_root = self.root.clone();
        let excludes = self.excludes.clone();
        std::thread::spawn(move || {
            // Scanner::new might fail if patterns are invalid, but we validated config earlier.
            // However, Scanner::new returns Result.
            match Scanner::new(scanner_root, &excludes) {
                Ok(scanner) => {
                    if let Err(e) = scanner.scan_parallel(tx) {
                        tracing::error!("Scanner failed: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to initialize scanner: {}", e);
                }
            }
        });

        // 2. Ingest & Build Tree (Bottom-Up Strategy)
        let mut dir_map: HashMap<PathBuf, ScannedDir> = HashMap::new();
        let mut scanned_count = 0;
        
        for scanned_dir in rx {
            dir_map.insert(scanned_dir.path.clone(), scanned_dir);
            scanned_count += 1;
            if scanned_count % 100 == 0 {
                info!("Received scan results for {} directories...", scanned_count);
            }
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
        
        let total_dirs = paths.len();
        info!("Processing {} directories...", total_dirs);
        let mut processed = 0;

        for path in paths {
            if let Some(dir_info) = dir_map.get(&path) {
                // 1. Parallel process files
                let file_results: Vec<Result<Option<(TreeEntry, Option<(PathBuf, Hash, u64)>)>>> = dir_info.entries.par_iter()
                    .filter(|e| !e.is_dir)
                    .map(|entry| {
                        let entry_path = entry.path.clone();
                        match std::fs::metadata(&entry_path) {
                            Ok(fs_metadata) => {
                                let mtime = fs_metadata.modified()
                                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                                    .unwrap_or(0);
                                let size = fs_metadata.len();
                                
                                // Check Index (Fast Path)
                                let content_hash = match diff_engine.check_index(&entry_path, mtime, size)? {
                                    Some(hash) => hash,
                                    None => compute_file_hash(&entry_path)?
                                };

                                let mut needs_backup = false;
                                if diff_engine.should_backup_blob(&content_hash)? {
                                    needs_backup = true;
                                }

                                let metadata = FileMetadata {
                                    size,
                                    mtime,
                                    mode: 0o100644,
                                    uid: 0,
                                    gid: 0,
                                    content_hash,
                                };

                                let new_file = if needs_backup {
                                    Some((entry_path.clone(), content_hash, size))
                                } else {
                                    None
                                };

                                let tree_entry = TreeEntry {
                                    name: entry.name.clone(),
                                    mode: metadata.mode,
                                    hash: content_hash,
                                };
                                
                                Ok(Some((tree_entry, new_file)))
                            },
                            Err(e) => {
                                tracing::warn!("Failed to get metadata for {:?}: {}", entry_path, e);
                                Ok(None)
                            }
                        }
                    })
                    .collect();

                let mut tree_entries = Vec::new();

                // Collect file results
                for res in file_results {
                    match res {
                        Ok(Some((entry, new_file))) => {
                            tree_entries.push(entry);
                            if let Some((p, h, s)) = new_file {
                                new_files.push((p, h));
                                total_size += s;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => tracing::error!("Error processing file: {}", e),
                    }
                }

                // 2. Process subdirectories (Serial, needs tree_hashes)
                for entry in &dir_info.entries {
                    if entry.is_dir {
                        let entry_path = entry.path.clone();
                        if let Some(hash) = tree_hashes.get(&entry_path) {
                            tree_entries.push(TreeEntry {
                                name: entry.name.clone(),
                                mode: 0o040755,
                                hash: *hash,
                            });
                        } else {
                            debug!("Subdirectory hash not found for {:?}, assuming empty or error", entry_path);
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
                
                processed += 1;
                if processed % 50 == 0 || processed == total_dirs {
                    info!("Processed {}/{} directories. Current: {:?}", processed, total_dirs, path);
                }
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

        let pipeline = Pipeline::new(db, root.to_path_buf(), vec![]);
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
