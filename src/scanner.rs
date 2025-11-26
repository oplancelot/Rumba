use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use jwalk::WalkDir;
use tracing::{debug, info, warn};
use globset::{Glob, GlobSet, GlobSetBuilder};

#[derive(Debug)]
pub struct ScannedDir {
    pub path: PathBuf,
    pub entries: Vec<ScannedEntry>,
}

#[derive(Debug, Clone)]
pub struct ScannedEntry {
    pub name: String,
    pub is_dir: bool,
    // We capture minimal metadata needed for sorting and initial processing
    pub path: PathBuf, 
}

pub struct Scanner {
    root: PathBuf,
    excludes: GlobSet,
}

impl Scanner {
    pub fn new(root: PathBuf, exclude_patterns: &[String]) -> anyhow::Result<Self> {
        let mut builder = GlobSetBuilder::new();
        for pattern in exclude_patterns {
            builder.add(Glob::new(pattern)?);
        }
        let excludes = builder.build()?;
        
        Ok(Self { root, excludes })
    }

    pub fn scan_parallel(&self, tx: Sender<ScannedDir>) -> anyhow::Result<()> {
        let excludes = self.excludes.clone();
        
        WalkDir::new(&self.root)
            .sort(true)
            .process_read_dir(move |_depth, _path, _state, children| {
                // Print current directory being scanned
                // Use debug! to avoid spamming unless requested
                debug!("Scanning: {:?}", _path);

                // 1. Filter excluded files/directories
                // We filter in-place so jwalk doesn't recurse into excluded directories
                let mut i = 0;
                while i < children.len() {
                    let should_remove = if let Ok(child) = &children[i] {
                        if excludes.is_match(child.path()) {
                            debug!("Skipping excluded item: {:?}", child.path());
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    if should_remove {
                        let _ = children.remove(i);
                    } else {
                        i += 1;
                    }
                }

                // 2. Collect sorted entries to send
                // jwalk .sort(true) sorts by name, but we want to be sure and convert to our type
                // Also, we need to ensure consistent ordering for our own processing if needed.
                // Since we modified the list (removed items), the sort order of remaining items is preserved if it was sorted.
                // But let's be safe.
                children.sort_by(|a, b| {
                    match (a, b) {
                        (Ok(a), Ok(b)) => a.file_name().cmp(b.file_name()),
                        (Ok(_), Err(_)) => Ordering::Less,
                        (Err(_), Ok(_)) => Ordering::Greater,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                });

                let mut entries = Vec::with_capacity(children.len());
                
                for child in children.iter() {
                    if let Ok(child) = child {
                        entries.push(ScannedEntry {
                            name: child.file_name().to_string_lossy().to_string(),
                            is_dir: child.file_type().is_dir(),
                            path: child.path(),
                        });
                    }
                }

                // 3. Send the sorted directory listing
                // We use the path from the callback arguments
                debug!("Scanning directory: {:?} ({} entries)", _path, entries.len());
                if let Err(e) = tx.send(ScannedDir {
                    path: _path.to_path_buf(),
                    entries,
                }) {
                    debug!("Scanner channel closed: {}", e);
                }
            })
            .into_iter()
            .for_each(|_| {}); // Drive the iterator

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    use std::sync::mpsc;

    #[test]
    fn test_deterministic_scan() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        // Create a structure with mixed order creation
        fs::create_dir(root.join("b_dir"))?;
        fs::write(root.join("c_file.txt"), "content")?;
        fs::write(root.join("a_file.txt"), "content")?;
        fs::create_dir(root.join("b_dir").join("sub_a"))?;
        fs::write(root.join("b_dir").join("sub_b.txt"), "content")?;

        let scanner = Scanner::new(root.to_path_buf(), &[])?;
        let (tx, rx) = mpsc::channel();

        scanner.scan_parallel(tx)?;

        // Collect all results
        let mut results: Vec<ScannedDir> = rx.into_iter().collect();
        
        // Sort results by path to make assertion easy
        results.sort_by(|a, b| a.path.cmp(&b.path));

        // Verify Root Directory
        let root_dir = results.iter().find(|d| d.path == root).expect("Root not found");
        assert_eq!(root_dir.entries.len(), 3);
        assert_eq!(root_dir.entries[0].name, "a_file.txt");
        assert_eq!(root_dir.entries[1].name, "b_dir");
        assert_eq!(root_dir.entries[2].name, "c_file.txt");

        Ok(())
    }

    #[test]
    fn test_scan_with_excludes() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        fs::write(root.join("keep.txt"), "content")?;
        fs::write(root.join("ignore.tmp"), "content")?;
        fs::create_dir(root.join("ignore_dir"))?;
        fs::write(root.join("ignore_dir").join("file.txt"), "content")?;

        // Note: globset matches against the path. 
        // If we want to exclude a directory, we should match it.
        let excludes = vec!["**/*.tmp".to_string(), "**/ignore_dir".to_string()];
        let scanner = Scanner::new(root.to_path_buf(), &excludes)?;
        let (tx, rx) = mpsc::channel();

        scanner.scan_parallel(tx)?;

        let results: Vec<ScannedDir> = rx.into_iter().collect();
        
        let root_dir = results.iter().find(|d| d.path == root).expect("Root not found");
        
        let names: Vec<String> = root_dir.entries.iter().map(|e| e.name.clone()).collect();
        assert!(names.contains(&"keep.txt".to_string()));
        assert!(!names.contains(&"ignore.tmp".to_string()));
        assert!(!names.contains(&"ignore_dir".to_string()));

        // Ensure ignore_dir was not scanned
        let ignore_dir_scanned = results.iter().any(|d| d.path.ends_with("ignore_dir"));
        assert!(!ignore_dir_scanned);

        Ok(())
    }

    #[test]
    fn test_scan_with_excludes_deep() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let root = temp_dir.path();

        fs::create_dir(root.join("deep_ignore"))?;
        fs::create_dir(root.join("deep_ignore").join("subdir"))?;
        fs::write(root.join("deep_ignore").join("subdir").join("file.txt"), "content")?;

        // Pattern with trailing /**
        // Note: In globset, **/foo/** matches anything inside foo, but not foo itself usually.
        let excludes = vec!["**/deep_ignore/**".to_string()];
        let scanner = Scanner::new(root.to_path_buf(), &excludes)?;
        let (tx, rx) = mpsc::channel();

        scanner.scan_parallel(tx)?;

        let results: Vec<ScannedDir> = rx.into_iter().collect();
        
        // We expect deep_ignore to be scanned (as directory itself might not match), 
        // but its contents (subdir) should match and be removed.
        // So deep_ignore should be empty.
        
        // Find deep_ignore dir in results
        let deep_dir = results.iter().find(|d| d.path.ends_with("deep_ignore"));
        
        if let Some(d) = deep_dir {
            assert!(d.entries.is_empty(), "deep_ignore should be empty but has {:?}", d.entries);
        }
        
        // Ensure subdir was NOT scanned as a directory (because it was removed from children of deep_ignore)
        let subdir_scanned = results.iter().any(|d| d.path.ends_with("subdir"));
        assert!(!subdir_scanned, "subdir should not be scanned");

        Ok(())
    }
}
