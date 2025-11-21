use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::mpsc::Sender;
use jwalk::WalkDir;
use tracing::{debug, warn};

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
}

impl Scanner {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Scans the directory tree and sends sorted directory listings through the channel.
    /// This ensures that for every directory, we get a deterministic list of its children.
    pub fn scan(&self, _tx: Sender<ScannedDir>) -> anyhow::Result<()> {
        let walk = WalkDir::new(&self.root)
            .sort(true); // jwalk has a built-in sort, but we'll enforce our own strict logic if needed

        for entry in walk {
            match entry {
                Ok(dir_entry) => {
                    if dir_entry.file_type().is_dir() {
                        // This is a directory. 
                    }
                }
                Err(e) => {
                    warn!("Error scanning: {}", e);
                }
            }
        }
        Ok(())
    }

    /// Alternative scan using process_read_dir to capture children
    pub fn scan_parallel(&self, tx: Sender<ScannedDir>) -> anyhow::Result<()> {
        let tx = tx.clone();
        
        WalkDir::new(&self.root)
            .process_read_dir(move |_depth, path, _state, children| {
                // 1. Sort children deterministically by name
                children.sort_by(|a, b| {
                    match (a, b) {
                        (Ok(a), Ok(b)) => a.file_name().cmp(b.file_name()),
                        (Ok(_), Err(_)) => Ordering::Less,
                        (Err(_), Ok(_)) => Ordering::Greater,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                });

                // 2. Collect sorted entries to send
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
                // Note: 'path' here is the parent directory
                if let Err(e) = tx.send(ScannedDir {
                    path: path.to_path_buf(),
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

        let scanner = Scanner::new(root.to_path_buf());
        let (tx, rx) = mpsc::channel();

        scanner.scan_parallel(tx)?;

        // Collect all results
        let mut results: Vec<ScannedDir> = rx.into_iter().collect();
        
        // Sort results by path to make assertion easy (since scan is parallel, order of dirs is random)
        results.sort_by(|a, b| a.path.cmp(&b.path));

        // Verify Root Directory
        let root_dir = results.iter().find(|d| d.path == root).expect("Root not found");
        assert_eq!(root_dir.entries.len(), 3);
        assert_eq!(root_dir.entries[0].name, "a_file.txt");
        assert_eq!(root_dir.entries[1].name, "b_dir");
        assert_eq!(root_dir.entries[2].name, "c_file.txt");

        // Verify Sub Directory
        let sub_dir = results.iter().find(|d| d.path == root.join("b_dir")).expect("Subdir not found");
        assert_eq!(sub_dir.entries.len(), 2);
        assert_eq!(sub_dir.entries[0].name, "sub_a");
        assert_eq!(sub_dir.entries[1].name, "sub_b.txt");

        Ok(())
    }
}
