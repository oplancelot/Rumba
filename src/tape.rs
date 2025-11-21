use anyhow::Result;
use std::io::Write;
use std::process::{Command, Stdio, Child};
use tar::Builder;
use std::collections::HashMap;
use crate::models::{Hash, BlobLocation};

pub enum TapeOutput {
    /// Write to rustltfs process via pipe
    RustLtfs(Child),
    /// Write to tar file
    TarFile(std::fs::File),
}

pub struct TapeWriter {
    output: TapeOutput,
    tape_id: u64,
    current_offset: u64,
}

impl TapeWriter {
    /// Create a new TapeWriter that pipes to rustltfs process
    pub fn new_rustltfs(rustltfs_path: &str, device_path: &str, tape_id: u64) -> Result<Self> {
        let child = Command::new(rustltfs_path)
            .arg("write")
            .arg("--device")
            .arg(device_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        
        Ok(Self {
            output: TapeOutput::RustLtfs(child),
            tape_id,
            current_offset: 0,
        })
    }
    
    /// Create a new TapeWriter that writes to a tar file
    pub fn new_tar_file(file_path: &str, tape_id: u64) -> Result<Self> {
        let file = std::fs::File::create(file_path)?;
        
        Ok(Self {
            output: TapeOutput::TarFile(file),
            tape_id,
            current_offset: 0,
        })
    }
    
    /// Write the backup plan to tape/file
    /// Returns a map of file hashes to their locations on tape
    pub fn write_plan(&mut self, plan: &crate::pipeline::BackupPlan) -> Result<HashMap<Hash, BlobLocation>> {
        let mut blob_locations = HashMap::new();
        
        // Get the writer based on output mode
        let writer: Box<dyn Write> = match &mut self.output {
            TapeOutput::RustLtfs(child) => {
                Box::new(child.stdin.take().expect("Failed to get rustltfs stdin"))
            }
            TapeOutput::TarFile(file) => {
                Box::new(file.try_clone()?)
            }
        };
        
        let mut tar_builder = Builder::new(writer);
        
        for (path, hash) in &plan.new_files {
            // Record the current offset before writing
            let offset = self.current_offset;
            
            // Read the file content
            let content = std::fs::read(path)?;
            let size = content.len() as u64;
            
            // Create a tar header
            let mut header = tar::Header::new_gnu();
            header.set_size(size);
            header.set_mode(0o644);
            header.set_cksum();
            
            // Use "original_filename_hash" as tar entry name for content-addressable storage
            let filename = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unnamed");
            let hash_str = hex::encode(hash);
            let tar_entry_name = format!("{}_{}", filename, &hash_str[..16]); // Use first 16 chars of hash
            
            tar_builder.append_data(&mut header, &tar_entry_name, content.as_slice())?;
            
            // Calculate new offset (tar adds 512-byte headers and rounds to 512-byte blocks)
            let header_size = 512u64;
            let data_blocks = (size + 511) / 512;  // Round up
            self.current_offset = offset + header_size + (data_blocks * 512);
            
            // Record blob location
            blob_locations.insert(*hash, BlobLocation {
                tape_id: self.tape_id,
                offset,
            });
        }
        
        // Finish the tar archive
        tar_builder.finish()?;
        
        Ok(blob_locations)
    }
    
    /// Finish writing and clean up
    pub fn finish(self) -> Result<()> {
        match self.output {
            TapeOutput::RustLtfs(mut child) => {
                // Wait for rustltfs process to complete
                let status = child.wait()?;
                if !status.success() {
                    anyhow::bail!("rustltfs process failed with status: {}", status);
                }
                Ok(())
            }
            TapeOutput::TarFile(file) => {
                // Sync and close the file
                file.sync_all()?;
                drop(file);
                Ok(())
            }
        }
    }
}
