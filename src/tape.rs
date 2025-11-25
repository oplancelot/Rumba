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
    /// Write to any stream (e.g., stdout for piping to rustltfs)
    TarStream(Box<dyn Write>),
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
    
    /// Create a new TapeWriter that writes to a stream (e.g., stdout)
    /// This enables zero-temp-file streaming via PowerShell pipes:
    /// `rumba backup --output - | rustltfs write --device TAPE0 --destination /path`
    pub fn new_tar_stream<W: Write + 'static>(writer: W) -> Result<Self> {
        Ok(Self {
            output: TapeOutput::TarStream(Box::new(writer)),
            tape_id: 0,  // tape_id not relevant for streaming
            current_offset: 0,
        })
    }
    
    /// Write the backup plan to tape/file
    /// Returns a map of file hashes to their locations on tape
    pub fn write_plan(&mut self, plan: &crate::pipeline::BackupPlan) -> Result<HashMap<Hash, BlobLocation>> {
        let mut blob_locations = HashMap::new();
        
        // For TarStream, we need to take ownership of the Box<dyn Write>
        // We'll use mem::replace to temporarily swap it out
        let needs_stream_handling = matches!(self.output, TapeOutput::TarStream(_));
        
        if needs_stream_handling {
            // Take ownership of the stream using mem::replace
            let dummy_output = TapeOutput::TarFile(std::fs::File::create("/dev/null").unwrap_or_else(|_| {
                // Fallback for Windows
                std::fs::File::create("NUL").expect("Failed to create dummy file")
            }));
            let original_output = std::mem::replace(&mut self.output, dummy_output);
            
            if let TapeOutput::TarStream(stream) = original_output {
                let mut tar_builder = Builder::new(stream);
                self.build_tar_archive(&mut tar_builder, plan, &mut blob_locations)?;
                if let Err(e) = tar_builder.finish() {
                    // Ignore "Incorrect function" (os error 1) which happens on Windows pipes
                    if let Some(os_err) = e.raw_os_error() {
                        if os_err != 1 {
                            return Err(e.into());
                        }
                    } else {
                        return Err(e.into());
                    }
                }
                // Don't restore output, it's consumed
            }
        } else {
            // Get the writer based on output mode
            let writer: Box<dyn Write> = match &mut self.output {
                TapeOutput::RustLtfs(child) => {
                    Box::new(child.stdin.take().expect("Failed to get rustltfs stdin"))
                }
                TapeOutput::TarFile(file) => {
                    Box::new(file.try_clone()?)
                }
                TapeOutput::TarStream(_) => unreachable!(), // Handled above
            };
            
            let mut tar_builder = Builder::new(writer);
            self.build_tar_archive(&mut tar_builder, plan, &mut blob_locations)?;
            if let Err(e) = tar_builder.finish() {
                 if let Some(os_err) = e.raw_os_error() {
                    if os_err != 1 {
                        return Err(e.into());
                    }
                } else {
                    return Err(e.into());
                }
            }
        }
        
        Ok(blob_locations)
    }
    
    /// Helper method to build tar archive (extracted for reuse)
    fn build_tar_archive<W: Write>(
        &mut self,
        tar_builder: &mut Builder<W>,
        plan: &crate::pipeline::BackupPlan,
        blob_locations: &mut HashMap<Hash, BlobLocation>,
    ) -> Result<()> {
        for (path, hash) in &plan.new_files {
            // Deduplication: If we've already written this hash in this session, skip it.
            if blob_locations.contains_key(hash) {
                continue;
            }

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
        
        Ok(())
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
                // Ignore "Incorrect function" which might happen for NUL device
                if let Err(e) = file.sync_all() {
                    if let Some(os_err) = e.raw_os_error() {
                        if os_err != 1 {
                            return Err(e.into());
                        }
                    }
                }
                drop(file);
                Ok(())
            }
            TapeOutput::TarStream(mut stream) => {
                // Flush the stream to ensure all data is written
                // Ignore errors on flush (e.g. "Incorrect function" on Windows pipes)
                let _ = stream.flush();
                drop(stream);
                Ok(())
            }
        }
    }
}
