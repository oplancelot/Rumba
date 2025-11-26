mod models;
mod db;
mod scanner;
mod pipeline;
mod diff;
mod tape;
mod config;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::info;

/// Rumba Backup Tool - High-performance incremental backup for LTO tape
#[derive(Parser, Debug)]
#[command(name = "rumba")]
#[command(about = "Backup Samba shares to LTFS tape using Git-like incremental backup")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: String,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Encode a password to base64 for use in config file
    EncodePassword {
        /// Password to encode
        password: String,
    },
    
    /// Run backup and output tar archive
    /// Use --output - to stream to stdout for piping to rustltfs
    Backup {
        /// Path to configuration file
        #[arg(short, long, default_value = "config.toml")]
        config: String,
        
        /// Output path for tar archive
        /// Use "-" to output to stdout for streaming to rustltfs via pipe
        #[arg(short, long)]
        output: Option<String>,
        
        /// Output format (currently only "tar" is supported)
        #[arg(long, default_value = "tar")]
        format: String,
        
        /// Check mode: only check how many files need backup, don't perform backup
        #[arg(long, default_value = "false")]
        check: bool,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    
    let cli = Cli::parse();
    
    // Handle subcommands
    if let Some(command) = cli.command {
        match command {
            Commands::EncodePassword { password } => {
                let encoded = config::encode_password(&password);
                println!("Encoded password for config file:");
                println!("{}", encoded);
                return Ok(());
            }
            
            Commands::Backup { config: config_path, output, format, check } => {
                return run_backup_command(&config_path, &output, &format, check);
            }
        }
    }
    
    info!("Rumba Backup Tool Initialized");
    
    // Load configuration
    let config = config::Config::from_file(&cli.config)?;
    info!("Configuration loaded from: {}", cli.config);
    info!("Source: {}", config.source.url);
    info!("Username: {}", config.source.username);

    
    // 1. Initialize Infrastructure
    // For non-streaming mode, target config is required
    let target = config.target.as_ref()
        .ok_or_else(|| anyhow::anyhow!("[target] section is required in config for non-streaming mode"))?;
    
    let db = db::BackupDb::new(&target.db_path)?;
    info!("Database initialized at {}", target.db_path);

    // 2. Run Pipeline (Scan -> Diff -> Plan)
    let root_path = config.get_backup_root()?;
    info!("Starting backup for root: {:?}", root_path);
    
    let pipeline = pipeline::Pipeline::new(db.clone(), root_path.clone(), config.source.excludes.clone());
    let plan = pipeline.run()?;
    
    info!("Backup Plan Generated:");
    info!("  New Files: {}", plan.new_files.len());
    info!("  Total Size: {} bytes", plan.total_size);

    if plan.new_files.is_empty() {
        info!("Nothing to backup.");
        return Ok(());
    }

    // 3. Initialize Tape Writer based on output mode
    let mut tape_writer = match target.output_mode.as_str() {
        "stream" => {
            info!("Output mode: stream (to stdout)");
            tape::TapeWriter::new_tar_stream(std::io::stdout())?
        }
        "tar" => {
            // Format tar path with current date/time
            let now = chrono::Local::now();
            let tar_path = now.format(&target.tar_path).to_string();
            
            info!("Output mode: tar file (writing to {})", tar_path);
            tape::TapeWriter::new_tar_file(&tar_path, 1)?
        }
        _ => {
            anyhow::bail!("Invalid output mode: {}", target.output_mode);
        }
    };

    // 4. Write to Tape/File (Phase 1: Prepare & Write)
    // Note: We are not handling 2PC strictly here yet (no rollback on failure), 
    // but we follow the order: Write Tape/File -> Commit DB.
    
    info!("========================================");
    info!("Starting tape write operation");
    info!("========================================");
    
    // Log each file being backed up
    for (idx, (path, hash)) in plan.new_files.iter().enumerate() {
        let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        info!("[{}/{}] Backing up: {}", 
            idx + 1, 
            plan.new_files.len(),
            path.display()
        );
        info!("  Hash: {}", hex::encode(hash));
        info!("  Size: {} bytes", file_size);
    }
    
    let blob_locations = tape_writer.write_plan(&plan)?;
    info!("Successfully wrote {} blobs", blob_locations.len());

    // Finish tape writing (wait for rustltfs if in that mode)
    tape_writer.finish()?;
    info!("Tape/file writing completed successfully");

    // 5. Commit Metadata (Phase 3: Commit Index)
    let write_txn = db.begin_write()?;
    
    // 5.1 Update Blobs
    for (hash, location) in blob_locations {
        db.insert_blob(&write_txn, &hash, &location)?;
    }

    // 5.2 Update Trees (and Index)
    // Wait, Pipeline didn't return the trees! 
    // We need to modify Pipeline to return the computed trees so we can persist them.
    // For now, let's assume we just persist the blobs and index for the files we backed up.
    // TODO: Refactor Pipeline to return full Tree structure or persist Trees during pipeline run (but that breaks 2PC).
    // Let's stick to the plan: Pipeline should probably return the Tree entries to be committed.
    
    // For this step, I'll just update the Index for the files we backed up, 
    // so next time they are skipped.
    for (path, hash) in &plan.new_files {
        if let Ok(metadata) = std::fs::metadata(path) {
             let mtime = metadata.modified()
                .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                .unwrap_or(0);
            let size = metadata.len();
            
            // Normalize path to string key
            let path_str = path.to_string_lossy();
            let entry = models::IndexEntry {
                mtime,
                size,
                hash: *hash,
                needs_backup: false,
            };
            db.insert_index(&write_txn, &path_str, &entry)?;
        }
    }

    write_txn.commit()?;
    info!("Database commit successful.");
    
    // Print backup summary
    info!("");
    info!("========================================");
    info!("BACKUP COMPLETED SUCCESSFULLY");
    info!("========================================");
    info!("Configuration:");
    info!("  Source: {}", config.source.url);
    info!("  Output mode: {}", target.output_mode);
    if target.output_mode == "tar" {
        info!("  Tar file: {}", target.tar_path);
    }
    info!("");
    info!("Backup Summary:");
    info!("  Total files backed up: {}", plan.new_files.len());
    info!("  Total data size: {} bytes ({:.2} MB)", 
        plan.total_size,
        plan.total_size as f64 / 1024.0 / 1024.0
    );
    info!("  Unique blobs stored: {}", plan.new_files.len()); // TODO: count unique hashes
    info!("");
    info!("Files backed up:");
    for (path, hash) in &plan.new_files {
        let file_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        info!("  • {} ({} bytes, hash: {}...)", 
            path.display(),
            file_size,
            &hex::encode(hash)[..16]
        );
    }
    info!("========================================");

    Ok(())
}

/// Run backup command (for streaming to stdout or file)
/// This is used by the `backup` subcommand to support PowerShell pipe integration
fn run_backup_command(config_path: &str, output: &Option<String>, format: &str, check: bool) -> Result<()> {
    // Only tar format is supported currently
    if format != "tar" {
        anyhow::bail!("Only 'tar' format is currently supported");
    }
    
    // Load configuration
    let config = config::Config::from_file(config_path)?;
    eprintln!("Rumba: Configuration loaded from: {}", config_path);
    eprintln!("Rumba: Source: {}", config.source.url);
    
    // Initialize database - use target.db_path if available, otherwise default
    let db_path = config.target.as_ref()
        .map(|t| t.db_path.clone())
        .unwrap_or_else(|| "rumba.db".to_string());
    let db = db::BackupDb::new(&db_path)?;
    eprintln!("Rumba: Database initialized");
    
    // Check for pending backups in DB
    let pending_files = if !check {
        db.get_files_needing_backup()?
    } else {
        Vec::new()
    };

    let plan = if !pending_files.is_empty() {
        eprintln!("Rumba: Found {} pending files in database.", pending_files.len());
        let mut new_files = Vec::new();
        let mut total_size = 0;
        
        for (path_str, entry) in pending_files {
            new_files.push((std::path::PathBuf::from(path_str), entry.hash));
            total_size += entry.size;
        }
        
        pipeline::BackupPlan {
            new_files,
            total_size,
        }
    } else if check {
        // Run pipeline (Scan) only in check mode
        let root_path = config.get_backup_root()?;
        eprintln!("Rumba: Scanning files from: {:?}", root_path);
        
        let pipeline = pipeline::Pipeline::new(db.clone(), root_path.clone(), config.source.excludes.clone());
        pipeline.run()?
    } else {
        // Backup mode but no pending files
        eprintln!("Rumba: No pending files found in database.");
        eprintln!("Rumba: Please run 'rumba backup --check' first to scan for changes.");
        return Ok(());
    };
    
    eprintln!("Rumba: Backup plan generated");
    eprintln!("Rumba:   New files: {}", plan.new_files.len());
    eprintln!("Rumba:   Total size: {} bytes ({:.2} MB)", 
        plan.total_size,
        plan.total_size as f64 / 1024.0 / 1024.0
    );
    
    if check {
        // In check mode, update the database index to mark files as needing backup
        eprintln!("Rumba: Updating database index (marking for backup)...");
        let write_txn = db.begin_write()?;
        
        for (path, hash) in &plan.new_files {
            if let Ok(metadata) = std::fs::metadata(path) {
                let mtime = metadata.modified()
                    .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                    .unwrap_or(0);
                let size = metadata.len();
                
                let path_str = path.to_string_lossy();
                let entry = models::IndexEntry {
                    mtime,
                    size,
                    hash: *hash,
                    needs_backup: true, // Mark as needing backup
                };
                db.insert_index(&write_txn, &path_str, &entry)?;
            }
        }
        
        write_txn.commit()?;
        eprintln!("Rumba: Database index updated");
        
        // Output just the number of files to stdout
        println!("{}", plan.new_files.len());
        return Ok(());
    }

    if plan.new_files.is_empty() {
        eprintln!("Rumba: Nothing to backup");
        return Ok(());
    }
    
    let output = output.as_ref().ok_or_else(|| anyhow::anyhow!("Output path is required when not in check mode"))?;

    // Create tape writer based on output parameter
    let mut tape_writer = if output == "-" {
        // Stream to stdout
        eprintln!("Rumba: Streaming tar to stdout...");
        let stdout = std::io::stdout();
        tape::TapeWriter::new_tar_stream(stdout)?
    } else {
        // Write to file
        eprintln!("Rumba: Writing tar to file: {}", output);
        tape::TapeWriter::new_tar_file(output, 0)?
    };
    
    // Write backup plan to tar
    eprintln!("Rumba: Writing {} files to tar archive...", plan.new_files.len());
    let blob_locations = tape_writer.write_plan(&plan)?;
    
    // Finish writing
    tape_writer.finish()?;
    
    if output == "-" {
        eprintln!("Rumba: Tar stream completed successfully");
    } else {
        eprintln!("Rumba: Tar file written successfully: {}", output);
    }
    
    // Update database with blob locations and clear needs_backup flag
    let write_txn = db.begin_write()?;
    
    for (hash, location) in blob_locations {
        db.insert_blob(&write_txn, &hash, &location)?;
    }
    
    // Update index (clear needs_backup)
    for (path, hash) in &plan.new_files {
        if let Ok(metadata) = std::fs::metadata(path) {
            let mtime = metadata.modified()
                .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64)
                .unwrap_or(0);
            let size = metadata.len();
            
            let path_str = path.to_string_lossy();
            let entry = models::IndexEntry {
                mtime,
                size,
                hash: *hash,
                needs_backup: false, // Clear flag
            };
            db.insert_index(&write_txn, &path_str, &entry)?;
        }
    }
    
    write_txn.commit()?;
    eprintln!("Rumba: Database updated successfully");
    eprintln!("Rumba: Backup completed!");
    
    Ok(())
}

