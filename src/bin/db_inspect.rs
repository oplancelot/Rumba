use anyhow::Result;
use clap::{Parser, Subcommand};
use rumba::{models, db};
use redb::ReadableTable;
use rkyv::Deserialize;

/// Database inspection tool for Rumba backup
#[derive(Parser, Debug)]
#[command(name = "db-inspect")]
#[command(about = "Inspect Rumba backup database contents")]
struct Cli {
    /// Path to database file
    #[arg(short, long, default_value = "backup_meta.redb")]
    db_path: String,
    
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show database statistics
    Stats,
    /// List all blobs
    ListBlobs,
    /// List index entries
    ListIndex {
        /// Optional path filter
        #[arg(short, long)]
        filter: Option<String>,
    },
    /// Show a specific index entry
    ShowIndex {
        /// Path to show
        path: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    let db = db::BackupDb::new(&cli.db_path)?;
    
    match cli.command {
        Commands::Stats => show_stats(&db)?,
        Commands::ListBlobs => list_blobs(&db)?,
        Commands::ListIndex { filter } => list_index(&db, filter.as_deref())?,
        Commands::ShowIndex { path } => show_index(&db, &path)?,
    }
    
    Ok(())
}

fn show_stats(db: &db::BackupDb) -> Result<()> {
    let txn = db.begin_read()?;
    
    // Count blobs
    let blobs_table = txn.open_table(db::BLOBS_TABLE)?;
    let blob_count = blobs_table.iter()?.count();
    
    // Count index entries
    let index_table = txn.open_table(db::INDEX_TABLE)?;
    let index_count = index_table.iter()?.count();
    
    println!("Database Statistics");
    println!("==================");
    println!("Database path: {}", db.path().display());
    println!("Blobs count:   {}", blob_count);
    println!("Index entries: {}", index_count);
    
    Ok(())
}

fn list_blobs(db: &db::BackupDb) -> Result<()> {
    let txn = db.begin_read()?;
    let table = txn.open_table(db::BLOBS_TABLE)?;
    
    println!("Blobs in database:");
    println!("{:<66} {:>10} {:>10}", "Hash", "Tape ID", "Offset");
    println!("{}", "=".repeat(90));
    
    for result in table.iter()? {
        let (hash_bytes, location_bytes) = result?;
        let hash = hash_bytes.value();
        let location_value = location_bytes.value();
        
        // Copy to owned Vec for proper alignment
        let bytes = location_value.to_vec();
        let archived = unsafe { rkyv::archived_root::<models::BlobLocation>(&bytes) };
        let location: models::BlobLocation = archived.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize blob location: {}", e))?;
        
        println!("{} {:>10} {:>10}", 
            hex::encode(hash), 
            location.tape_id, 
            location.offset
        );
    }
    
    Ok(())
}

fn list_index(db: &db::BackupDb, filter: Option<&str>) -> Result<()> {
    let txn = db.begin_read()?;
    let table = txn.open_table(db::INDEX_TABLE)?;
    
    println!("Index entries:");
    println!("{:<50} {:>12} {:>15} {}", "Path", "Size", "Mtime", "Hash (first 16 bytes)");
    println!("{}", "=".repeat(120));
    
    for result in table.iter()? {
        let (path_bytes, entry_bytes) = result?;
        let path = path_bytes.value();
        
        // Apply filter if specified
        if let Some(filter_str) = filter {
            if !path.contains(filter_str) {
                continue;
            }
        }
        
        let entry_value = entry_bytes.value();
        
        // Copy to owned Vec to ensure proper alignment
        // redb returns 4-byte aligned data, but rkyv needs proper alignment
        let bytes = entry_value.to_vec();
        let archived = unsafe { rkyv::archived_root::<models::IndexEntry>(&bytes) };
        let entry: models::IndexEntry = archived.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize index entry: {}", e))?;
        
        println!("{:<50} {:>12} {:>15} {}", 
            if path.len() > 50 { path[..47].to_string() + "..." } else { path.to_string() },
            entry.size,
            entry.mtime,
            hex::encode(&entry.hash[..16])
        );
    }
    
    Ok(())
}

fn show_index(db: &db::BackupDb, path: &str) -> Result<()> {
    let entry = db.get_index_entry(path)?;
    
    match entry {
        Some(e) => {
            println!("Index entry for: {}", path);
            println!("================");
            println!("Size:  {} bytes", e.size);
            println!("Mtime: {} (timestamp)", e.mtime);
            println!("Hash:  {}", hex::encode(e.hash));
        }
        None => {
            println!("No index entry found for: {}", path);
        }
    }
    
    Ok(())
}
