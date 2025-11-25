# Rumba - Rust LTFS Git-like Backup Tool

High-performance incremental backup tool for LTO tape using Git-inspired content-addressable storage.

[中文文档](README_CN.md)

## Features

- ✅ **Configuration Management**: TOML-based configuration file support
- ✅ **SMB Credential Management**: Username/password authentication for Samba shares
- ✅ **Password Obfuscation**: Base64 encoding for password protection in config files
- ✅ **Incremental Backup**: Content-based deduplication using file hashes
- ✅ **redb Metadata Storage**: Embedded database for backup metadata
- ✅ **Git-like Mechanism**: Content-Addressable Storage (CAS) + Merkle Tree
- ✅ **Streaming Backup**: Zero-temp-file streaming to tape via PowerShell pipes
- 🚧 **LTFS Integration**: Integration with rustltfs for real tape writing

## Quick Start

### 1. Configuration

Copy the example configuration and edit:

```bash
copy config.example.toml config.toml
```

Edit `config.toml` with your settings:

```toml
[source]
url = "\\\\server\\share\\path"
username = "your_username"
password = "your_password"  # or use base64 encoding

[target]
output_mode = "tar"
tape_path = "backup.tar"
db_path = "backup_meta.redb"

[tape]
device = "\\\\.\\TAPE0"  # Windows: \\.\TAPE0, Linux: /dev/sg0
rumba_path = "rumba"
rustltfs_path = "rustltfs"
database_path = "rumba.db"
```

### 2. Password Encoding (Optional)

To avoid storing passwords in plain text:

```bash
cargo run --bin rumba -- encode-password "your_password"
```

Copy the `base64:xxx` output to the `password` field in config.

### 3. Run Backup

#### Method 1: Direct tar output

```bash
cargo run --bin rumba -- backup --config config.toml --output backup.tar
```

#### Method 2: Streaming to tape (recommended)

```powershell
# Simple usage - all parameters from config file
.\scripts\backup-streaming.ps1 -ConfigFile config.toml
```

This will:
- Stream tar data directly from Rumba to rustltfs (zero temp files)
- Write to tape device specified in config
- Backup database metadata
- Generate logs

### 4. Inspect Database

Use the `db-inspect` tool to view backup metadata:

```bash
# Show statistics
cargo run --bin db-inspect -- stats

# List all blobs
cargo run --bin db-inspect -- list-blobs

# List index entries
cargo run --bin db-inspect -- list-index
```

## Architecture

### Core Design Principles

Rumba borrows from Git's internal mechanisms, designed for massive file backups:

1. **Content-Addressable Storage (CAS)**:
   - Files are stored by content hash (BLAKE3), not filename
   - **Automatic deduplication**: Identical content stored only once
   - **Data integrity**: Hash serves as checksum, preventing silent corruption

2. **Efficient Incremental Backup**:
   - **Level 1 - Quick Check**: Compare file `mtime` and `size` (like Git Index)
   - **Level 2 - Content Check**: If metadata changed, compute content hash and check `blobs` table
   - **Level 3 - Data Write**: Only new content blocks are written to tape

3. **Metadata Separation**:
   - File content streamed to tape (linear storage, optimal for LTO)
   - File metadata (names, permissions, directory structure) in fast local KV database (redb)

### System Architecture

```mermaid
graph TD
    Source[SMB Share] -->|Parallel scan| Scanner(Scanner)
    Scanner -->|Sorted directory stream| Pipeline{Pipeline}
    
    subgraph Pipeline Process
        Pipeline -->|Bottom-up tree build| TreeBuilder[Merkle Tree Builder]
        TreeBuilder -->|1. Check mtime/size| IndexCheck{Index Check}
        IndexCheck -->|Unchanged| Skip[Skip]
        IndexCheck -->|Changed| Hasher[Compute BLAKE3]
        Hasher -->|2. Check content hash| DedupCheck{Dedup Check}
        DedupCheck -->|Hash exists| UpdateIdx[Update Index Only]
        DedupCheck -->|New hash| NewFile[Add to Backup Plan]
    end
    
    NewFile --> BackupPlan[Generate Backup Plan]
    BackupPlan --> TapeWriter(Tape Writer)
    TapeWriter -->|Stream tar| Output[Output: rustltfs / tar]
    TapeWriter -.->|3. Transaction commit| DB[(Redb Metadata DB)]
    
    DB <--> IndexCheck
    DB <--> DedupCheck
```

## Streaming Backup

Rumba supports **zero-temp-file streaming** for optimal performance:

```powershell
# Rumba generates tar → PowerShell pipe → rustltfs writes to tape
rumba backup --config config.toml --output - | `
    rustltfs write --device \\.\TAPE0 --destination /incremental_20251125/backup.tar
```

**Benefits**:
- ✅ Zero temporary files
- ✅ Reduced disk I/O by 66%
- ✅ Real-time streaming
- ✅ Memory-efficient

See [scripts/README.md](scripts/README.md) for detailed usage.

## Project Structure

```
Rumba/
├── src/
│   ├── main.rs          # Main entry point
│   ├── lib.rs           # Library interface
│   ├── config.rs        # Configuration management
│   ├── models.rs        # Data structure definitions
│   ├── db.rs            # redb database operations
│   ├── scanner.rs       # File scanner
│   ├── pipeline.rs      # Backup pipeline
│   ├── diff.rs          # Diff engine
│   ├── tape.rs          # Tape writer
│   └── bin/
│       └── db_inspect.rs # Database inspection tool
├── scripts/
│   ├── backup-streaming.ps1  # Streaming backup script
│   └── restore-from-tape.ps1 # Restore script
├── config.example.toml   # Configuration example
└── Cargo.toml
```

## Configuration

### [source] - Backup Source

- `url`: SMB share path (Windows UNC format)
- `username`: SMB username
- `password`: SMB password (plain text or base64 encoded)
- `excludes`: Glob patterns to exclude from backup

### [target] - Backup Target

- `output_mode`: "rustltfs" or "tar"
- `tape_path`: Tape device path or tar file path
- `db_path`: Metadata database path

### [backup] - Backup Behavior

- `parallel_threads`: Number of parallel scanning threads (default: CPU cores)
- `compression_level`: Zstd compression level 0-22 (default: 3)

### [tape] - Tape Device Configuration

- `device`: Tape device path (Windows: `\\\\.\\TAPE0`, Linux: `/dev/sg0`)
- `rumba_path`: Path to rumba executable
- `rustltfs_path`: Path to rustltfs executable
- `database_path`: Database file path
- `skip_database`: Skip database backup
- `email_notification`: Enable email notifications
- `email_to`: Email recipient
- `smtp_server`: SMTP server address

## Security Notes

⚠️ **Password Storage**:

- Base64 encoding provides **obfuscation**, not encryption
- Not recommended for production use
- Consider using:
  - Windows Credential Manager API
  - Runtime password prompts
  - Environment variables

## Technology Stack

- **Language**: Rust 2021 Edition
- **Database**: redb (embedded KV store)
- **Serialization**: rkyv (zero-copy)
- **Hashing**: BLAKE3 (SIMD-accelerated)
- **Scanning**: jwalk (parallel traversal)
- **Configuration**: TOML + serde
- **CLI**: clap

## License

MIT

## Contributing

Development follows the specifications in DEVELOPMENT_SPEC.md.
