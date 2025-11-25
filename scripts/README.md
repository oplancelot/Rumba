# Rumba Tape Backup Scripts Usage Guide

## Overview

These PowerShell scripts are used to write Rumba backups to LTFS tape and restore data from tape.

## Files

- `backup-streaming.ps1` - Streaming backup script (writes Rumba data to tape)
- `restore-from-tape.ps1` - Restore script (extracts data from tape)
- `README.md` - This file

## Prerequisites

### Software Dependencies

1. **PowerShell 5.1+** (Windows) or **PowerShell Core 7+** (cross-platform)
2. **Rumba** - Compiled and executable
3. **rustltfs** - Compiled and executable
4. **tar** - For extracting backups (included in Windows 10+)

### Hardware Requirements

1. LTO tape drive (LTFS compatible)
2. Tape formatted as LTFS

## Quick Start

### 1. Backup to Tape

```powershell
# Basic usage - all parameters from config file
.\scripts\backup-streaming.ps1 -ConfigFile config.toml
```

### 2. List Backups on Tape

```powershell
.\scripts\restore-from-tape.ps1 `
    -TapeDevice "\\.\TAPE0" `
    -List
```

### 3. Restore from Tape

```powershell
.\scripts\restore-from-tape.ps1 `
    -TapeDevice "\\.\TAPE0" `
    -RestoreDir "C:\restore" `
    -BackupDate "20251125" `
    -SessionId "20251125_080639"
```

## Parameters

### backup-streaming.ps1

| Parameter | Required | Description | Default |
|-----------|----------|-------------|---------|
| `-ConfigFile` | ✅ | Rumba configuration file path | `config.toml` |
| `-LogDir` | ❌ | Log directory | `.\logs` |

All other parameters (tape device, paths, etc.) are read from the config file `[tape]` section.

### restore-from-tape.ps1

| Parameter | Required | Description | Default |
|-----------|----------|-------------|---------|
| `-TapeDevice` | ✅ | Tape device path | - |
| `-RestoreDir` | ✅* | Restore destination directory | - |
| `-BackupDate` | ✅* | Backup date (YYYYMMDD) | - |
| `-SessionId` | ✅* | Session ID (YYYYMMDD_HHMMSS) | - |
| `-RustLtfsPath` | ❌ | rustltfs executable path | `rustltfs` |
| `-List` | ❌ | List available backups | `false` |
| `-DatabaseOnly` | ❌ | Restore database only | `false` |
| `-TarOnly` | ❌ | Restore tar file only | `false` |

*Note: Not required when using `-List`

## Tape Directory Structure

```
Tape Root/
├── incremental_20251125/
│   ├── database/
│   │   └── rumba_20251125_080639.db
│   └── backup_20251125_080639.tar
│
├── incremental_20251126/
│   ├── database/
│   │   └── rumba_20251126_083012.db
│   └── backup_20251126_083012.tar
│
└── incremental_20251127/
    └── ...
```

## Setting Up Scheduled Tasks

### Windows Task Scheduler

#### Method 1: Using PowerShell

```powershell
# Create daily task at 2:00 AM
$Action = New-ScheduledTaskAction `
    -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\rumba\scripts\backup-streaming.ps1 -ConfigFile C:\rumba\config.toml"

$Trigger = New-ScheduledTaskTrigger -Daily -At "02:00AM"

$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

$Principal = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" `
    -LogonType ServiceAccount `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName "Rumba Tape Backup" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Principal $Principal `
    -Description "Daily Rumba tape backup task"
```

#### Method 2: Using GUI

1. Open Task Scheduler (`taskschd.msc`)
2. Create Basic Task
3. Set trigger (Daily at 2:00 AM)
4. Action: Start a program
   - Program: `PowerShell.exe`
   - Arguments: `-NoProfile -ExecutionPolicy Bypass -File C:\rumba\scripts\backup-streaming.ps1 -ConfigFile C:\rumba\config.toml`
5. Set to "Run with highest privileges"

### Linux cron

Create script `/usr/local/bin/rumba-backup.sh`:

```bash
#!/bin/bash
pwsh /opt/rumba/scripts/backup-streaming.ps1 \
    -ConfigFile /etc/rumba/config.toml
```

Add to crontab:

```bash
# Edit crontab
sudo crontab -e

# Add daily execution at 2:00 AM
0 2 * * * /usr/local/bin/rumba-backup.sh >> /var/log/rumba-backup.log 2>&1
```

## Logging and Monitoring

### Log Files

Log files are saved in the directory specified by `-LogDir` (default: `.\logs`):

```
logs/
├── backup_20251125_080639.log
├── backup_20251126_083012.log
└── ...
```

### Viewing Logs

```powershell
# View latest log
Get-Content .\logs\backup_*.log -Tail 50

# View specific log
Get-Content .\logs\backup_20251125_080639.log
```

## Troubleshooting

### Issue 1: "rustltfs command not found"

**Solution**:
- Specify full path in config file `[tape]` section
- Or add rustltfs to PATH environment variable

### Issue 2: "Cannot access tape device"

**Solution**:
- Verify tape drive is connected
- Confirm device path is correct (Windows: `\\.\TAPE0`, Linux: `/dev/sg0`)
- Run script with administrator privileges

### Issue 3: "Rumba backup failed"

**Solution**:
- Check Rumba configuration file
- Verify Rumba executable path
- Check log file for detailed error

### Issue 4: "Insufficient temp space"

**Solution**:
- Streaming mode uses zero temp files
- If using old script, switch to `backup-streaming.ps1`

## Best Practices

### 1. Regular Restore Testing

```powershell
# Test restore monthly
.\scripts\restore-from-tape.ps1 `
    -TapeDevice "\\.\TAPE0" `
    -RestoreDir "C:\test-restore" `
    -BackupDate "20251125" `
    -SessionId "20251125_080639"
```

### 2. Keep Multiple Generations

- Recommend keeping at least 7 days of backups
- Weekly full backup
- Monthly archive

### 3. Monitor Tape Health

```powershell
# Check tape space
rustltfs space --tape \\.\TAPE0 --detailed
```

### 4. Log Rotation

```powershell
# Clean logs older than 30 days
Get-ChildItem .\logs -Filter "backup_*.log" | 
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } | 
    Remove-Item
```

### 5. Verify Backup Integrity

```powershell
# Verify after backup
.\scripts\backup-streaming.ps1 -ConfigFile config.toml
.\scripts\restore-from-tape.ps1 -TapeDevice \\.\TAPE0 -List
```

## Performance Optimization

### 1. Use SSD for Temp Directory

Not needed with streaming mode - zero temp files!

### 2. Adjust rustltfs Parameters

Modify in config file `[tape]` section or script:
- `--speed-limit` - Limit write speed
- `--parallel` - Enable parallel processing
- `--index-interval` - Adjust index update interval

## Security Recommendations

1. **Encrypt Sensitive Data**: Consider encrypting database before writing to tape
2. **Access Control**: Restrict script and tape access permissions
3. **Offsite Storage**: Regularly store tapes offsite
4. **Audit Logs**: Keep logs of all backup and restore operations

## Support

For issues, check:
- Log files: `.\logs\backup_*.log`
- Rumba documentation: `README.md`
- rustltfs documentation: (rustltfs project)

## Changelog

- **v3.0** (2025-11-25)
  - ✨ Smart backup check: Skips tape write if no files to backup
  - 📋 Config consolidation: `rustltfs_path` and `db_path` moved to `[target]` section
  - 🔧 Simplified configuration structure
  - Config-driven parameters
  - Zero temp files streaming
  - Full error handling

