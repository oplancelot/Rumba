# Rumba Tape Restore Script
# Function: Restore backup data from tape
# Author: Rumba Team
# Version: 1.0

param(
    [Parameter(Mandatory = $true, HelpMessage = "Tape device path")]
    [string]$TapeDevice,
    
    [Parameter(Mandatory = $true, HelpMessage = "Restore destination directory")]
    [string]$RestoreDir,
    
    [Parameter(HelpMessage = "Backup date to restore (YYYYMMDD format)")]
    [string]$BackupDate,
    
    [Parameter(HelpMessage = "Session ID to restore (YYYYMMDD_HHMMSS format)")]
    [string]$SessionId,
    
    [Parameter(HelpMessage = "Path to rustltfs executable")]
    [string]$RustLtfsPath = "rustltfs",
    
    [Parameter(HelpMessage = "Restore database only")]
    [switch]$DatabaseOnly,
    
    [Parameter(HelpMessage = "Restore tar file only")]
    [switch]$TarOnly,
    
    [Parameter(HelpMessage = "List available backups")]
    [switch]$List
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║          Rumba Tape Restore System v1.0                   ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ============================================================================
# List available backups
# ============================================================================

if ($List) {
    Write-Host "📋 Listing backups on tape..." -ForegroundColor Cyan
    Write-Host ""
    
    & $RustLtfsPath read --device $TapeDevice
    
    Write-Host ""
    Write-Host "Tip: Use -BackupDate and -SessionId parameters to specify backup to restore" -ForegroundColor Yellow
    exit 0
}

# ============================================================================
# Validate parameters
# ============================================================================

if ([string]::IsNullOrEmpty($BackupDate)) {
    Write-Host "❌ Error: -BackupDate parameter is required" -ForegroundColor Red
    Write-Host "   Use -List parameter to view available backups" -ForegroundColor Yellow
    exit 1
}

if ([string]::IsNullOrEmpty($SessionId)) {
    Write-Host "❌ Error: -SessionId parameter is required" -ForegroundColor Red
    Write-Host "   Use -List parameter to view available backups" -ForegroundColor Yellow
    exit 1
}

# ============================================================================
# Restore Process
# ============================================================================

$IncrementalDir = "/incremental_$BackupDate"
$TarSource = "$IncrementalDir/backup_$SessionId.tar"
$DbSource = "$IncrementalDir/database/rumba_$SessionId.db"

Write-Host "📅 Backup Date:   $BackupDate" -ForegroundColor White
Write-Host "🆔 Session ID:    $SessionId" -ForegroundColor White
Write-Host "📼 Tape Device:   $TapeDevice" -ForegroundColor White
Write-Host "📁 Restore Dir:   $RestoreDir" -ForegroundColor White
Write-Host ""

# Create restore directory
if (-not (Test-Path $RestoreDir)) {
    New-Item -ItemType Directory -Path $RestoreDir | Out-Null
    Write-Host "✅ Restore directory created: $RestoreDir" -ForegroundColor Green
}

try {
    # ========================================
    # Restore tar file
    # ========================================
    if (-not $DatabaseOnly) {
        Write-Host ""
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "📦 Restoring tar backup file" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        
        $TarDest = Join-Path $RestoreDir "backup_$SessionId.tar"
        
        Write-Host "   Source: $TarSource" -ForegroundColor White
        Write-Host "   Destination: $TarDest" -ForegroundColor White
        Write-Host ""
        
        # Extract tar file from tape
        & $RustLtfsPath extract `
            --device $TapeDevice `
            --source $TarSource `
            --destination $TarDest `
            --progress
        
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to extract tar file, exit code: $LASTEXITCODE"
        }
        
        Write-Host ""
        Write-Host "   ✅ Tar file restored: $TarDest" -ForegroundColor Green
        
        # Extract tar content
        Write-Host ""
        Write-Host "   📂 Extracting tar file..." -ForegroundColor Cyan
        
        $ExtractDir = Join-Path $RestoreDir "extracted"
        if (-not (Test-Path $ExtractDir)) {
            New-Item -ItemType Directory -Path $ExtractDir | Out-Null
        }
        
        # Use tar command to extract (requires tar tool installed)
        tar -xf $TarDest -C $ExtractDir
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Tar file extracted to: $ExtractDir" -ForegroundColor Green
        }
        else {
            Write-Host "   ⚠️  Tar extraction failed, please extract manually: $TarDest" -ForegroundColor Yellow
        }
    }
    
    # ========================================
    # Restore database
    # ========================================
    if (-not $TarOnly) {
        Write-Host ""
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "💾 Restoring database" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        
        $DbDest = Join-Path $RestoreDir "rumba_$SessionId.db"
        
        Write-Host "   Source: $DbSource" -ForegroundColor White
        Write-Host "   Destination: $DbDest" -ForegroundColor White
        Write-Host ""
        
        # Extract database from tape
        & $RustLtfsPath extract `
            --device $TapeDevice `
            --source $DbSource `
            --destination $DbDest `
            --progress
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "   ⚠️  Failed to extract database, it may not exist" -ForegroundColor Yellow
        }
        else {
            Write-Host ""
            Write-Host "   ✅ Database restored: $DbDest" -ForegroundColor Green
        }
    }
    
    # ========================================
    # Complete
    # ========================================
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║                  ✅ Restore Successful!                    ║" -ForegroundColor Green
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 Restore Summary:" -ForegroundColor White
    Write-Host "   Restore Dir: $RestoreDir" -ForegroundColor White
    if (-not $DatabaseOnly) {
        Write-Host "   Tar File: $TarDest" -ForegroundColor White
        Write-Host "   Extracted: $ExtractDir" -ForegroundColor White
    }
    if (-not $TarOnly) {
        Write-Host "   Database: $DbDest" -ForegroundColor White
    }
    Write-Host ""
    
}
catch {
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Red
    Write-Host "║                  ❌ Restore Failed!                        ║" -ForegroundColor Red
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Red
    Write-Host ""
    Write-Host "   ❌ Error: $_" -ForegroundColor Red
    Write-Host ""
    exit 1
}
