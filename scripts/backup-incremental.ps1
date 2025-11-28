# Rumba Streaming Tape Backup Script v1.0
# Reads all parameters from config file - simple usage

param(
    [string]$ConfigFile = "config.toml",
    
    [string]$LogDir = ".\logs"
)

$ErrorActionPreference = "Stop"

# Ensure we are running from the project root (parent of scripts dir)
Set-Location "$PSScriptRoot\.."

# Generate timestamps
$Date = Get-Date -Format "yyyyMMdd"
$SessionId = Get-Date -Format "yyyyMMdd_HHmmss"

# Create log directory
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}
$LogFile = Join-Path $LogDir "backup_incremental_$SessionId.log"
Start-Transcript -Path $LogFile -Append

Write-Host ""
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host "       Rumba Streaming Tape Backup v1.0                       " -ForegroundColor Cyan
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host ""

# Simple TOML parser
function Get-TomlValue {
    param([string]$File, [string]$Section, [string]$Key)
    
    $content = Get-Content $File
    $inSection = $false
    
    foreach ($line in $content) {
        $line = $line.Trim()
        
        if ($line -match "^\[$Section\]") {
            $inSection = $true
            continue
        }
        
        if ($line -match '^\[') {
            $inSection = $false
        }
        
        # Simplified regex to avoid parser issues
        # Matches: key = "value" or key = value
        if ($inSection -and $line -match "^$Key\s*=\s*") {
            # Split by = and take the second part
            $parts = $line -split '=', 2
            if ($parts.Count -eq 2) {
                # Trim whitespace and quotes
                return $parts[1].Trim().Trim('"').Trim("'")
            }
        }
    }
    
    return $null
}

try {
    # Read configuration
    Write-Host "Reading config: $ConfigFile" -ForegroundColor Cyan
    
    $TapeDevice = Get-TomlValue -File $ConfigFile -Section "tape" -Key "device"
    $RumbaPath = Get-TomlValue -File $ConfigFile -Section "tape" -Key "rumba_path"
    $RustLtfsPath = Get-TomlValue -File $ConfigFile -Section "tape" -Key "rustltfs_path"
    $DatabasePath = Get-TomlValue -File $ConfigFile -Section "target" -Key "db_path"
    $SkipDb = Get-TomlValue -File $ConfigFile -Section "tape" -Key "skip_database"
    
    # Default values
    # Default values based on script location
    $ScriptRoot = $PSScriptRoot
    
    if (-not $RumbaPath) { 
        if (Test-Path "$ScriptRoot\..\rumba.exe") {
            $RumbaPath = "$ScriptRoot\..\rumba.exe"
        }
        else {
            $RumbaPath = "rumba.exe" 
        }
    }
    
    if (-not $RustLtfsPath) { 
        if (Test-Path "$ScriptRoot\..\rustltfs.exe") {
            $RustLtfsPath = "$ScriptRoot\..\rustltfs.exe"
        }
        else {
            $RustLtfsPath = "rustltfs.exe" 
        }
    }
    
    if (-not $DatabasePath) { $DatabasePath = "rumba.db" }
    
    Write-Host "Config loaded" -ForegroundColor Green
    Write-Host "   Tape device: $TapeDevice" -ForegroundColor White
    Write-Host "   Database: $DatabasePath" -ForegroundColor White
    Write-Host ""
    
    # Resolve paths to absolute to ensure cmd finds them
    $AbsRumbaPath = (Resolve-Path $RumbaPath).Path
    $AbsRustLtfsPath = (Resolve-Path $RustLtfsPath).Path
    $AbsConfigFile = (Resolve-Path $ConfigFile).Path
    
    
    # Step 0: Check if there are files to backup
    Write-Host "--------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host "Checking for files to backup" -ForegroundColor Cyan
    Write-Host "--------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host ""
    
    # Temporarily allow stderr output (rumba writes diagnostics to stderr)
    $prevErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    
    # Capture stdout only (file count), let stderr go to console
    $FileCount = & $AbsRumbaPath backup --config $AbsConfigFile --check
    
    # Restore error preference
    $ErrorActionPreference = $prevErrorActionPreference
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to check backup status (exit code: $LASTEXITCODE)"
    }
    
    Write-Host "   Files to backup: $FileCount" -ForegroundColor White
    Write-Host ""
    
    if ([int]$FileCount -eq 0) {
        Write-Host ""
        Write-Host "===============================================================" -ForegroundColor Green
        Write-Host "       No files to backup - everything is up to date!         " -ForegroundColor Green
        Write-Host "===============================================================" -ForegroundColor Green
        Write-Host ""
        return
    }
    
    # Step 1: Stream tar to tape
    Write-Host "--------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host "Streaming tar to tape" -ForegroundColor Cyan
    Write-Host "--------------------------------------------------------------" -ForegroundColor Cyan
    
    $TarDest = "/incremental_$Date/backup_$SessionId.tar"
    Write-Host "   Destination: $TarDest" -ForegroundColor White
    Write-Host "   Starting stream..." -ForegroundColor Yellow
    Write-Host ""
    
    # Use cmd /c for binary safe piping to avoid PowerShell encoding issues
    # Note: rustltfs write uses --tape and --output, not --device and --destination
    $Command = "cmd /c `"$AbsRumbaPath backup --config $AbsConfigFile --format tar --output - | $AbsRustLtfsPath write --tape $TapeDevice --output $TarDest --verify --progress`""
    Invoke-Expression $Command
    
    if ($LASTEXITCODE -ne 0) {
        throw "Backup failed"
    }
    
    Write-Host ""
    Write-Host "   Tar backup complete" -ForegroundColor Green
    
    # Step 2: Backup database
    if ($SkipDb -ne "true" -and (Test-Path $DatabasePath)) {
        Write-Host ""
        Write-Host "--------------------------------------------------------------" -ForegroundColor Cyan
        Write-Host "Writing database" -ForegroundColor Cyan
        Write-Host "--------------------------------------------------------------" -ForegroundColor Cyan
        
        $DbDest = "/incremental_$Date/database/rumba_$SessionId.db"
        Write-Host "   Destination: $DbDest" -ForegroundColor White
        Write-Host ""
        
        & $RustLtfsPath write --tape $TapeDevice --output $DbDest --verify --progress $DatabasePath
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host ""
            Write-Host "   Database backup complete" -ForegroundColor Green
        }
    }
    
    # Complete
    Write-Host ""
    Write-Host "==============================================================" -ForegroundColor Green
    Write-Host "                  Backup successful!                          " -ForegroundColor Green
    Write-Host "==============================================================" -ForegroundColor Green
    Write-Host ""
    
}
catch {
    Write-Host ""
    Write-Host "==============================================================" -ForegroundColor Red
    Write-Host "                  Backup failed!                              " -ForegroundColor Red
    Write-Host "==============================================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "   Error: $_" -ForegroundColor Red
    exit 1
}
finally {
    Stop-Transcript
    Write-Host "Log: $LogFile" -ForegroundColor Cyan
    Write-Host ""
}
