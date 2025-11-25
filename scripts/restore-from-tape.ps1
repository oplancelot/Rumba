# Rumba 磁带恢复脚本
# 功能: 从磁带恢复备份数据
# 作者: Rumba Team
# 版本: 1.0

param(
    [Parameter(Mandatory=$true, HelpMessage="磁带设备路径")]
    [string]$TapeDevice,
    
    [Parameter(Mandatory=$true, HelpMessage="恢复目标目录")]
    [string]$RestoreDir,
    
    [Parameter(HelpMessage="要恢复的备份日期 (YYYYMMDD格式)")]
    [string]$BackupDate,
    
    [Parameter(HelpMessage="要恢复的会话ID (YYYYMMDD_HHMMSS格式)")]
    [string]$SessionId,
    
    [Parameter(HelpMessage="rustltfs可执行文件路径")]
    [string]$RustLtfsPath = "rustltfs",
    
    [Parameter(HelpMessage="只恢复数据库")]
    [switch]$DatabaseOnly,
    
    [Parameter(HelpMessage="只恢复tar文件")]
    [switch]$TarOnly,
    
    [Parameter(HelpMessage="列出可用的备份")]
    [switch]$List
)

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║          Rumba 磁带恢复系统 v1.0                          ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ============================================================================
# 列出可用备份
# ============================================================================

if ($List) {
    Write-Host "📋 列出磁带上的备份..." -ForegroundColor Cyan
    Write-Host ""
    
    & $RustLtfsPath read --device $TapeDevice
    
    Write-Host ""
    Write-Host "提示: 使用 -BackupDate 和 -SessionId 参数指定要恢复的备份" -ForegroundColor Yellow
    exit 0
}

# ============================================================================
# 验证参数
# ============================================================================

if ([string]::IsNullOrEmpty($BackupDate)) {
    Write-Host "❌ 错误: 必须指定 -BackupDate 参数" -ForegroundColor Red
    Write-Host "   使用 -List 参数查看可用的备份" -ForegroundColor Yellow
    exit 1
}

if ([string]::IsNullOrEmpty($SessionId)) {
    Write-Host "❌ 错误: 必须指定 -SessionId 参数" -ForegroundColor Red
    Write-Host "   使用 -List 参数查看可用的备份" -ForegroundColor Yellow
    exit 1
}

# ============================================================================
# 恢复流程
# ============================================================================

$IncrementalDir = "/incremental_$BackupDate"
$TarSource = "$IncrementalDir/backup_$SessionId.tar"
$DbSource = "$IncrementalDir/database/rumba_$SessionId.db"

Write-Host "📅 备份日期:   $BackupDate" -ForegroundColor White
Write-Host "🆔 会话ID:     $SessionId" -ForegroundColor White
Write-Host "📼 磁带设备:   $TapeDevice" -ForegroundColor White
Write-Host "📁 恢复目录:   $RestoreDir" -ForegroundColor White
Write-Host ""

# 创建恢复目录
if (-not (Test-Path $RestoreDir)) {
    New-Item -ItemType Directory -Path $RestoreDir | Out-Null
    Write-Host "✅ 恢复目录已创建: $RestoreDir" -ForegroundColor Green
}

try {
    # ========================================
    # 恢复 tar 文件
    # ========================================
    if (-not $DatabaseOnly) {
        Write-Host ""
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "📦 恢复 tar 备份文件" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        
        $TarDest = Join-Path $RestoreDir "backup_$SessionId.tar"
        
        Write-Host "   源路径: $TarSource" -ForegroundColor White
        Write-Host "   目标路径: $TarDest" -ForegroundColor White
        Write-Host ""
        
        # 从磁带提取tar文件
        & $RustLtfsPath extract `
            --device $TapeDevice `
            --source $TarSource `
            --destination $TarDest `
            --progress
        
        if ($LASTEXITCODE -ne 0) {
            throw "提取 tar 文件失败,退出码: $LASTEXITCODE"
        }
        
        Write-Host ""
        Write-Host "   ✅ tar 文件已恢复: $TarDest" -ForegroundColor Green
        
        # 解压tar文件
        Write-Host ""
        Write-Host "   📂 解压 tar 文件..." -ForegroundColor Cyan
        
        $ExtractDir = Join-Path $RestoreDir "extracted"
        if (-not (Test-Path $ExtractDir)) {
            New-Item -ItemType Directory -Path $ExtractDir | Out-Null
        }
        
        # 使用tar命令解压 (需要安装tar工具)
        tar -xf $TarDest -C $ExtractDir
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ tar 文件已解压到: $ExtractDir" -ForegroundColor Green
        } else {
            Write-Host "   ⚠️  tar 解压失败,请手动解压: $TarDest" -ForegroundColor Yellow
        }
    }
    
    # ========================================
    # 恢复数据库
    # ========================================
    if (-not $TarOnly) {
        Write-Host ""
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "💾 恢复数据库" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
        
        $DbDest = Join-Path $RestoreDir "rumba_$SessionId.db"
        
        Write-Host "   源路径: $DbSource" -ForegroundColor White
        Write-Host "   目标路径: $DbDest" -ForegroundColor White
        Write-Host ""
        
        # 从磁带提取数据库
        & $RustLtfsPath extract `
            --device $TapeDevice `
            --source $DbSource `
            --destination $DbDest `
            --progress
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "   ⚠️  提取数据库失败,可能不存在" -ForegroundColor Yellow
        } else {
            Write-Host ""
            Write-Host "   ✅ 数据库已恢复: $DbDest" -ForegroundColor Green
        }
    }
    
    # ========================================
    # 完成
    # ========================================
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║                  ✅ 恢复成功完成!                          ║" -ForegroundColor Green
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 恢复摘要:" -ForegroundColor White
    Write-Host "   恢复目录: $RestoreDir" -ForegroundColor White
    if (-not $DatabaseOnly) {
        Write-Host "   tar 文件: $TarDest" -ForegroundColor White
        Write-Host "   解压目录: $ExtractDir" -ForegroundColor White
    }
    if (-not $TarOnly) {
        Write-Host "   数据库: $DbDest" -ForegroundColor White
    }
    Write-Host ""
    
} catch {
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Red
    Write-Host "║                  ❌ 恢复失败!                              ║" -ForegroundColor Red
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Red
    Write-Host ""
    Write-Host "   ❌ 错误信息: $_" -ForegroundColor Red
    Write-Host ""
    exit 1
}
