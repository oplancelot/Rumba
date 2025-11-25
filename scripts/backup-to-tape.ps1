# Rumba 磁带备份脚本
# 功能: 调用 Rumba 生成备份,然后通过 rustltfs 写入磁带
# 作者: Rumba Team
# 版本: 1.0

param(
    [Parameter(Mandatory=$true, HelpMessage="Rumba配置文件路径")]
    [string]$ConfigFile,
    
    [Parameter(Mandatory=$true, HelpMessage="磁带设备路径 (如: /dev/sg0 或 \\.\TAPE0)")]
    [string]$TapeDevice,
    
    [Parameter(HelpMessage="Rumba可执行文件路径")]
    [string]$RumbaPath = "rumba",
    
    [Parameter(HelpMessage="rustltfs可执行文件路径")]
    [string]$RustLtfsPath = "rustltfs",
    
    [Parameter(HelpMessage="临时目录")]
    [string]$TempDir = "$env:TEMP\rumba_backup",
    
    [Parameter(HelpMessage="日志目录")]
    [string]$LogDir = ".\logs",
    
    [Parameter(HelpMessage="保留临时文件 (用于调试)")]
    [switch]$KeepTemp,
    
    [Parameter(HelpMessage="跳过数据库备份")]
    [switch]$SkipDatabase,
    
    [Parameter(HelpMessage="启用邮件通知")]
    [switch]$EmailNotification,
    
    [Parameter(HelpMessage="邮件接收地址")]
    [string]$EmailTo = "",
    
    [Parameter(HelpMessage="SMTP服务器")]
    [string]$SmtpServer = ""
)

# ============================================================================
# 初始化
# ============================================================================

$ErrorActionPreference = "Stop"
$ProgressPreference = "Continue"

# 生成时间戳
$Date = Get-Date -Format "yyyyMMdd"
$SessionId = Get-Date -Format "yyyyMMdd_HHmmss"
$IncrementalDir = "/incremental_$Date"

# 日志文件
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Path $LogDir | Out-Null
}
$LogFile = Join-Path $LogDir "backup_$SessionId.log"

# 开始记录日志
Start-Transcript -Path $LogFile -Append

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║          Rumba 磁带备份系统 v1.0                          ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "📅 日期:       $Date" -ForegroundColor White
Write-Host "🆔 会话ID:     $SessionId" -ForegroundColor White
Write-Host "📼 磁带设备:   $TapeDevice" -ForegroundColor White
Write-Host "📁 增量目录:   $IncrementalDir" -ForegroundColor White
Write-Host "📝 日志文件:   $LogFile" -ForegroundColor White
Write-Host ""

# ============================================================================
# 函数定义
# ============================================================================

function Write-Step {
    param([string]$Message, [string]$Color = "Cyan")
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor $Color
    Write-Host $Message -ForegroundColor $Color
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor $Color
}

function Write-Success {
    param([string]$Message)
    Write-Host "   ✅ $Message" -ForegroundColor Green
}

function Write-Info {
    param([string]$Message)
    Write-Host "   ℹ️  $Message" -ForegroundColor White
}

function Write-Warning {
    param([string]$Message)
    Write-Host "   ⚠️  $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "   ❌ $Message" -ForegroundColor Red
}

function Format-FileSize {
    param([long]$Bytes)
    if ($Bytes -ge 1TB) { return "{0:N2} TB" -f ($Bytes / 1TB) }
    if ($Bytes -ge 1GB) { return "{0:N2} GB" -f ($Bytes / 1GB) }
    if ($Bytes -ge 1MB) { return "{0:N2} MB" -f ($Bytes / 1MB) }
    if ($Bytes -ge 1KB) { return "{0:N2} KB" -f ($Bytes / 1KB) }
    return "$Bytes B"
}

function Send-Notification {
    param(
        [string]$Subject,
        [string]$Body,
        [bool]$IsSuccess = $true
    )
    
    if (-not $EmailNotification -or [string]::IsNullOrEmpty($EmailTo)) {
        return
    }
    
    try {
        $Priority = if ($IsSuccess) { "Normal" } else { "High" }
        Send-MailMessage `
            -From "rumba-backup@company.com" `
            -To $EmailTo `
            -Subject $Subject `
            -Body $Body `
            -SmtpServer $SmtpServer `
            -Priority $Priority
        Write-Info "邮件通知已发送到: $EmailTo"
    } catch {
        Write-Warning "发送邮件失败: $_"
    }
}

# ============================================================================
# 主流程
# ============================================================================

$StartTime = Get-Date
$Success = $false

try {
    # 创建临时目录
    if (-not (Test-Path $TempDir)) {
        New-Item -ItemType Directory -Path $TempDir | Out-Null
        Write-Info "临时目录已创建: $TempDir"
    }
    
    # ========================================
    # 步骤 1: 生成 tar 备份文件
    # ========================================
    Write-Step "📦 步骤 1/4: 生成 tar 备份文件"
    
    $TarFile = Join-Path $TempDir "backup_$SessionId.tar"
    
    Write-Info "调用 Rumba 生成备份..."
    Write-Info "配置文件: $ConfigFile"
    Write-Info "输出文件: $TarFile"
    
    # 调用 Rumba
    $RumbaArgs = @(
        "backup",
        "--config", $ConfigFile,
        "--output", $TarFile,
        "--format", "tar"
    )
    
    & $RumbaPath @RumbaArgs
    
    if ($LASTEXITCODE -ne 0) {
        throw "Rumba 备份失败,退出码: $LASTEXITCODE"
    }
    
    if (-not (Test-Path $TarFile)) {
        throw "tar 文件未生成: $TarFile"
    }
    
    $TarSize = (Get-Item $TarFile).Length
    Write-Success "tar 文件已生成: $(Format-FileSize $TarSize)"
    
    # ========================================
    # 步骤 2: 准备数据库快照
    # ========================================
    $DbFile = $null
    $DbSize = 0
    
    if (-not $SkipDatabase) {
        Write-Step "🗄️  步骤 2/4: 准备数据库快照"
        
        # 尝试从配置文件读取数据库路径
        # TODO: 实现配置文件解析
        $RumbaDbPath = "rumba.db"
        
        if (Test-Path $RumbaDbPath) {
            $DbFile = Join-Path $TempDir "rumba_$SessionId.db"
            Copy-Item $RumbaDbPath $DbFile
            $DbSize = (Get-Item $DbFile).Length
            Write-Success "数据库已复制: $(Format-FileSize $DbSize)"
        } else {
            Write-Warning "数据库文件不存在: $RumbaDbPath"
            Write-Info "跳过数据库备份"
        }
    } else {
        Write-Step "🗄️  步骤 2/4: 跳过数据库备份 (--SkipDatabase)"
    }
    
    # ========================================
    # 步骤 3: 写入 tar 到磁带
    # ========================================
    Write-Step "📼 步骤 3/4: 写入 tar 文件到磁带"
    
    $TarDestination = "$IncrementalDir/backup_$SessionId.tar"
    
    Write-Info "源文件: $TarFile"
    Write-Info "目标路径: $TarDestination"
    Write-Info "正在写入..."
    
    $RustLtfsArgs = @(
        "write",
        "--device", $TapeDevice,
        "--source", $TarFile,
        "--destination", $TarDestination,
        "--verify",
        "--progress"
    )
    
    & $RustLtfsPath @RustLtfsArgs
    
    if ($LASTEXITCODE -ne 0) {
        throw "rustltfs 写入 tar 失败,退出码: $LASTEXITCODE"
    }
    
    Write-Success "tar 文件已写入磁带"
    
    # ========================================
    # 步骤 4: 写入数据库到磁带
    # ========================================
    if ($DbFile -and (Test-Path $DbFile)) {
        Write-Step "💾 步骤 4/4: 写入数据库到磁带"
        
        $DbDestination = "$IncrementalDir/database/rumba_$SessionId.db"
        
        Write-Info "源文件: $DbFile"
        Write-Info "目标路径: $DbDestination"
        Write-Info "正在写入..."
        
        $RustLtfsArgs = @(
            "write",
            "--device", $TapeDevice,
            "--source", $DbFile,
            "--destination", $DbDestination,
            "--verify",
            "--progress"
        )
        
        & $RustLtfsPath @RustLtfsArgs
        
        if ($LASTEXITCODE -ne 0) {
            throw "rustltfs 写入数据库失败,退出码: $LASTEXITCODE"
        }
        
        Write-Success "数据库已写入磁带"
    } else {
        Write-Step "💾 步骤 4/4: 跳过数据库写入"
    }
    
    # ========================================
    # 完成
    # ========================================
    $Success = $true
    $EndTime = Get-Date
    $Duration = $EndTime - $StartTime
    
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║                  ✅ 备份成功完成!                          ║" -ForegroundColor Green
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 备份统计:" -ForegroundColor White
    Write-Host "   磁带目录:     $IncrementalDir" -ForegroundColor White
    Write-Host "   tar 文件:     $TarDestination ($(Format-FileSize $TarSize))" -ForegroundColor White
    if ($DbFile) {
        Write-Host "   数据库:       $DbDestination ($(Format-FileSize $DbSize))" -ForegroundColor White
    }
    Write-Host "   总大小:       $(Format-FileSize ($TarSize + $DbSize))" -ForegroundColor White
    Write-Host "   耗时:         $($Duration.ToString('hh\:mm\:ss'))" -ForegroundColor White
    Write-Host ""
    
    # 发送成功通知
    $NotificationBody = @"
Rumba 磁带备份已成功完成

会话信息:
- 日期: $Date
- 会话ID: $SessionId
- 磁带目录: $IncrementalDir

备份内容:
- tar 文件: $(Format-FileSize $TarSize)
- 数据库: $(Format-FileSize $DbSize)
- 总大小: $(Format-FileSize ($TarSize + $DbSize))

耗时: $($Duration.ToString('hh\:mm\:ss'))

日志文件: $LogFile
"@
    
    Send-Notification -Subject "✅ Rumba 备份成功 - $Date" -Body $NotificationBody -IsSuccess $true
    
} catch {
    $Success = $false
    $EndTime = Get-Date
    $Duration = $EndTime - $StartTime
    
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Red
    Write-Host "║                  ❌ 备份失败!                              ║" -ForegroundColor Red
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Red
    Write-Host ""
    Write-Error "错误信息: $_"
    Write-Host ""
    Write-Host "详细信息请查看日志文件: $LogFile" -ForegroundColor Yellow
    
    # 发送失败通知
    $NotificationBody = @"
Rumba 磁带备份失败!

会话信息:
- 日期: $Date
- 会话ID: $SessionId
- 磁带设备: $TapeDevice

错误信息:
$_

耗时: $($Duration.ToString('hh\:mm\:ss'))

日志文件: $LogFile

请检查日志文件以获取详细信息。
"@
    
    Send-Notification -Subject "❌ Rumba 备份失败 - $Date" -Body $NotificationBody -IsSuccess $false
    
} finally {
    # 清理临时文件
    if (-not $KeepTemp) {
        Write-Host ""
        Write-Host "🧹 清理临时文件..." -ForegroundColor Yellow
        if (Test-Path $TempDir) {
            try {
                Remove-Item -Path $TempDir -Recurse -Force
                Write-Success "临时文件已清理"
            } catch {
                Write-Warning "清理临时文件失败: $_"
            }
        }
    } else {
        Write-Host ""
        Write-Warning "保留临时文件 (--KeepTemp): $TempDir"
    }
    
    # 停止日志记录
    Stop-Transcript
    
    Write-Host ""
    Write-Host "📝 完整日志已保存到: $LogFile" -ForegroundColor Cyan
    Write-Host ""
}

# 返回退出码
if ($Success) {
    exit 0
} else {
    exit 1
}
