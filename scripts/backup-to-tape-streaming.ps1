# Rumba 流式磁带备份脚本 v2.0
# 零临时文件,性能最优方案
# 使用 PowerShell 管道: Rumba stdout → rustltfs stdin

param(
    [Parameter(Mandatory=$true, HelpMessage="Rumba配置文件路径")]
    [string]$ConfigFile,
    
    [Parameter(Mandatory=$true, HelpMessage="磁带设备路径")]
    [string]$TapeDevice,
    
    [Parameter(HelpMessage="Rumba可执行文件路径")]
    [string]$RumbaPath = "rumba",
    
    [Parameter(HelpMessage="rustltfs可执行文件路径")]
    [string]$RustLtfsPath = "rustltfs",
    
    [Parameter(HelpMessage="日志目录")]
    [string]$LogDir = ".\logs",
    
    [Parameter(HelpMessage="跳过数据库备份")]
    [switch]$SkipDatabase,
    
    [Parameter(HelpMessage="数据库文件路径")]
    [string]$DatabasePath = "rumba.db",
    
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
$LogFile = Join-Path $LogDir "backup_streaming_$SessionId.log"

# 开始记录日志
Start-Transcript -Path $LogFile -Append

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║       Rumba 流式磁带备份系统 v2.0                         ║" -ForegroundColor Cyan
Write-Host "║           (零临时文件 · 性能最优)                         ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "📅 日期:       $Date" -ForegroundColor White
Write-Host "🆔 会话ID:     $SessionId" -ForegroundColor White
Write-Host "📼 磁带设备:   $TapeDevice" -ForegroundColor White
Write-Host "📁 增量目录:   $IncrementalDir" -ForegroundColor White
Write-Host "⚡ 传输模式:   流式管道 (Rumba → rustltfs)" -ForegroundColor Yellow
Write-Host "💾 临时文件:   0 (零临时文件)" -ForegroundColor Green
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
        Write-Info "邮件通知已发送"
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
    # ========================================
    # 步骤 1: 流式写入 tar 到磁带
    # ========================================
    Write-Step "📦 步骤 1/2: 流式写入 tar 备份"
    
    $TarDestination = "$IncrementalDir/backup_$SessionId.tar"
    
    Write-Info "配置文件: $ConfigFile"
    Write-Info "目标路径: $TarDestination"
    Write-Info "传输模式: Rumba stdout → PowerShell 管道 → rustltfs stdin"
    Write-Host ""
    Write-Host "   ⚡ 开始流式传输 (零临时文件)..." -ForegroundColor Cyan
    Write-Host ""
    
    # 流式管道: Rumba 生成 tar → rustltfs 写入磁带
    # 关键: --output - 表示输出到 stdout
    #       rustltfs 不指定 --source 时从 stdin 读取
    & $RumbaPath backup `
        --config $ConfigFile `
        --format tar `
        --output - | `
    & $RustLtfsPath write `
        --device $TapeDevice `
        --destination $TarDestination `
        --verify `
        --progress
    
    if ($LASTEXITCODE -ne 0) {
        throw "流式备份失败,退出码: $LASTEXITCODE"
    }
    
    Write-Host ""
    Write-Success "tar 备份已完成 (流式传输,无临时文件)"
    
    # ========================================
    # 步骤 2: 写入数据库
    # ========================================
    if (-not $SkipDatabase) {
        Write-Step "💾 步骤 2/2: 写入数据库"
        
        $DbDestination = "$IncrementalDir/database/rumba_$SessionId.db"
        
        if (Test-Path $DatabasePath) {
            Write-Info "源文件: $DatabasePath"
            Write-Info "目标路径: $DbDestination"
            Write-Host ""
            
            & $RustLtfsPath write `
                --device $TapeDevice `
                --source $DatabasePath `
                --destination $DbDestination `
                --verify `
                --progress
            
            if ($LASTEXITCODE -ne 0) {
                throw "数据库写入失败,退出码: $LASTEXITCODE"
            }
            
            Write-Host ""
            Write-Success "数据库已写入磁带"
        } else {
            Write-Warning "数据库文件不存在: $DatabasePath"
            Write-Info "跳过数据库备份"
        }
    } else {
        Write-Step "💾 步骤 2/2: 跳过数据库备份 (--SkipDatabase)"
    }
    
    # ========================================
    # 完成
    # ========================================
    $Success = $true
    $EndTime = Get-Date
    $Duration = $EndTime - $StartTime
    
    Write-Host ""
    Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║                  ✅ 流式备份成功!                          ║" -ForegroundColor Green
    Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 备份统计:" -ForegroundColor White
    Write-Host "   磁带目录:     $IncrementalDir" -ForegroundColor White
    Write-Host "   tar 文件:     $TarDestination" -ForegroundColor White
    if (-not $SkipDatabase -and (Test-Path $DatabasePath)) {
        Write-Host "   数据库:       $DbDestination" -ForegroundColor White
    }
    Write-Host "   传输模式:     流式管道 (零临时文件)" -ForegroundColor Yellow
    Write-Host "   耗时:         $($Duration.ToString('hh\:mm\:ss'))" -ForegroundColor White
    Write-Host ""
    Write-Host "💡 性能优势:" -ForegroundColor Cyan
    Write-Host "   • 零临时文件,节省磁盘空间" -ForegroundColor White
    Write-Host "   • 减少磁盘 I/O,提升性能" -ForegroundColor White
    Write-Host "   • 实时传输,边生成边写入" -ForegroundColor White
    Write-Host ""
    
    # 发送成功通知
    $NotificationBody = @"
Rumba 流式磁带备份已成功完成

会话信息:
- 日期: $Date
- 会话ID: $SessionId
- 磁带目录: $IncrementalDir

备份内容:
- tar 文件: $TarDestination
- 数据库: $DbDestination

传输模式: 流式管道 (零临时文件)
耗时: $($Duration.ToString('hh\:mm\:ss'))

性能优势:
- 零临时文件,节省磁盘空间
- 减少磁盘 I/O,提升性能
- 实时传输,边生成边写入

日志文件: $LogFile
"@
    
    Send-Notification -Subject "✅ Rumba 流式备份成功 - $Date" -Body $NotificationBody -IsSuccess $true
    
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
Rumba 流式磁带备份失败!

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
    
    Send-Notification -Subject "❌ Rumba 流式备份失败 - $Date" -Body $NotificationBody -IsSuccess $false
    
} finally {
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
