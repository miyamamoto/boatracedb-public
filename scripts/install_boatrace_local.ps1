param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$BootstrapArgs
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$VenvDir = Join-Path $RootDir ".venv"
$LogDir = Join-Path $RootDir "logs\bootstrap-install"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Get-PythonCommand {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return @("py", "-3.11")
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return @("python")
    }
    throw "Python 3.11 以上が見つかりません。"
}

function Invoke-Step {
    param(
        [string]$Label,
        [string]$LogFile,
        [string]$FilePath,
        [string[]]$Arguments
    )

    $stdout = "${LogFile}.out"
    $stderr = "${LogFile}.err"
    $process = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $Arguments `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr

    $start = Get-Date
    while (-not $process.HasExited) {
        $elapsed = [int]((Get-Date) - $start).TotalSeconds
        Write-Progress -Activity "BoatRace bootstrap" -Status "$Label (${elapsed}s)" -PercentComplete 0
        Start-Sleep -Milliseconds 200
    }

    if ($process.ExitCode -ne 0) {
        Write-Host "[NG] $Label"
        if (Test-Path $stdout) { Get-Content $stdout -Tail 40 }
        if (Test-Path $stderr) { Get-Content $stderr -Tail 40 }
        throw "$Label に失敗しました。ログ: $LogFile"
    }

    Write-Progress -Activity "BoatRace bootstrap" -Completed
    Write-Host "[OK] $Label"
}

$pythonCmd = Get-PythonCommand
$pythonExe = $pythonCmd[0]
$pythonArgs = @()
if ($pythonCmd.Length -gt 1) {
    $pythonArgs = $pythonCmd[1..($pythonCmd.Length - 1)]
}

Push-Location $RootDir
try {
    $versionCheckArgs = $pythonArgs + @("-c", "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)")
    & $pythonExe @versionCheckArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python 3.11 以上が必要です。"
    }

    if (-not (Test-Path $VenvDir)) {
        Invoke-Step `
            -Label "仮想環境を作成中" `
            -LogFile (Join-Path $LogDir "01_create_venv.log") `
            -FilePath $pythonExe `
            -Arguments ($pythonArgs + @("-m", "venv", $VenvDir))
    } else {
        Write-Host "[SKIP] 仮想環境は既にあります: $VenvDir"
    }

    $venvPython = Join-Path $VenvDir "Scripts\python.exe"

    Invoke-Step `
        -Label "pip を更新中" `
        -LogFile (Join-Path $LogDir "02_upgrade_pip.log") `
        -FilePath $venvPython `
        -Arguments @("-m", "pip", "install", "--upgrade", "pip")

    Invoke-Step `
        -Label "依存関係をインストール中" `
        -LogFile (Join-Path $LogDir "03_install_requirements.log") `
        -FilePath $venvPython `
        -Arguments @("-m", "pip", "install", "-e", ".")

    & $venvPython -c "import lightgbm"
    if ($LASTEXITCODE -ne 0) {
        throw "LightGBM を読み込めません。依存関係の導入ログを確認してください。"
    }

    & $venvPython (Join-Path $RootDir "scripts\boatrace_bootstrap.py") @BootstrapArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
