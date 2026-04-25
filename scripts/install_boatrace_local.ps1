param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$BootstrapArgs
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$VenvDir = Join-Path $RootDir ".venv"
$LogDir = Join-Path $RootDir "logs\bootstrap-install"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Show-InstallerHeader {
    Write-Host "BoatRace Local Predictor installer"
    Write-Host ""
    Write-Host "これから次の順でセットアップします。"
    Write-Host "  1. Python 仮想環境を作成"
    Write-Host "  2. pip と依存関係を導入"
    Write-Host "  3. 過去データを取得"
    Write-Host "  4. 特徴量を作成してローカルモデルを学習"
    Write-Host "  5. 対象日の予測を生成"
    Write-Host "  6. Claude Code / Codex skill を導入"
    Write-Host ""
    Write-Host "注意:"
    Write-Host "  - 初回はデータ取得、特徴量作成、LightGBM 学習に時間がかかります。"
    Write-Host "  - 90日学習では端末やネットワークにより数分から十数分程度かかることがあります。"
    Write-Host "  - 詳細ログは logs\bootstrap-install\ に保存します。"
    Write-Host ""
}

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
        [string]$Step,
        [string]$Label,
        [string]$Estimate,
        [string]$LogFile,
        [string]$FilePath,
        [string[]]$Arguments
    )

    Write-Host "[$Step] $Label"
    Write-Host "     目安: $Estimate"
    Write-Host "     ログ: $LogFile"

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
    $spinner = @("|", "/", "-", "\")
    $spinnerIndex = 0
    while (-not $process.HasExited) {
        $elapsed = [int]((Get-Date) - $start).TotalSeconds
        $elapsedText = "{0:D2}:{1:D2}" -f [int]($elapsed / 60), ($elapsed % 60)
        $spin = $spinner[$spinnerIndex % $spinner.Length]
        $spinnerIndex += 1
        Write-Progress -Activity "BoatRace bootstrap" -Status "[$spin] $Label 経過 $elapsedText" -PercentComplete 0
        Write-Host "`r     [$spin] 実行中 $elapsedText 経過" -NoNewline
        Start-Sleep -Milliseconds 200
    }

    if ($process.ExitCode -ne 0) {
        Write-Host "`r     [NG] 失敗しました                         "
        if (Test-Path $stdout) { Get-Content $stdout -Tail 40 }
        if (Test-Path $stderr) { Get-Content $stderr -Tail 40 }
        throw "$Label に失敗しました。ログ: $LogFile"
    }

    $elapsed = [int]((Get-Date) - $start).TotalSeconds
    $elapsedText = "{0:D2}:{1:D2}" -f [int]($elapsed / 60), ($elapsed % 60)
    Write-Progress -Activity "BoatRace bootstrap" -Completed
    Write-Host "`r     [OK] 完了 $elapsedText 経過                         "
    Write-Host ""
}

$pythonCmd = Get-PythonCommand
$pythonExe = $pythonCmd[0]
$pythonArgs = @()
if ($pythonCmd.Length -gt 1) {
    $pythonArgs = $pythonCmd[1..($pythonCmd.Length - 1)]
}

Push-Location $RootDir
try {
    Show-InstallerHeader

    $versionCheckArgs = $pythonArgs + @("-c", "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)")
    & $pythonExe @versionCheckArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python 3.11 以上が必要です。"
    }

    if (-not (Test-Path $VenvDir)) {
        Invoke-Step `
            -Step "1/6" `
            -Label "仮想環境を作成中" `
            -Estimate "通常 10秒から1分程度" `
            -LogFile (Join-Path $LogDir "01_create_venv.log") `
            -FilePath $pythonExe `
            -Arguments ($pythonArgs + @("-m", "venv", $VenvDir))
    } else {
        Write-Host "[1/6] Python 仮想環境を作成"
        Write-Host "     [SKIP] 既にあります: $VenvDir"
        Write-Host ""
    }

    $venvPython = Join-Path $VenvDir "Scripts\python.exe"

    Invoke-Step `
        -Step "2/6" `
        -Label "pip を更新中" `
        -Estimate "通常 10秒から1分程度" `
        -LogFile (Join-Path $LogDir "02_upgrade_pip.log") `
        -FilePath $venvPython `
        -Arguments @("-m", "pip", "install", "--upgrade", "pip")

    Invoke-Step `
        -Step "3/6" `
        -Label "依存関係をインストール中" `
        -Estimate "初回は数分かかることがあります。DuckDB、LightGBM、rich などを導入します" `
        -LogFile (Join-Path $LogDir "03_install_requirements.log") `
        -FilePath $venvPython `
        -Arguments @("-m", "pip", "install", "-e", ".")

    & $venvPython -c "import lightgbm"
    if ($LASTEXITCODE -ne 0) {
        throw "LightGBM を読み込めません。依存関係の導入ログを確認してください。"
    }

    Write-Host "[4/6-6/6] データ取得、特徴量作成、学習、予測、skill 導入へ進みます。"
    Write-Host "     ここからは画面に全体進捗、ステージ別進捗、現在処理中の内容を表示します。"
    Write-Host "     特に「特徴量作成と学習」は履歴集計と LightGBM 学習を行うため時間がかかります。"
    Write-Host ""

    & $venvPython (Join-Path $RootDir "scripts\boatrace_bootstrap.py") @BootstrapArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
