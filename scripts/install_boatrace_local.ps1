param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$BootstrapArgs
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
$VenvDir = Join-Path $RootDir ".venv"
$LogDir = Join-Path $RootDir "logs\bootstrap-install"
$UvInstallDir = Join-Path $RootDir ".tools\bin"
$UvBin = $env:UV_BIN

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Show-InstallerHeader {
    Write-Host "BoatRace Local Predictor installer"
    Write-Host ""
    Write-Host "これから次の順でセットアップします。"
    Write-Host "  1. Python/依存管理ツール uv を確認または導入"
    Write-Host "  2. アプリ専用の Python 3.11 環境を作成"
    Write-Host "  3. DuckDB、LightGBM などの依存関係を導入"
    Write-Host "  4. 過去データを取得"
    Write-Host "  5. 特徴量を作成してローカルモデルを学習"
    Write-Host "  6. 対象日の予測を生成"
    Write-Host "  7. Claude Code / Codex skill と Claude MCP を導入"
    Write-Host ""
    Write-Host "注意:"
    Write-Host "  - Python 3.11 や LightGBM はアプリ専用環境に自動導入します。"
    Write-Host "  - 初回はデータ取得、特徴量作成、LightGBM 学習に時間がかかります。"
    Write-Host "  - SQL分析用に投入する過去データ量もこのあと確認します。"
    Write-Host "  - Claude Code / Claude Desktop には読み取り専用の boatrace-local MCP server を登録します。"
    Write-Host "  - このシステムの予測、買い目候補、SQL分析結果、番組表、説明文は参考情報です。"
    Write-Host "  - 開発者および配布者は出力の正確性や利用結果について一切の責任を負いません。"
    Write-Host "  - 回収率はオッズ、購入点数、資金配分、直前情報に左右され、プラス収支は保証されません。"
    Write-Host "  - 予測はあくまでレースを楽しむための材料です。"
    Write-Host "  - 舟券購入やその他の判断は、必ず利用者自身の責任で行ってください。"
    Write-Host "  - 180日分ではデータ取得だけで約1時間かかる前提です。"
    Write-Host "  - 180日分の初回セットアップ全体は、おおよそ1.5から2.5時間を見込んでください。"
    Write-Host "  - 詳細ログは logs\bootstrap-install\ に保存します。"
    Write-Host ""
}

function Test-BootstrapArg {
    param(
        [string[]]$Args,
        [string]$Name
    )
    foreach ($arg in $Args) {
        if ($arg -eq $Name -or $arg.StartsWith("${Name}=")) {
            return $true
        }
    }
    return $false
}

function Resolve-BootstrapArgs {
    param([string[]]$Args)

    $resolved = @($Args)
    if (Test-BootstrapArg -Args $resolved -Name "--analysis-days") {
        return $resolved
    }
    if ($env:BOATRACE_ANALYSIS_DAYS) {
        $envDays = 0
        if (-not [int]::TryParse($env:BOATRACE_ANALYSIS_DAYS, [ref]$envDays) -or $envDays -lt 180) {
            throw "BOATRACE_ANALYSIS_DAYS は 180 以上の整数にしてください。"
        }
        return $resolved + @("--analysis-days", $env:BOATRACE_ANALYSIS_DAYS)
    }
    if ([Console]::IsInputRedirected) {
        return $resolved
    }

    Write-Host "SQL分析用にDuckDBへ投入する過去実績日数を選んでください。"
    Write-Host "目的により必要な履歴量が変わります。180日が最小で、標準です。"
    Write-Host "目安として、180日分ではデータ取得だけで約1時間、初回全体で1.5から2.5時間程度かかります。"
    Write-Host ""
    Write-Host "  1) 180日  標準: 直近傾向、今日/明日の予測説明、軽い選手確認。初回合計 約1.5-2.5時間"
    Write-Host "  2) 365日  年間分析: 選手・モーターの年間傾向、会場別の比較。初回合計 約3-5時間"
    Write-Host "  3) 730日  中長期分析: 季節差、会場相性、選手の変化を広めに確認。初回合計 約6-10時間"
    Write-Host "  4) 1095日 長期分析: 3年程度の傾向、長期比較、研究用途。初回合計 約9-15時間"
    Write-Host "  5) カスタム 180日以上で指定"
    Write-Host ""
    $answer = Read-Host "選択 [1]"
    if ([string]::IsNullOrWhiteSpace($answer)) { $answer = "1" }

    switch ($answer) {
        "1" { $days = "180" }
        "2" { $days = "365" }
        "3" { $days = "730" }
        "4" { $days = "1095" }
        "5" {
            $custom = Read-Host "日数を入力してください [180以上]"
            if ([string]::IsNullOrWhiteSpace($custom)) { $custom = "180" }
            $customDays = 0
            if (-not [int]::TryParse($custom, [ref]$customDays) -or $customDays -lt 180) {
                throw "SQL分析用の履歴日数は 180 以上の整数にしてください。"
            }
            $days = $custom
        }
        default {
            throw "選択肢は 1, 2, 3, 4, 5 のいずれかです。"
        }
    }
    return $resolved + @("--analysis-days", $days)
}

function Get-UvCommand {
    if ($UvBin -and (Test-Path $UvBin)) {
        return $UvBin
    }
    $cmd = Get-Command uv -ErrorAction SilentlyContinue
    if ($cmd) {
        return $cmd.Source
    }
    $localUv = Join-Path $UvInstallDir "uv.exe"
    if (Test-Path $localUv) {
        return $localUv
    }
    return $null
}

function Install-Uv {
    $existing = Get-UvCommand
    if ($existing) {
        Write-Host "[1/7] Python/依存管理ツール uv を確認"
        Write-Host "     [SKIP] 既にあります: $existing"
        Write-Host ""
        return $existing
    }

    New-Item -ItemType Directory -Force -Path $UvInstallDir | Out-Null
    $oldInstallDir = $env:UV_INSTALL_DIR
    $env:UV_INSTALL_DIR = $UvInstallDir
    try {
        Invoke-Step `
            -Step "1/7" `
            -Label "Python/依存管理ツール uv を導入中" `
            -Estimate "通常 10秒から1分程度。uv は Python 3.11 の自動取得にも使います" `
            -LogFile (Join-Path $LogDir "01_install_uv.log") `
            -FilePath "powershell" `
            -Arguments @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", "irm https://astral.sh/uv/install.ps1 | iex")
    } finally {
        $env:UV_INSTALL_DIR = $oldInstallDir
    }

    $installed = Get-UvCommand
    if (-not $installed) {
        throw "uv の導入後に実行ファイルを見つけられませんでした。ログ: $(Join-Path $LogDir "01_install_uv.log")"
    }
    return $installed
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

Push-Location $RootDir
try {
    Show-InstallerHeader
    $EffectiveBootstrapArgs = Resolve-BootstrapArgs -Args $BootstrapArgs
    $uvExe = Install-Uv

    if (-not (Test-Path $VenvDir)) {
        Invoke-Step `
            -Step "2/7" `
            -Label "アプリ専用 Python 3.11 環境を作成中" `
            -Estimate "初回は Python 3.11 runtime の取得を含むため数分かかることがあります" `
            -LogFile (Join-Path $LogDir "02_create_python_env.log") `
            -FilePath $uvExe `
            -Arguments @("venv", "--python", "3.11", $VenvDir)
    } else {
        Write-Host "[2/7] アプリ専用 Python 3.11 環境を作成"
        Write-Host "     [SKIP] 既にあります: $VenvDir"
        Write-Host ""
    }

    $venvPython = Join-Path $VenvDir "Scripts\python.exe"

    Invoke-Step `
        -Step "3/7" `
        -Label "依存関係をインストール中" `
        -Estimate "初回は数分かかることがあります。DuckDB、LightGBM、rich などをアプリ専用環境へ導入します" `
        -LogFile (Join-Path $LogDir "03_install_requirements.log") `
        -FilePath $uvExe `
        -Arguments @("pip", "install", "--python", $venvPython, "-e", ".")

    & $venvPython -c "import lightgbm"
    if ($LASTEXITCODE -ne 0) {
        throw "LightGBM を読み込めません。依存関係の導入ログを確認してください。"
    }

    Write-Host "[4/7-7/7] データ取得、特徴量作成、学習、予測、skill/MCP 導入へ進みます。"
    Write-Host "     ここからは画面に全体進捗、ステージ別進捗、現在処理中の内容を表示します。"
    Write-Host "     特に「特徴量作成と学習」は履歴集計と LightGBM 学習を行うため時間がかかります。"
    Write-Host ""

    & $venvPython (Join-Path $RootDir "scripts\boatrace_bootstrap.py") @EffectiveBootstrapArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
