param(
    [switch]$SkipDependencyInstall,
    [switch]$SkipSmokeTest,
    [switch]$DebugConsole
)

$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$distRoot = Join-Path $repoRoot 'dist'
$consumerDist = Join-Path $distRoot 'consumer'
$buildRoot = Join-Path $repoRoot 'build\pyinstaller'
$specPath = Join-Path $repoRoot 'installer\packaging\opening_trainer_consumer.spec'
$entrypoint = Join-Path $repoRoot 'main.py'
$venvPython = Join-Path $repoRoot '.venv\Scripts\python.exe'

Write-Host "Repository root: $repoRoot"
Write-Host "Consumer payload output: $consumerDist"

if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "Repo-local virtual environment interpreter not found at '$venvPython'. Create it first (for example: py -3.11 -m venv .venv; .\.venv\Scripts\python.exe -m pip install -r requirements.txt)."
}

if (-not (Test-Path -LiteralPath $entrypoint)) {
    throw "Entrypoint not found: $entrypoint"
}

if (-not $SkipDependencyInstall) {
    Write-Host 'Ensuring PyInstaller is installed...'
    & $venvPython -m pip install --upgrade pyinstaller
}

Write-Host 'Verifying PyInstaller availability...'
& $venvPython -m PyInstaller --version | Out-Null
Write-Host 'Verifying python-chess availability in repo virtual environment...'
& $venvPython -c "import chess; print(chess.__version__)" | Out-Null

if (Test-Path -LiteralPath $consumerDist) {
    Remove-Item -LiteralPath $consumerDist -Recurse -Force
}
if (Test-Path -LiteralPath $buildRoot) {
    Remove-Item -LiteralPath $buildRoot -Recurse -Force
}

New-Item -ItemType Directory -Path $consumerDist -Force | Out-Null
New-Item -ItemType Directory -Path $buildRoot -Force | Out-Null

Write-Host 'Building consumer payload with PyInstaller...'
$pyInstallerArgs = @(
    "-m", "PyInstaller",
    "--noconfirm",
    "--clean",
    "--distpath", $consumerDist,
    "--workpath", $buildRoot
)
if ($DebugConsole) {
    Write-Host "Debug console build enabled for local diagnostics."
    $pyInstallerArgs += "--console"
}
$pyInstallerArgs += $specPath
& $venvPython @pyInstallerArgs

$outputExe = Join-Path $consumerDist 'OpeningTrainer.exe'
if (-not (Test-Path -LiteralPath $outputExe)) {
    throw "Consumer payload build failed. Missing expected executable: $outputExe"
}

if (-not $SkipSmokeTest) {
    Write-Host 'Running consumer payload smoke test...'
    & $outputExe --show-runtime --runtime-mode dev
    if ($LASTEXITCODE -ne 0) {
        throw "Consumer payload smoke test failed with exit code $LASTEXITCODE."
    }

    Write-Host 'Running consumer payload runtime mode inference smoke test...'
    $smokeRoot = Join-Path $env:TEMP "OpeningTrainerConsumerModeSmoke_$([guid]::NewGuid().ToString('N'))"
    $smokeLocalAppData = Join-Path $smokeRoot "LocalAppData"
    $smokeState = Join-Path $smokeLocalAppData "OpeningTrainer"
    $smokeContent = Join-Path $smokeLocalAppData "OpeningTrainerContent"
    $originalLocalAppData = $env:LOCALAPPDATA
    $originalAssumeInstalled = $env:OPENING_TRAINER_ASSUME_INSTALLED
    try {
        New-Item -ItemType Directory -Path $smokeState -Force | Out-Null
        New-Item -ItemType Directory -Path (Join-Path $smokeContent "stockfish") -Force | Out-Null
        New-Item -ItemType Directory -Path (Join-Path $smokeContent "Timing Conditioned Corpus Bundles") -Force | Out-Null
        Set-Content -LiteralPath (Join-Path $smokeState "runtime.consumer.json") -Value "{}" -Encoding utf8
        Set-Content -LiteralPath (Join-Path $smokeContent "opening_book.bin") -Value "book" -Encoding utf8
        $env:LOCALAPPDATA = $smokeLocalAppData
        $env:OPENING_TRAINER_ASSUME_INSTALLED = "1"
        & $outputExe --show-runtime
        if ($LASTEXITCODE -ne 0) {
            throw "Consumer payload runtime mode inference smoke test failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        $env:LOCALAPPDATA = $originalLocalAppData
        $env:OPENING_TRAINER_ASSUME_INSTALLED = $originalAssumeInstalled
        if (Test-Path -LiteralPath $smokeRoot) {
            Remove-Item -LiteralPath $smokeRoot -Recurse -Force
        }
    }

    Write-Host 'Running consumer payload GUI bootstrap smoke test...'
    & $outputExe --runtime-mode consumer --probe-gui-bootstrap
    if ($LASTEXITCODE -ne 0) {
        throw "Consumer payload GUI bootstrap smoke test failed with exit code $LASTEXITCODE."
    }

    Write-Host 'Running consumer payload real GUI startup probe...'
    & $outputExe --runtime-mode consumer --probe-real-gui-startup
    if ($LASTEXITCODE -ne 0) {
        throw "Consumer payload real GUI startup probe failed with exit code $LASTEXITCODE."
    }
}

Write-Host "Consumer payload build complete: $outputExe"
