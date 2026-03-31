param(
    [switch]$SkipDependencyInstall
)

$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$distRoot = Join-Path $repoRoot 'dist'
$consumerDist = Join-Path $distRoot 'consumer'
$buildRoot = Join-Path $repoRoot 'build\pyinstaller'
$specPath = Join-Path $repoRoot 'installer\packaging\opening_trainer_consumer.spec'
$entrypoint = Join-Path $repoRoot 'main.py'

Write-Host "Repository root: $repoRoot"
Write-Host "Consumer payload output: $consumerDist"

if (-not (Test-Path -LiteralPath $entrypoint)) {
    throw "Entrypoint not found: $entrypoint"
}

if (-not $SkipDependencyInstall) {
    Write-Host 'Ensuring PyInstaller is installed...'
    python -m pip install --upgrade pyinstaller
}

Write-Host 'Verifying PyInstaller availability...'
python -m PyInstaller --version | Out-Null

if (Test-Path -LiteralPath $consumerDist) {
    Remove-Item -LiteralPath $consumerDist -Recurse -Force
}
if (Test-Path -LiteralPath $buildRoot) {
    Remove-Item -LiteralPath $buildRoot -Recurse -Force
}

New-Item -ItemType Directory -Path $consumerDist -Force | Out-Null
New-Item -ItemType Directory -Path $buildRoot -Force | Out-Null

Write-Host 'Building consumer payload with PyInstaller...'
python -m PyInstaller --noconfirm --clean --distpath $consumerDist --workpath $buildRoot $specPath

$outputExe = Join-Path $consumerDist 'OpeningTrainer.exe'
if (-not (Test-Path -LiteralPath $outputExe)) {
    throw "Consumer payload build failed. Missing expected executable: $outputExe"
}

Write-Host "Consumer payload build complete: $outputExe"
