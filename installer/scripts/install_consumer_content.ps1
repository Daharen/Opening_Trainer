param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestPath,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $true)]
    [string]$ContentRoot
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $ManifestPath)) {
    throw "Manifest file not found: $ManifestPath"
}

$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
$downloadUrl = [string]$manifest.download_url
$archiveFileName = [string]$manifest.archive_filename
$sha256 = [string]$manifest.sha256

if ([string]::IsNullOrWhiteSpace($downloadUrl)) {
    throw "Manifest does not define a non-empty download_url."
}

if ([string]::IsNullOrWhiteSpace($archiveFileName)) {
    $archiveFileName = 'opening_trainer_content_seed.zip'
}

New-Item -ItemType Directory -Path $AppStateRoot -Force | Out-Null
New-Item -ItemType Directory -Path $ContentRoot -Force | Out-Null

$downloadTarget = Join-Path -Path $env:TEMP -ChildPath $archiveFileName
if (Test-Path -LiteralPath $downloadTarget) {
    Remove-Item -LiteralPath $downloadTarget -Force
}

Invoke-WebRequest -Uri $downloadUrl -OutFile $downloadTarget -UseBasicParsing

if (-not (Test-Path -LiteralPath $downloadTarget)) {
    throw "Content download failed: archive was not created at $downloadTarget"
}

if (-not [string]::IsNullOrWhiteSpace($sha256)) {
    $actualHash = (Get-FileHash -LiteralPath $downloadTarget -Algorithm SHA256).Hash.ToLowerInvariant()
    $expectedHash = $sha256.ToLowerInvariant()
    if ($actualHash -ne $expectedHash) {
        throw "Content checksum mismatch. Expected $expectedHash but got $actualHash"
    }
}

Expand-Archive -LiteralPath $downloadTarget -DestinationPath $ContentRoot -Force

$requiredPaths = @(
    (Join-Path $ContentRoot 'canonical_predecessor_master.sqlite'),
    (Join-Path $ContentRoot 'opening_book.bin'),
    (Join-Path $ContentRoot 'opening_book_names.zip'),
    (Join-Path $ContentRoot 'stockfish'),
    (Join-Path $ContentRoot 'Timing Conditioned Corpus Bundles')
)

foreach ($requiredPath in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Extracted content is incomplete. Missing required path: $requiredPath"
    }
}

$runtimeConfigPath = Join-Path -Path $AppStateRoot -ChildPath 'runtime.consumer.json'
$enginePath = Join-Path -Path $ContentRoot -ChildPath 'stockfish\\stockfish-windows-x86-64-avx2.exe'

if (-not (Test-Path -LiteralPath $enginePath)) {
    $fallbackEnginePath = Join-Path -Path $ContentRoot -ChildPath 'stockfish\\stockfish-windows-x86-64.exe'
    if (Test-Path -LiteralPath $fallbackEnginePath) {
        $enginePath = $fallbackEnginePath
    }
}

$runtimeConfig = [ordered]@{
    corpus_bundle_dir = (Join-Path $ContentRoot 'Timing Conditioned Corpus Bundles')
    predecessor_master_db_path = (Join-Path $ContentRoot 'canonical_predecessor_master.sqlite')
    opening_book_path = (Join-Path $ContentRoot 'opening_book.bin')
    engine_executable_path = $enginePath
    strict_assets = $true
    opponent_fallback_mode = 'current_bundle_only'
}

$runtimeConfig | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $runtimeConfigPath -Encoding UTF8

if (Test-Path -LiteralPath $downloadTarget) {
    Remove-Item -LiteralPath $downloadTarget -Force
}
