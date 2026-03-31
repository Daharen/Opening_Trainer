param(
    [switch]$SkipPayloadBuild
)

$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$payloadBuildScript = Join-Path $repoRoot 'installer\scripts\build_consumer_payload.ps1'
$manifestPath = Join-Path $repoRoot 'installer\consumer_content_manifest.json'
$issPath = Join-Path $repoRoot 'installer\opening_trainer_installer.iss'
$outputInstaller = Join-Path $repoRoot 'installer\dist\OpeningTrainerSetup.exe'
$payloadExe = Join-Path $repoRoot 'dist\consumer\OpeningTrainer.exe'

if (-not $SkipPayloadBuild) {
    & $payloadBuildScript
}

if (-not (Test-Path -LiteralPath $payloadExe)) {
    throw "Consumer payload is missing. Expected: $payloadExe"
}

$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace([string]$manifest.download_url)) {
    throw 'Manifest download_url cannot be empty.'
}
if (-not ([string]$manifest.download_url -match '^https://[^/]*s3')) {
    throw "Manifest download_url must point to an S3 host. Found: $($manifest.download_url)"
}

$iscc = Get-Command ISCC.exe -ErrorAction SilentlyContinue
if (-not $iscc) {
    $defaultIscc = 'C:\Program Files (x86)\Inno Setup 6\ISCC.exe'
    if (Test-Path -LiteralPath $defaultIscc) {
        $iscc = @{ Source = $defaultIscc }
    }
    else {
        throw 'Unable to locate ISCC.exe. Install Inno Setup 6 and ensure ISCC.exe is on PATH.'
    }
}

Push-Location $repoRoot
try {
    & $iscc.Source $issPath
}
finally {
    Pop-Location
}

if (-not (Test-Path -LiteralPath $outputInstaller)) {
    throw "Installer build failed. Missing expected output: $outputInstaller"
}

Write-Host "Installer build complete: $outputInstaller"
