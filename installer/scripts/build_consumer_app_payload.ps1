param(
    [switch]$SkipPayloadBuild
)

$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$payloadBuildScript = Join-Path $repoRoot 'installer\scripts\build_consumer_payload.ps1'
$consumerDist = Join-Path $repoRoot 'dist\consumer'
$appPayloadDist = Join-Path $repoRoot 'dist\consumer_app_payload'
$payloadZip = Join-Path $appPayloadDist 'OpeningTrainer-app.zip'

if (-not $SkipPayloadBuild) {
    & $payloadBuildScript
}

if (-not (Test-Path -LiteralPath $consumerDist)) {
    throw "Consumer payload folder missing: $consumerDist"
}

New-Item -ItemType Directory -Path $appPayloadDist -Force | Out-Null
if (Test-Path -LiteralPath $payloadZip) {
    Remove-Item -LiteralPath $payloadZip -Force
}

Compress-Archive -Path (Join-Path $consumerDist '*') -DestinationPath $payloadZip -CompressionLevel Optimal
$hash = (Get-FileHash -LiteralPath $payloadZip -Algorithm SHA256).Hash.ToLowerInvariant()
Write-Host "App payload package complete: $payloadZip"
Write-Host "App payload SHA256: $hash"
