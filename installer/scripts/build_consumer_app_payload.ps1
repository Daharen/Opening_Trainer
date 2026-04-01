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
$stagingRoot = Join-Path $appPayloadDist 'staging'
$updaterHelperSource = Join-Path $repoRoot 'installer\scripts\apply_app_update.ps1'

$appUpdateManifestPath = Join-Path $repoRoot 'installer\app_update_manifest.json'
$payloadIdentityFilename = 'payload_identity.json'

function Copy-WithRetry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourcePath,
        [Parameter(Mandatory = $true)]
        [string]$DestinationPath,
        [int]$RetryCount = 8,
        [int]$RetryDelayMilliseconds = 500
    )

    for ($attempt = 1; $attempt -le $RetryCount; $attempt++) {
        try {
            Copy-Item -LiteralPath $SourcePath -Destination $DestinationPath -Recurse -Force
            return
        }
        catch {
            if ($attempt -eq $RetryCount) {
                throw "Failed to copy '$SourcePath' to '$DestinationPath' after $RetryCount attempts. Last error: $($_.Exception.Message)"
            }
            Start-Sleep -Milliseconds $RetryDelayMilliseconds
        }
    }
}

if (-not $SkipPayloadBuild) {
    & $payloadBuildScript
}

if (-not (Test-Path -LiteralPath $consumerDist)) {
    throw "Consumer payload folder missing: $consumerDist"
}
if (-not (Test-Path -LiteralPath $updaterHelperSource -PathType Leaf)) {
    throw "Updater helper script missing: $updaterHelperSource"
}
if (-not (Test-Path -LiteralPath $appUpdateManifestPath -PathType Leaf)) {
    throw "App update manifest missing: $appUpdateManifestPath"
}

$appUpdateManifest = Get-Content -LiteralPath $appUpdateManifestPath -Raw | ConvertFrom-Json
$payloadIdentityPath = Join-Path $consumerDist $payloadIdentityFilename
$payloadIdentity = [ordered]@{
    marker_schema_version = 1
    app_version = [string]$appUpdateManifest.app_version
    build_id = [string]$appUpdateManifest.build_id
    channel = [string]$appUpdateManifest.channel
    payload_sha256 = [string]$appUpdateManifest.payload_sha256
    generated_at_utc = (Get-Date).ToUniversalTime().ToString('o')
}
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[System.IO.File]::WriteAllText($payloadIdentityPath, ($payloadIdentity | ConvertTo-Json -Depth 8), $utf8NoBom)
Write-Host "Wrote payload identity marker: $payloadIdentityPath"

$consumerUpdaterRoot = Join-Path $consumerDist 'updater'
New-Item -ItemType Directory -Path $consumerUpdaterRoot -Force | Out-Null
Copy-Item -LiteralPath $updaterHelperSource -Destination (Join-Path $consumerUpdaterRoot 'apply_app_update.ps1') -Force

New-Item -ItemType Directory -Path $appPayloadDist -Force | Out-Null
if (Test-Path -LiteralPath $payloadZip) {
    Remove-Item -LiteralPath $payloadZip -Force
}
if (Test-Path -LiteralPath $stagingRoot) {
    Remove-Item -LiteralPath $stagingRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $stagingRoot -Force | Out-Null

Write-Host "Staging app payload files from '$consumerDist' to '$stagingRoot'..."
Get-ChildItem -LiteralPath $consumerDist -Force | ForEach-Object {
    Copy-WithRetry -SourcePath $_.FullName -DestinationPath $stagingRoot
}

Write-Host "Creating app payload zip: $payloadZip"
Compress-Archive -Path (Join-Path $stagingRoot '*') -DestinationPath $payloadZip -CompressionLevel Optimal
if (-not (Test-Path -LiteralPath $payloadZip -PathType Leaf)) {
    throw "App payload zip was not created at expected path: $payloadZip"
}

$hash = (Get-FileHash -LiteralPath $payloadZip -Algorithm SHA256).Hash.ToLowerInvariant()
if (Test-Path -LiteralPath $stagingRoot) {
    Remove-Item -LiteralPath $stagingRoot -Recurse -Force
}
Write-Host "App payload package complete: $payloadZip"
Write-Host "App payload SHA256: $hash"
