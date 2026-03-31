param(
    [Parameter(Mandatory = $false)]
    [string]$AppVersion = '0.1.1',
    [Parameter(Mandatory = $false)]
    [string]$Channel = 'dev',
    [Parameter(Mandatory = $false)]
    [string]$ManifestPath = 'installer/app_update_manifest.json',
    [Parameter(Mandatory = $false)]
    [string]$PayloadRepoRelativePath = 'installer/payloads/dev/OpeningTrainer-app.zip',
    [Parameter(Mandatory = $false)]
    [string]$PayloadBaseUrl = 'https://raw.githubusercontent.com/daharen/Opening_Trainer/main'
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
Set-Location $repoRoot

# build_consumer_app_payload.ps1 already owns the consumer payload build flow.
# Do not call build_consumer_payload.ps1 separately here or Windows may still
# have OpeningTrainer.exe locked when the second build tries to clean dist/consumer.
& (Join-Path $repoRoot 'installer/scripts/build_consumer_app_payload.ps1')
if ($LASTEXITCODE -ne 0) { throw 'build_consumer_app_payload.ps1 failed.' }

$sourcePayload = Join-Path $repoRoot 'dist/consumer_app_payload/OpeningTrainer-app.zip'
if (-not (Test-Path -LiteralPath $sourcePayload)) { throw "Missing payload zip: $sourcePayload" }

$payloadPath = Join-Path $repoRoot $PayloadRepoRelativePath
$payloadDir = Split-Path -Parent $payloadPath
New-Item -ItemType Directory -Path $payloadDir -Force | Out-Null
Copy-Item -LiteralPath $sourcePayload -Destination $payloadPath -Force

$sha = (Get-FileHash -LiteralPath $payloadPath -Algorithm SHA256).Hash.ToLowerInvariant()
$commit = (& git rev-parse --short=12 HEAD).Trim()
if ([string]::IsNullOrWhiteSpace($commit)) {
    $commit = (Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss')
}
$publishedAtUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$payloadUrl = ($PayloadBaseUrl.TrimEnd('/') + '/' + $PayloadRepoRelativePath.Replace('\', '/'))

$manifestObj = [ordered]@{
    manifest_version = 1
    channel = $Channel
    app_version = $AppVersion
    build_id = $commit
    payload_filename = [System.IO.Path]::GetFileName($PayloadRepoRelativePath)
    payload_url = $payloadUrl
    payload_sha256 = $sha
    published_at_utc = $publishedAtUtc
    minimum_bootstrap_version = '0.1.0'
    notes = "Dev publish $commit"
}

$manifestFullPath = Join-Path $repoRoot $ManifestPath
[System.IO.File]::WriteAllText($manifestFullPath, ($manifestObj | ConvertTo-Json -Depth 8), [System.Text.UTF8Encoding]::new($false))

Write-Host "Published dev update manifest: $manifestFullPath"
Write-Host "Payload copied to: $payloadPath"
Write-Host "payload_sha256=$sha"
Write-Host "build_id=$commit"
Write-Host 'Next steps:'
Write-Host "  git add $ManifestPath $PayloadRepoRelativePath"
Write-Host "  git commit -m 'Publish dev updater payload $commit'"
Write-Host '  git push origin main'
