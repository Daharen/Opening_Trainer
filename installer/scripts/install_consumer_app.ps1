param(
    [Parameter(Mandatory = $true)]
    [string]$BootstrapRoot,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $true)]
    [string]$DefaultAppRoot,
    [Parameter(Mandatory = $true)]
    [string]$SecondaryAppRoot,
    [Parameter(Mandatory = $false)]
    [string]$OverrideAppRoot,
    [Parameter(Mandatory = $false)]
    [string]$Channel = 'dev',
    [Parameter(Mandatory = $false)]
    [string]$AppVersion = '0.1.0',
    [Parameter(Mandatory = $false)]
    [string]$BuildId = 'bootstrap',
    [Parameter(Mandatory = $false)]
    [string]$PayloadFilename = 'OpeningTrainer-app.zip',
    [Parameter(Mandatory = $false)]
    [string]$PayloadSha256 = '',
    [Parameter(Mandatory = $false)]
    [string]$DefaultManifestUrl = 'https://raw.githubusercontent.com/eric-gitta-moore/Opening_Trainer/main/installer/app_update_manifest.json',
    [Parameter(Mandatory = $false)]
    [string]$UpdaterHelperScriptPath = ''
)

$ErrorActionPreference = 'Stop'

function Test-AppRootWritable {
    param([string]$CandidateRoot)

    $probeRoot = Join-Path $CandidateRoot '.probe'
    $fileA = Join-Path $probeRoot 'write-probe-a.tmp'
    $fileB = Join-Path $probeRoot 'write-probe-b.tmp'
    $result = [ordered]@{ root = $CandidateRoot; ok = $false; detail = '' }

    try {
        New-Item -ItemType Directory -Path $probeRoot -Force | Out-Null
        Set-Content -LiteralPath $fileA -Value 'probe' -Encoding utf8
        Move-Item -LiteralPath $fileA -Destination $fileB -Force
        Add-Content -LiteralPath $fileB -Value 'append' -Encoding utf8
        Remove-Item -LiteralPath $fileB -Force
        Remove-Item -LiteralPath $probeRoot -Force
        $result.ok = $true
        $result.detail = 'create/write/replace/delete/cleanup-ok'
    }
    catch {
        $result.detail = $_.Exception.Message
    }

    return [pscustomobject]$result
}

function Resolve-AppRoot {
    param([string]$OverrideRoot)

    if (-not [string]::IsNullOrWhiteSpace($OverrideRoot)) {
        $overrideResult = Test-AppRootWritable -CandidateRoot $OverrideRoot
        if (-not $overrideResult.ok) {
            throw "User-selected mutable app root failed writable probe: $($overrideResult.root) ($($overrideResult.detail))"
        }
        Write-Host "Writable probe selected explicit override root: $($overrideResult.root)"
        return $overrideResult
    }

    $orderedCandidates = @($DefaultAppRoot, $SecondaryAppRoot)
    foreach ($candidate in $orderedCandidates) {
        $probe = Test-AppRootWritable -CandidateRoot $candidate
        Write-Host "Writable probe root=$($probe.root) ok=$($probe.ok) detail=$($probe.detail)"
        if ($probe.ok) {
            return $probe
        }
    }

    throw 'No writable mutable app roots passed probe. Prompt user for explicit override root and rerun installer.'
}

$selected = Resolve-AppRoot -OverrideRoot $OverrideAppRoot
$targetRoot = $selected.root

if (Test-Path -LiteralPath $targetRoot) {
    Remove-Item -LiteralPath $targetRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $targetRoot -Force | Out-Null
Copy-Item -Path (Join-Path $BootstrapRoot '*') -Destination $targetRoot -Recurse -Force

$updaterRoot = Join-Path $AppStateRoot 'updater'
$logsRoot = Join-Path $AppStateRoot 'logs'
New-Item -ItemType Directory -Path $AppStateRoot -Force | Out-Null
New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
New-Item -ItemType Directory -Path $logsRoot -Force | Out-Null
if (-not [string]::IsNullOrWhiteSpace($UpdaterHelperScriptPath) -and (Test-Path -LiteralPath $UpdaterHelperScriptPath)) {
    Copy-Item -LiteralPath $UpdaterHelperScriptPath -Destination (Join-Path $updaterRoot 'apply_app_update.ps1') -Force
}

$manifestPath = Join-Path $AppStateRoot 'installed_app_manifest.json'
$installedManifest = [ordered]@{
    installed_app_manifest_version = 1
    app_version = $AppVersion
    build_id = $BuildId
    channel = $Channel
    mutable_app_root = $targetRoot
    payload_filename = $PayloadFilename
    payload_sha256 = $PayloadSha256
    installed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    bootstrap_version = $AppVersion
}

$updaterConfigPath = Join-Path $updaterRoot 'updater_config.json'
$updaterConfig = [ordered]@{
    config_version = 1
    channel = $Channel
    manifest_url = $DefaultManifestUrl
    last_checked_utc = $null
}

$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[System.IO.File]::WriteAllText($manifestPath, ($installedManifest | ConvertTo-Json -Depth 8), $utf8NoBom)
[System.IO.File]::WriteAllText($updaterConfigPath, ($updaterConfig | ConvertTo-Json -Depth 8), $utf8NoBom)
Write-Host "Installed app payload root: $targetRoot"
Write-Host "Installed app manifest: $manifestPath"
Write-Host "Updater config: $updaterConfigPath"
