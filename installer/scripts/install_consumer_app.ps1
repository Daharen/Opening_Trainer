param(
    [Parameter(Mandatory = $true)]
    [string]$BootstrapRoot,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $true)]
    [string]$DefaultAppRoot,
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
    [string]$DefaultManifestUrl = 'https://raw.githubusercontent.com/daharen/Opening_Trainer/main/installer/app_update_manifest.json',
    [Parameter(Mandatory = $false)]
    [string]$UpdaterHelperScriptPath = '',
    [Parameter(Mandatory = $false)]
    [string]$LogPath = '',
    [Parameter(Mandatory = $false)]
    [string]$ContentRoot = ''
)

$ErrorActionPreference = 'Stop'
$script:LogFilePath = $null

function Initialize-AppInstallLog {
    if ([string]::IsNullOrWhiteSpace($LogPath)) {
        $script:LogFilePath = Join-Path $AppStateRoot 'install_consumer_app.log'
    }
    else {
        $script:LogFilePath = $LogPath
    }
    New-Item -ItemType Directory -Path (Split-Path -Parent $script:LogFilePath) -Force | Out-Null
}

function Write-AppInstallLog {
    param([string]$Message)
    $timestamp = (Get-Date).ToUniversalTime().ToString('o')
    $line = "[$timestamp] $Message"
    Add-Content -LiteralPath $script:LogFilePath -Value $line -Encoding utf8
    Write-Host $line
}

function Get-DirectoryTreeSummary {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [int]$MaxEntries = 150
    )

    if (-not (Test-Path -LiteralPath $Root)) {
        return "missing root=$Root"
    }

    $entries = Get-ChildItem -LiteralPath $Root -Recurse -Force -ErrorAction SilentlyContinue | Sort-Object FullName
    if (-not $entries) {
        return "root=$Root entries=0"
    }

    $selected = $entries | Select-Object -First $MaxEntries
    $items = foreach ($entry in $selected) {
        $relative = $entry.FullName.Substring($Root.Length).TrimStart('\\')
        $entryType = if ($entry.PSIsContainer) { 'dir' } else { 'file' }
        "{0}:{1}" -f $entryType, $relative
    }
    $suffix = if ($entries.Count -gt $MaxEntries) { "; truncated=$($entries.Count - $MaxEntries)" } else { '' }
    return "root=$Root entries=$($entries.Count) sample=$($items -join ', ')$suffix"
}

function Assert-PathExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [Parameter(Mandatory = $false)]
        [string]$Phase = 'verification',
        [switch]$Directory
    )

    $pathType = if ($Directory.IsPresent) { 'Container' } else { 'Leaf' }
    $exists = Test-Path -LiteralPath $Path -PathType $pathType
    Write-AppInstallLog "VERIFY phase=$Phase label=$Label path=$Path exists=$exists expectedType=$pathType"
    if (-not $exists) {
        throw "INSTALL_CONSUMER_APP_FAILURE phase=$Phase missing_label=$Label missing_path=$Path expected_type=$pathType"
    }
}

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
        Write-AppInstallLog "Writable probe selected explicit override root: $($overrideResult.root)"
        return $overrideResult
    }

    $defaultProbe = Test-AppRootWritable -CandidateRoot $DefaultAppRoot
    Write-AppInstallLog "Writable probe root=$($defaultProbe.root) ok=$($defaultProbe.ok) detail=$($defaultProbe.detail)"
    if (-not $defaultProbe.ok) {
        throw "Default mutable app root failed writable probe: $($defaultProbe.root) ($($defaultProbe.detail))"
    }

    return $defaultProbe
}

try {
    Initialize-AppInstallLog
    Write-AppInstallLog 'INSTALL_CONSUMER_APP_START'
    Write-AppInstallLog "SCRIPT_PATH=$($MyInvocation.MyCommand.Path)"
    Write-AppInstallLog "WINDOWS_USER=$([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)"
    try {
        $principal = [System.Security.Principal.WindowsPrincipal]::new([System.Security.Principal.WindowsIdentity]::GetCurrent())
        Write-AppInstallLog "IS_ADMIN=$($principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator))"
    }
    catch {
        Write-AppInstallLog "IS_ADMIN=unknown reason=$($_.Exception.Message)"
    }
    Write-AppInstallLog "LOCALAPPDATA=$env:LOCALAPPDATA"
    Write-AppInstallLog "USERPROFILE=$env:USERPROFILE"
    Write-AppInstallLog "BootstrapRoot=$BootstrapRoot"
    Write-AppInstallLog "AppStateRoot=$AppStateRoot"
    Write-AppInstallLog "DefaultAppRoot=$DefaultAppRoot"
    Write-AppInstallLog "OverrideAppRoot=$OverrideAppRoot"
    Write-AppInstallLog "UpdaterHelperScriptPath=$UpdaterHelperScriptPath"
    Write-AppInstallLog "ContentRoot=$ContentRoot"

    $bootstrapExe = Join-Path $BootstrapRoot 'OpeningTrainer.exe'
    $bootstrapUpdaterDir = Join-Path $BootstrapRoot 'updater'
    $bootstrapUpdaterHelper = Join-Path $bootstrapUpdaterDir 'apply_app_update.ps1'
    $bootstrapRootHelper = Join-Path $BootstrapRoot 'apply_app_update.ps1'
    $payloadIdentityMarker = Join-Path $BootstrapRoot 'payload_identity.json'

    Assert-PathExists -Path $BootstrapRoot -Label 'bootstrap_root' -Phase 'source-precheck' -Directory
    Assert-PathExists -Path $bootstrapExe -Label 'bootstrap_executable' -Phase 'source-precheck'
    Assert-PathExists -Path $bootstrapUpdaterDir -Label 'bootstrap_updater_dir' -Phase 'source-precheck' -Directory
    Assert-PathExists -Path $bootstrapUpdaterHelper -Label 'bootstrap_updater_helper' -Phase 'source-precheck'
    Assert-PathExists -Path $payloadIdentityMarker -Label 'bootstrap_payload_identity_marker' -Phase 'source-precheck'

    Write-AppInstallLog "SOURCE_CHECK label=updater_helper_input path=$UpdaterHelperScriptPath exists=$(( -not [string]::IsNullOrWhiteSpace($UpdaterHelperScriptPath)) -and (Test-Path -LiteralPath $UpdaterHelperScriptPath))"
    Write-AppInstallLog "SOURCE_CHECK label=bootstrap_root_helper path=$bootstrapRootHelper exists=$(Test-Path -LiteralPath $bootstrapRootHelper)"
    Write-AppInstallLog "SOURCE_CHECK label=bootstrap_tree_summary summary=$(Get-DirectoryTreeSummary -Root $BootstrapRoot)"

    $selected = Resolve-AppRoot -OverrideRoot $OverrideAppRoot
    $targetRoot = $selected.root
    Write-AppInstallLog "SELECTED_MUTABLE_APP_ROOT=$targetRoot"

    if (Test-Path -LiteralPath $targetRoot) {
        Write-AppInstallLog "Removing existing mutable app root: $targetRoot"
        Remove-Item -LiteralPath $targetRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Path $targetRoot -Force | Out-Null

    Write-AppInstallLog "Copying bootstrap payload from $BootstrapRoot to $targetRoot"
    Copy-Item -Path (Join-Path $BootstrapRoot '*') -Destination $targetRoot -Recurse -Force

    $updaterRoot = Join-Path $AppStateRoot 'updater'
    $logsRoot = Join-Path $AppStateRoot 'logs'
    $mutableUpdaterRoot = Join-Path $targetRoot 'updater'
    New-Item -ItemType Directory -Path $AppStateRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $logsRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $mutableUpdaterRoot -Force | Out-Null

    $helperSourceCandidates = @()
    if (-not [string]::IsNullOrWhiteSpace($UpdaterHelperScriptPath)) {
        $helperSourceCandidates += $UpdaterHelperScriptPath
    }
    $helperSourceCandidates += $bootstrapUpdaterHelper
    $helperSourceCandidates += $bootstrapRootHelper
    Write-AppInstallLog "HELPER_SOURCE_CANDIDATES=$($helperSourceCandidates -join '; ')"

    $resolvedHelperSource = $null
    foreach ($candidate in $helperSourceCandidates) {
        $exists = Test-Path -LiteralPath $candidate -PathType Leaf
        Write-AppInstallLog "HELPER_SOURCE_CHECK path=$candidate exists=$exists"
        if ($exists) {
            $resolvedHelperSource = $candidate
            break
        }
    }
    if ($null -eq $resolvedHelperSource) {
        throw "Updater helper source was not found. Candidates: $($helperSourceCandidates -join '; ')"
    }

    $appStateHelper = Join-Path $updaterRoot 'apply_app_update.ps1'
    $mutableHelper = Join-Path $mutableUpdaterRoot 'apply_app_update.ps1'
    Copy-Item -LiteralPath $resolvedHelperSource -Destination $appStateHelper -Force
    Write-AppInstallLog "Provisioned updater helper to app state: $appStateHelper source=$resolvedHelperSource"
    Copy-Item -LiteralPath $resolvedHelperSource -Destination $mutableHelper -Force
    Write-AppInstallLog "Provisioned updater helper to mutable app payload: $mutableHelper source=$resolvedHelperSource"

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
    Write-AppInstallLog "Provisioned installed app manifest: $manifestPath"
    [System.IO.File]::WriteAllText($updaterConfigPath, ($updaterConfig | ConvertTo-Json -Depth 8), $utf8NoBom)
    Write-AppInstallLog "Provisioned updater config to app state: $updaterConfigPath"
    $mutableUpdaterConfigPath = Join-Path (Join-Path $targetRoot 'updater') 'updater_config.json'
    [System.IO.File]::WriteAllText($mutableUpdaterConfigPath, ($updaterConfig | ConvertTo-Json -Depth 8), $utf8NoBom)
    Write-AppInstallLog "Provisioned updater config to mutable payload: $mutableUpdaterConfigPath"

    $mutablePayloadIdentity = Join-Path $targetRoot 'payload_identity.json'
    Assert-PathExists -Path (Join-Path $targetRoot 'OpeningTrainer.exe') -Label 'mutable_executable' -Phase 'post-copy'
    Assert-PathExists -Path $mutableHelper -Label 'mutable_updater_helper' -Phase 'post-copy'
    Assert-PathExists -Path $mutablePayloadIdentity -Label 'mutable_payload_identity_marker' -Phase 'post-copy'

    Assert-PathExists -Path $manifestPath -Label 'app_state_installed_manifest' -Phase 'post-provision'
    Assert-PathExists -Path $appStateHelper -Label 'app_state_updater_helper' -Phase 'post-provision'
    Assert-PathExists -Path $updaterConfigPath -Label 'app_state_updater_config' -Phase 'post-provision'

    Write-AppInstallLog "TARGET_TREE_SUMMARY mutable=$(Get-DirectoryTreeSummary -Root $targetRoot)"
    Write-AppInstallLog "TARGET_TREE_SUMMARY app_state=$(Get-DirectoryTreeSummary -Root $AppStateRoot)"
    Write-AppInstallLog 'INSTALL_CONSUMER_APP_SUCCESS'
}
catch {
    Write-AppInstallLog "INSTALL_CONSUMER_APP_FAILURE: $($_.Exception.Message)"
    throw
}
