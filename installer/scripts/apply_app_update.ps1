param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestPathOrUrl,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $true)]
    [int]$WaitForPid,
    [Parameter(Mandatory = $false)]
    [string]$RelaunchExePath = '',
    [Parameter(Mandatory = $false)]
    [string]$RelaunchArgs = '["--runtime-mode","consumer"]'
)

$ErrorActionPreference = 'Stop'

$updaterRoot = Join-Path $AppStateRoot 'updater'
$logRoot = Join-Path $AppStateRoot 'logs'
New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
$logPath = Join-Path $updaterRoot 'apply_update.log'

function Write-Log {
    param([string]$Message)
    $line = "{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message
    Add-Content -LiteralPath $logPath -Value $line -Encoding utf8
}

function Read-Json {
    param([string]$Path)
    return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}

function Resolve-Manifest {
    param([string]$ManifestRef)
    if ($ManifestRef -match '^https?://') {
        $dest = Join-Path $updaterRoot 'manifest.latest.json'
        Invoke-WebRequest -Uri $ManifestRef -OutFile $dest -UseBasicParsing
        return Read-Json -Path $dest
    }
    return Read-Json -Path $ManifestRef
}

function Wait-ForProcessExit {
    param([int]$Pid, [int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($true) {
        $proc = Get-Process -Id $Pid -ErrorAction SilentlyContinue
        if (-not $proc) { return }
        if ((Get-Date) -gt $deadline) {
            throw "Timed out waiting for process $Pid to exit."
        }
        Start-Sleep -Milliseconds 300
    }
}

try {
    Write-Log "UPDATER_BEGIN manifest_ref=$ManifestPathOrUrl wait_pid=$WaitForPid"
    $manifest = Resolve-Manifest -ManifestRef $ManifestPathOrUrl
    $installedManifestPath = Join-Path $AppStateRoot 'installed_app_manifest.json'
    if (-not (Test-Path -LiteralPath $installedManifestPath)) {
        throw "Missing installed app manifest at $installedManifestPath"
    }
    $installed = Read-Json -Path $installedManifestPath
    $mutableRoot = [string]$installed.mutable_app_root
    if ([string]::IsNullOrWhiteSpace($mutableRoot)) {
        throw 'Installed app manifest did not define mutable_app_root.'
    }
    $mutableRoot = [System.IO.Path]::GetFullPath($mutableRoot)
    $downloadZip = Join-Path $updaterRoot ([string]$manifest.payload_filename)
    $stagingRoot = Join-Path $updaterRoot 'staging'
    $nextRoot = "$mutableRoot.next"
    $prevRoot = "$mutableRoot.prev"
    if (Test-Path -LiteralPath $stagingRoot) { Remove-Item -LiteralPath $stagingRoot -Recurse -Force }
    if (Test-Path -LiteralPath $nextRoot) { Remove-Item -LiteralPath $nextRoot -Recurse -Force }
    New-Item -ItemType Directory -Path $stagingRoot -Force | Out-Null

    Write-Log "DOWNLOAD_BEGIN url=$($manifest.payload_url)"
    Invoke-WebRequest -Uri ([string]$manifest.payload_url) -OutFile $downloadZip -UseBasicParsing
    $sha = (Get-FileHash -LiteralPath $downloadZip -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($sha -ne ([string]$manifest.payload_sha256).ToLowerInvariant()) {
        throw "Payload SHA256 mismatch expected=$($manifest.payload_sha256) actual=$sha"
    }
    Write-Log "DOWNLOAD_VERIFIED sha256=$sha"

    Expand-Archive -LiteralPath $downloadZip -DestinationPath $stagingRoot -Force
    New-Item -ItemType Directory -Path $nextRoot -Force | Out-Null
    Copy-Item -Path (Join-Path $stagingRoot '*') -Destination $nextRoot -Recurse -Force

    Wait-ForProcessExit -Pid $WaitForPid
    if (Test-Path -LiteralPath $prevRoot) { Remove-Item -LiteralPath $prevRoot -Recurse -Force }
    if (Test-Path -LiteralPath $mutableRoot) {
        Move-Item -LiteralPath $mutableRoot -Destination $prevRoot -Force
    }
    Move-Item -LiteralPath $nextRoot -Destination $mutableRoot -Force

    $installed.app_version = [string]$manifest.app_version
    $installed.build_id = [string]$manifest.build_id
    $installed.channel = [string]$manifest.channel
    $installed.payload_filename = [string]$manifest.payload_filename
    $installed.payload_sha256 = [string]$manifest.payload_sha256
    $installed.installed_at_utc = [DateTime]::UtcNow.ToString('o')
    [System.IO.File]::WriteAllText($installedManifestPath, ($installed | ConvertTo-Json -Depth 12), [System.Text.UTF8Encoding]::new($false))

    Write-Log "SWAP_OK mutable_root=$mutableRoot"
    $relaunchArgsArray = $null
    try { $relaunchArgsArray = ConvertFrom-Json -InputObject $RelaunchArgs } catch { $relaunchArgsArray = @('--runtime-mode', 'consumer') }
    if ([string]::IsNullOrWhiteSpace($RelaunchExePath)) {
        $RelaunchExePath = Join-Path $mutableRoot 'OpeningTrainer.exe'
    }
    if (Test-Path -LiteralPath $RelaunchExePath) {
        Start-Process -FilePath $RelaunchExePath -ArgumentList $relaunchArgsArray
        Write-Log "RELAUNCH_OK exe=$RelaunchExePath"
    } else {
        Write-Log "RELAUNCH_SKIPPED missing_exe=$RelaunchExePath"
    }
}
catch {
    Write-Log "UPDATER_FAILED error=$($_.Exception.Message)"
    throw
}
