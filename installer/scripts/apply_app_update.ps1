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
    [string]$RelaunchArgs = '["--runtime-mode","consumer"]',
    [Parameter(Mandatory = $false)]
    [string]$UpdateAttemptId = ''
)

$ErrorActionPreference = 'Stop'
$processHygieneHelperPath = Join-Path (Split-Path -Parent $PSCommandPath) 'process_hygiene.ps1'
if (Test-Path -LiteralPath $processHygieneHelperPath) {
    . $processHygieneHelperPath
}

$bootstrapUpdaterRoot = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'OpeningTrainer\updater'
if ([string]::IsNullOrWhiteSpace($bootstrapUpdaterRoot)) {
    $bootstrapUpdaterRoot = Join-Path $AppStateRoot 'updater'
}
$bootstrapLogPath = Join-Path $bootstrapUpdaterRoot 'apply_update.bootstrap.log'
$bootstrapFailurePath = Join-Path $bootstrapUpdaterRoot 'apply_update.bootstrap.failure.log'

function Write-BootstrapLine {
    param([string]$Message)
    try {
        New-Item -ItemType Directory -Path $bootstrapUpdaterRoot -Force | Out-Null
        $line = "{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message
        Add-Content -LiteralPath $bootstrapLogPath -Value $line -Encoding utf8
    } catch {
        # Best effort only: bootstrap failure details are written via Write-BootstrapFailure.
    }
}

function Write-BootstrapFailure {
    param([string]$Message)
    try {
        New-Item -ItemType Directory -Path $bootstrapUpdaterRoot -Force | Out-Null
        $line = "{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message
        Add-Content -LiteralPath $bootstrapFailurePath -Value $line -Encoding utf8
    } catch {
        # Avoid recursive failures in bootstrap fatal path.
    }
}

$boundParamsRaw = ''
try {
    $boundParamsRaw = ($PSBoundParameters | ConvertTo-Json -Compress -Depth 4)
} catch {
    $boundParamsRaw = "bound_parameter_json_failure=$($_.Exception.Message)"
}
if ([string]::IsNullOrWhiteSpace($UpdateAttemptId)) {
    $UpdateAttemptId = [guid]::NewGuid().ToString('N')
}
Write-BootstrapLine "BOOTSTRAP_ENTERED update_attempt_id=$UpdateAttemptId helper_pid=$PID script_path=$PSCommandPath cwd=$((Get-Location).Path) app_state_root=$AppStateRoot relaunch_exe_path=$RelaunchExePath wait_pid=$WaitForPid ps_version=$($PSVersionTable.PSVersion) raw_parameters=$boundParamsRaw marker=script_body_entered"

try {
    $updaterRoot = Join-Path $AppStateRoot 'updater'
    $logRoot = Join-Path $AppStateRoot 'logs'
    New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
    $logPath = Join-Path $updaterRoot 'apply_update.log'
    $launchFailurePath = Join-Path $updaterRoot 'apply_update.launch_failure.log'

    try {
        $startupLine = "{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), "UPDATER_HELPER_PROCESS_STARTED update_attempt_id=$UpdateAttemptId helper_pid=$PID app_state_root=$AppStateRoot manifest_ref=$ManifestPathOrUrl wait_pid=$WaitForPid"
        Set-Content -LiteralPath $logPath -Value $startupLine -Encoding utf8
    }
    catch {
        $reason = $_.Exception.Message
        $fallbackLine = "{0} UPDATER_HELPER_PROCESS_START_FAILURE update_attempt_id=$UpdateAttemptId helper_pid=$PID app_state_root=$AppStateRoot manifest_ref=$ManifestPathOrUrl wait_pid=$WaitForPid error=$reason" -f ([DateTime]::UtcNow.ToString('o'))
        Set-Content -LiteralPath $launchFailurePath -Value $fallbackLine -Encoding utf8
        Write-BootstrapLine "APPLY_LOG_INIT_FAILURE update_attempt_id=$UpdateAttemptId log_path=$logPath launch_failure_path=$launchFailurePath reason=$reason"
        Write-BootstrapFailure "APPLY_LOG_INIT_FAILURE update_attempt_id=$UpdateAttemptId log_path=$logPath launch_failure_path=$launchFailurePath reason=$reason"
        throw "Unable to initialize helper log at $logPath. Fallback failure artifact written to $launchFailurePath. error=$reason"
    }

    function Write-Log {
    param([string]$Message)
    $line = "{0} update_attempt_id={1} helper_pid={2} {3}" -f ([DateTime]::UtcNow.ToString('o')), $UpdateAttemptId, $PID, $Message
    Add-Content -LiteralPath $logPath -Value $line -Encoding utf8
}

    function Read-Json {
    param([string]$Path)
    return Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
}

    function Resolve-Manifest {
    param([string]$ManifestRef)
    $dest = Join-Path $updaterRoot 'manifest.latest.json'
    if ($ManifestRef -match '^https?://') {
        Invoke-WebRequest -Uri $ManifestRef -OutFile $dest -UseBasicParsing
        return Read-Json -Path $dest
    }
    $resolvedManifestPath = [System.IO.Path]::GetFullPath($ManifestRef)
    Copy-Item -LiteralPath $resolvedManifestPath -Destination $dest -Force
    return Read-Json -Path $dest
}

    function Assert-StagedPayloadIdentityMatchesManifest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ManifestPath,
        [Parameter(Mandatory = $true)]
        [string]$StagedPayloadIdentityPath
    )
    $manifestIdentity = Read-Json -Path $ManifestPath
    $stagedIdentity = Read-Json -Path $StagedPayloadIdentityPath

    $manifestBuildId = [string]$manifestIdentity.build_id
    $manifestChannel = [string]$manifestIdentity.channel
    $manifestPayloadSha = [string]$manifestIdentity.payload_sha256
    $stagedBuildId = [string]$stagedIdentity.build_id
    $stagedChannel = [string]$stagedIdentity.channel
    $stagedPayloadSha = [string]$stagedIdentity.payload_sha256

    $buildMismatch = $manifestBuildId -ne $stagedBuildId
    $channelMismatch = (-not [string]::IsNullOrWhiteSpace($manifestChannel) -and -not [string]::IsNullOrWhiteSpace($stagedChannel) -and $manifestChannel -ne $stagedChannel)
    $payloadShaMismatch = (-not [string]::IsNullOrWhiteSpace($manifestPayloadSha) -and -not [string]::IsNullOrWhiteSpace($stagedPayloadSha) -and $manifestPayloadSha.ToLowerInvariant() -ne $stagedPayloadSha.ToLowerInvariant())

    if ($buildMismatch -or $channelMismatch -or $payloadShaMismatch) {
        Write-Log ("STAGED_PAYLOAD_IDENTITY_MISMATCH manifest_path={0} staged_identity_path={1} manifest_build_id={2} staged_build_id={3} manifest_channel={4} staged_channel={5} manifest_payload_sha256={6} staged_payload_sha256={7}" -f $ManifestPath, $StagedPayloadIdentityPath, $manifestBuildId, $stagedBuildId, $manifestChannel, $stagedChannel, $manifestPayloadSha, $stagedPayloadSha)
        throw "Manifest/staged payload identity mismatch before swap. manifest_build_id=$manifestBuildId staged_build_id=$stagedBuildId manifest_channel=$manifestChannel staged_channel=$stagedChannel manifest_payload_sha256=$manifestPayloadSha staged_payload_sha256=$stagedPayloadSha manifest_path=$ManifestPath staged_identity_path=$StagedPayloadIdentityPath"
    }

    Write-Log ("STAGED_PAYLOAD_IDENTITY_VERIFIED manifest_path={0} staged_identity_path={1} manifest_build_id={2} staged_build_id={3} manifest_channel={4} staged_channel={5}" -f $ManifestPath, $StagedPayloadIdentityPath, $manifestBuildId, $stagedBuildId, $manifestChannel, $stagedChannel)
}

function Wait-ForProcessExit {
    param([int]$WaitForProcessId, [int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($true) {
        $proc = Get-Process -Id $WaitForProcessId -ErrorAction SilentlyContinue
        if (-not $proc) { return $true }
        if ((Get-Date) -gt $deadline) {
            return $false
        }
        Start-Sleep -Milliseconds 300
    }
}

    function Ensure-ProcessStopped {
    param(
        [Parameter(Mandatory = $true)]
        [int]$WaitForProcessId,
        [int]$GracePeriodSeconds = 6,
        [int]$ForcedWaitSeconds = 10
    )
    Write-Log "PROCESS_EXIT_WAIT_BEGIN wait_pid=$WaitForProcessId grace_period_seconds=$GracePeriodSeconds"
    $exitedDuringGrace = Wait-ForProcessExit -WaitForProcessId $WaitForProcessId -TimeoutSeconds $GracePeriodSeconds
    if ($exitedDuringGrace) {
        Write-Log "PROCESS_EXIT_WAIT_RESULT result=exited_during_grace wait_pid=$WaitForProcessId"
        return
    }

    Write-Log "PROCESS_EXIT_WAIT_RESULT result=still_running_after_grace wait_pid=$WaitForProcessId action=force_terminate"
    try {
        Stop-Process -Id $WaitForProcessId -Force -ErrorAction Stop
        Write-Log "PROCESS_FORCE_TERMINATE_RESULT result=issued wait_pid=$WaitForProcessId"
    } catch {
        $errorMessage = $_.Exception.Message
        Write-Log "PROCESS_FORCE_TERMINATE_RESULT result=failed wait_pid=$WaitForProcessId error=$errorMessage"
    }

    $exitedAfterForce = Wait-ForProcessExit -WaitForProcessId $WaitForProcessId -TimeoutSeconds $ForcedWaitSeconds
    if (-not $exitedAfterForce) {
        throw "Process $WaitForProcessId is still running after force-termination attempt."
    }
    Write-Log "PROCESS_FORCE_TERMINATE_RESULT result=confirmed_stopped wait_pid=$WaitForProcessId forced_wait_seconds=$ForcedWaitSeconds"
}

function Wait-ForMutableRootSwapReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$MutableRoot,
        [int]$TimeoutSeconds = 120
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $probeRoot = "$MutableRoot.swapprobe"
    while ($true) {
        if ((Get-Date) -gt $deadline) {
            throw "Timed out waiting for mutable app root to become swappable."
        }
        if (-not (Test-Path -LiteralPath $MutableRoot)) {
            Write-Log "MUTABLE_ROOT_SWAP_READY skipped=true reason=missing_mutable_root mutable_root=$MutableRoot"
            return
        }
        try {
            if (Test-Path -LiteralPath $probeRoot) {
                Remove-Item -LiteralPath $probeRoot -Recurse -Force
            }
            Move-Item -LiteralPath $MutableRoot -Destination $probeRoot -Force
            Move-Item -LiteralPath $probeRoot -Destination $MutableRoot -Force
            Write-Log "MUTABLE_ROOT_SWAP_READY mutable_root=$MutableRoot"
            return
        } catch {
            $errorMessage = $_.Exception.Message
            Write-Log "MUTABLE_ROOT_LOCK_STILL_PRESENT mutable_root=$MutableRoot error=$errorMessage"
            try {
                if (Test-Path -LiteralPath $probeRoot) {
                    Move-Item -LiteralPath $probeRoot -Destination $MutableRoot -Force
                }
            } catch {
                Write-Log "MUTABLE_ROOT_SWAP_PROBE_RECOVERY_FAILED mutable_root=$MutableRoot error=$($_.Exception.Message)"
            }
            Start-Sleep -Milliseconds 400
        }
    }
}

    function Ensure-MutableRootProcessesStopped {
    param(
        [Parameter(Mandatory = $true)]
        [string]$MutableRoot
    )
    if (-not (Get-Command Stop-TrainerProcessesForRoot -ErrorAction SilentlyContinue)) {
        Write-Log "MUTABLE_ROOT_PROCESS_CLEANUP_SKIPPED mutable_root=$MutableRoot reason=process_hygiene_helper_unavailable helper_path=$processHygieneHelperPath"
        return [pscustomobject]@{ found = @(); terminated = @(); remaining = @() }
    }
    $result = Stop-TrainerProcessesForRoot -PathPrefix $MutableRoot -ExcludeCurrentProcess -Label 'updater_mutable_root' -Logger { param($m) Write-Log $m }
    Write-Log "MUTABLE_ROOT_PROCESS_CLEANUP_SUMMARY mutable_root=$MutableRoot found=$($result.found.Count) terminated=$($result.terminated.Count) remaining=$($result.remaining.Count)"
    return $result
}

    function Relocate-HelperWorkingDirectory {
    param(
        [string]$MutableAppRoot
    )
    $currentLocation = (Get-Location).Path
    Write-Log "HELPER_CWD_BEFORE_RELOCATE cwd=$currentLocation"
    $normalizedCwd = [System.IO.Path]::GetFullPath($currentLocation).TrimEnd('\')
    $normalizedMutableRoot = [System.IO.Path]::GetFullPath($MutableAppRoot).TrimEnd('\')
    if ($normalizedCwd.StartsWith($normalizedMutableRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-Log "HELPER_CWD_INSIDE_MUTABLE_ROOT detected=true cwd=$normalizedCwd mutable_root=$normalizedMutableRoot"
    } else {
        Write-Log "HELPER_CWD_INSIDE_MUTABLE_ROOT detected=false cwd=$normalizedCwd mutable_root=$normalizedMutableRoot"
    }

    $candidateLocations = @(
        (Join-Path $AppStateRoot 'updater'),
        $AppStateRoot
    )
    foreach ($candidate in $candidateLocations) {
        try {
            New-Item -ItemType Directory -Path $candidate -Force | Out-Null
            Set-Location -LiteralPath $candidate
            $relocated = (Get-Location).Path
            Write-Log "HELPER_CWD_AFTER_RELOCATE cwd=$relocated"
            return
        } catch {
            Write-Log "HELPER_CWD_RELOCATE_ATTEMPT_FAILED candidate=$candidate error=$($_.Exception.Message)"
        }
    }
    throw "Unable to relocate helper working directory outside mutable app root."
}

    function Move-WithRetry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Source,
        [Parameter(Mandatory = $true)]
        [string]$Destination,
        [int]$MaxAttempts = 5,
        [int]$DelayMilliseconds = 350
    )
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        Write-Log "SWAP_MOVE_ATTEMPT attempt=$attempt source=$Source destination=$Destination"
        try {
            Move-Item -LiteralPath $Source -Destination $Destination -Force
            Write-Log "SWAP_MOVE_ATTEMPT_OK attempt=$attempt source=$Source destination=$Destination"
            return
        } catch {
            $errorMessage = $_.Exception.Message
            Write-Log "SWAP_MOVE_ATTEMPT_FAILED attempt=$attempt source=$Source destination=$Destination error=$errorMessage"
            if ($attempt -ge $MaxAttempts) {
                throw "Move failed after $MaxAttempts attempts source=$Source destination=$Destination error=$errorMessage"
            }
            Start-Sleep -Milliseconds $DelayMilliseconds
        }
    }
}

    $suppressorTimeoutSeconds = 25
    $suppressorPollMilliseconds = 100

    function Start-ErrorPopupSuppressor {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SuppressorScriptPath,
        [Parameter(Mandatory = $true)]
        [string]$SuppressorLogPath,
        [Parameter(Mandatory = $true)]
        [string]$AttemptId,
        [int]$TimeoutSeconds = 25,
        [int]$PollMilliseconds = 100
    )
    $suppressorScript = @'
$ErrorActionPreference = 'SilentlyContinue'
$ProgressPreference = 'SilentlyContinue'
$attemptId = '__UPDATE_ATTEMPT_ID__'
$timeoutSeconds = __SUPPRESSOR_TIMEOUT_SECONDS__
$pollMilliseconds = __SUPPRESSOR_POLL_MILLISECONDS__
$logPath = '__SUPPRESSOR_LOG_PATH__'
function Write-SuppressorLog {
    param([string]$Message)
    if ([string]::IsNullOrWhiteSpace($logPath)) { return }
    try {
        Add-Content -LiteralPath $logPath -Value ("{0} update_attempt_id={1} helper_pid=popup_suppressor {2}" -f ([DateTime]::UtcNow.ToString('o')), $attemptId, $Message) -Encoding utf8
    } catch {
    }
}
try {
    Add-Type -TypeDefinition @"
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
public static class PopupSuppressorNative {
    public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);
    [DllImport("user32.dll")] public static extern bool EnumWindows(EnumWindowsProc lpEnumFunc, IntPtr lParam);
    [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern bool IsWindow(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern IntPtr SendMessage(IntPtr hWnd, uint Msg, IntPtr wParam, IntPtr lParam);
    [DllImport("user32.dll")] public static extern bool PostMessage(IntPtr hWnd, uint Msg, IntPtr wParam, IntPtr lParam);
    [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern IntPtr SetActiveWindow(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern IntPtr SetFocus(IntPtr hWnd);
    [DllImport("user32.dll", SetLastError=true, CharSet=CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder lpString, int nMaxCount);
    [DllImport("user32.dll", SetLastError=true, CharSet=CharSet.Unicode)] public static extern int GetWindowTextLength(IntPtr hWnd);
    [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint lpdwProcessId);
    public static string ReadWindowText(IntPtr hWnd) {
        int length = GetWindowTextLength(hWnd);
        var sb = new StringBuilder(Math.Max(length + 1, 1024));
        GetWindowText(hWnd, sb, sb.Capacity);
        return sb.ToString();
    }
}
"@ -ErrorAction Stop | Out-Null
    $wmCommand = 0x0111
    $wmClose = 0x0010
    $vkReturn = 0x0D
    $vkSpace = 0x20
    $idOk = 1
    $deadline = (Get-Date).AddSeconds($timeoutSeconds)
    Write-SuppressorLog "POPUP_SUPPRESSOR_STARTED timeout_seconds=$timeoutSeconds poll_milliseconds=$pollMilliseconds popup_signature=title=Error_any"
    while ((Get-Date) -lt $deadline) {
        $topLevelWindows = New-Object 'System.Collections.Generic.List[System.IntPtr]'
        $collector = [PopupSuppressorNative+EnumWindowsProc]{
            param([System.IntPtr]$hWnd, [System.IntPtr]$lParam)
            $topLevelWindows.Add($hWnd) | Out-Null
            return $true
        }
        [PopupSuppressorNative]::EnumWindows($collector, [IntPtr]::Zero) | Out-Null
        foreach ($hWnd in $topLevelWindows) {
            if (-not [PopupSuppressorNative]::IsWindowVisible($hWnd)) { continue }
            $title = [PopupSuppressorNative]::ReadWindowText($hWnd)
            if ($title -cne 'Error') { continue }
            [uint32]$ownerPid = 0
            [PopupSuppressorNative]::GetWindowThreadProcessId($hWnd, [ref]$ownerPid) | Out-Null
            Write-SuppressorLog "POPUP_SUPPRESSOR_MATCH_DETECTED title=Error owner_pid=$ownerPid"
            [PopupSuppressorNative]::SendMessage($hWnd, $wmCommand, [IntPtr]$idOk, [IntPtr]::Zero) | Out-Null
            [PopupSuppressorNative]::SetForegroundWindow($hWnd) | Out-Null
            [PopupSuppressorNative]::SetActiveWindow($hWnd) | Out-Null
            [PopupSuppressorNative]::SetFocus($hWnd) | Out-Null
            [PopupSuppressorNative]::PostMessage($hWnd, 0x0100, [IntPtr]$vkReturn, [IntPtr]::Zero) | Out-Null
            [PopupSuppressorNative]::PostMessage($hWnd, 0x0101, [IntPtr]$vkReturn, [IntPtr]::Zero) | Out-Null
            Write-SuppressorLog "POPUP_SUPPRESSOR_ENTER_SENT owner_pid=$ownerPid"
            [PopupSuppressorNative]::PostMessage($hWnd, 0x0100, [IntPtr]$vkSpace, [IntPtr]::Zero) | Out-Null
            [PopupSuppressorNative]::PostMessage($hWnd, 0x0101, [IntPtr]$vkSpace, [IntPtr]::Zero) | Out-Null
            Write-SuppressorLog "POPUP_SUPPRESSOR_SPACE_SENT owner_pid=$ownerPid"
            [PopupSuppressorNative]::PostMessage($hWnd, $wmClose, [IntPtr]::Zero, [IntPtr]::Zero) | Out-Null
            Write-SuppressorLog "POPUP_SUPPRESSOR_WM_CLOSE_SENT owner_pid=$ownerPid"
            Start-Sleep -Milliseconds 40
            if ([PopupSuppressorNative]::IsWindow($hWnd)) {
                [PopupSuppressorNative]::PostMessage($hWnd, $wmClose, [IntPtr]::Zero, [IntPtr]::Zero) | Out-Null
                Start-Sleep -Milliseconds 40
            }
            if ([PopupSuppressorNative]::IsWindow($hWnd) -and $ownerPid -gt 0) {
                Stop-Process -Id ([int]$ownerPid) -Force -ErrorAction SilentlyContinue
                Write-SuppressorLog "POPUP_SUPPRESSOR_OWNER_KILLED owner_pid=$ownerPid"
            }
        }
        Start-Sleep -Milliseconds $pollMilliseconds
    }
    Write-SuppressorLog "POPUP_SUPPRESSOR_EXITED_AFTER_TIMEOUT timeout_seconds=$timeoutSeconds"
} catch {
    Write-SuppressorLog "POPUP_SUPPRESSOR_EXCEPTION error=$($_.Exception.Message)"
} finally {
    Remove-Item -LiteralPath $PSCommandPath -Force -ErrorAction SilentlyContinue
}
'@
    $suppressorScript = $suppressorScript.Replace('__UPDATE_ATTEMPT_ID__', $AttemptId)
    $suppressorScript = $suppressorScript.Replace('__SUPPRESSOR_TIMEOUT_SECONDS__', [string]$TimeoutSeconds)
    $suppressorScript = $suppressorScript.Replace('__SUPPRESSOR_POLL_MILLISECONDS__', [string]$PollMilliseconds)
    $suppressorScript = $suppressorScript.Replace('__SUPPRESSOR_LOG_PATH__', $SuppressorLogPath.Replace("'", "''"))
    [System.IO.File]::WriteAllText($SuppressorScriptPath, $suppressorScript, [System.Text.UTF8Encoding]::new($false))
    Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -ArgumentList @(
        '-NoProfile',
        '-NonInteractive',
        '-ExecutionPolicy', 'Bypass',
        '-File', $SuppressorScriptPath
    ) -ErrorAction SilentlyContinue | Out-Null
    Write-Log "POPUP_SUPPRESSOR_LAUNCH result=started mode=hidden_detached timeout_seconds=$TimeoutSeconds poll_milliseconds=$PollMilliseconds popup_signature=title=Error_any script_path=$SuppressorScriptPath"
}

    try {
        $phase = 'begin'
        Write-Log "UPDATER_BEGIN manifest_ref=$ManifestPathOrUrl wait_pid=$WaitForPid"
        $suppressorScriptPath = Join-Path $updaterRoot ("popup_suppressor_{0}.ps1" -f $UpdateAttemptId)
        Start-ErrorPopupSuppressor -SuppressorScriptPath $suppressorScriptPath -SuppressorLogPath $logPath -AttemptId $UpdateAttemptId -TimeoutSeconds $suppressorTimeoutSeconds -PollMilliseconds $suppressorPollMilliseconds
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
    Relocate-HelperWorkingDirectory -MutableAppRoot $mutableRoot
    $downloadZip = Join-Path $updaterRoot ([string]$manifest.payload_filename)
    $stagingRoot = Join-Path $updaterRoot 'staging'
    $nextRoot = "$mutableRoot.next"
    $prevRoot = "$mutableRoot.prev"
    if (Test-Path -LiteralPath $stagingRoot) { Remove-Item -LiteralPath $stagingRoot -Recurse -Force }
    if (Test-Path -LiteralPath $nextRoot) { Remove-Item -LiteralPath $nextRoot -Recurse -Force }
    New-Item -ItemType Directory -Path $stagingRoot -Force | Out-Null

    $phase = 'download_begin'
    Write-Log "DOWNLOAD_BEGIN url=$($manifest.payload_url)"
    Invoke-WebRequest -Uri ([string]$manifest.payload_url) -OutFile $downloadZip -UseBasicParsing
    $sha = (Get-FileHash -LiteralPath $downloadZip -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($sha -ne ([string]$manifest.payload_sha256).ToLowerInvariant()) {
        throw "Payload SHA256 mismatch expected=$($manifest.payload_sha256) actual=$sha"
    }
    Write-Log "DOWNLOAD_VERIFIED sha256=$sha"

    $phase = 'extract_payload'
    Expand-Archive -LiteralPath $downloadZip -DestinationPath $stagingRoot -Force
    $stagedPayloadIdentityPath = Join-Path $stagingRoot 'payload_identity.json'
    $manifestLatestPath = Join-Path $updaterRoot 'manifest.latest.json'
    if (-not (Test-Path -LiteralPath $manifestLatestPath)) {
        throw "Latest manifest copy is missing at $manifestLatestPath"
    }
    if (-not (Test-Path -LiteralPath $stagedPayloadIdentityPath)) {
        throw "Extracted payload is missing payload identity marker at $stagedPayloadIdentityPath"
    }
    Assert-StagedPayloadIdentityMatchesManifest -ManifestPath $manifestLatestPath -StagedPayloadIdentityPath $stagedPayloadIdentityPath
    New-Item -ItemType Directory -Path $nextRoot -Force | Out-Null
    Copy-Item -Path (Join-Path $stagingRoot '*') -Destination $nextRoot -Recurse -Force

    $phase = 'wait_for_app_exit'
    Write-Log "HELPER_WAITING_FOR_APP_RELEASE wait_pid=$WaitForPid"
    Ensure-ProcessStopped -WaitForProcessId $WaitForPid
    Write-Log "HELPER_WAIT_FOR_APP_RELEASE_COMPLETE wait_pid=$WaitForPid strategy=polite_then_forceful"
    $phase = 'pre_swap_process_cleanup'
    Ensure-MutableRootProcessesStopped -MutableRoot $mutableRoot | Out-Null
    $phase = 'wait_for_mutable_root_release'
    Wait-ForMutableRootSwapReady -MutableRoot $mutableRoot
    $phase = 'swap_begin'
    Write-Log "SWAP_BEGIN mutable_root=$mutableRoot"
    Write-Log "SWAP_TARGETS mutable_root=$mutableRoot prev_root=$prevRoot next_root=$nextRoot"
    if (Test-Path -LiteralPath $prevRoot) { Remove-Item -LiteralPath $prevRoot -Recurse -Force }
    if (Test-Path -LiteralPath $mutableRoot) {
        $phase = 'swap_move_app_to_prev'
        Move-WithRetry -Source $mutableRoot -Destination $prevRoot
        Write-Log "SWAP_MOVE_APP_TO_PREV_RESULT result=ok source=$mutableRoot destination=$prevRoot"
    }
    $phase = 'swap_move_next_to_app'
    Move-WithRetry -Source $nextRoot -Destination $mutableRoot
    Write-Log "SWAP_MOVE_NEXT_TO_APP_RESULT result=ok source=$nextRoot destination=$mutableRoot"

    $phase = 'installed_manifest_rewrite'
    $installed.app_version = [string]$manifest.app_version
    $installed.build_id = [string]$manifest.build_id
    $installed.channel = [string]$manifest.channel
    $installed.payload_filename = [string]$manifest.payload_filename
    $installed.payload_sha256 = [string]$manifest.payload_sha256
    $installed.installed_at_utc = [DateTime]::UtcNow.ToString('o')
    [System.IO.File]::WriteAllText($installedManifestPath, ($installed | ConvertTo-Json -Depth 12), [System.Text.UTF8Encoding]::new($false))
    Write-Log "INSTALLED_MANIFEST_REWRITE_RESULT result=ok path=$installedManifestPath app_version=$($installed.app_version) build_id=$($installed.build_id)"

    Write-Log "SWAP_OK mutable_root=$mutableRoot"
    $relaunchArgsArray = $null
    try { $relaunchArgsArray = ConvertFrom-Json -InputObject $RelaunchArgs } catch { $relaunchArgsArray = @('--runtime-mode', 'consumer') }
    $phase = 'relaunch'
    if ([string]::IsNullOrWhiteSpace($RelaunchExePath)) {
        $RelaunchExePath = Join-Path $mutableRoot 'OpeningTrainer.exe'
    }
    if (Test-Path -LiteralPath $RelaunchExePath) {
        $delaySeconds = 5
        $relaunchPayload = @{
            exe = [string]$RelaunchExePath
            args = @($relaunchArgsArray | ForEach-Object { [string]$_ })
            delay_seconds = $delaySeconds
            update_attempt_id = [string]$UpdateAttemptId
        } | ConvertTo-Json -Compress -Depth 8
        $relaunchPayloadBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($relaunchPayload))
        $relaunchTrampolinePath = Join-Path $updaterRoot ("relaunch_trampoline_{0}.ps1" -f $UpdateAttemptId)
$relaunchTrampolineScript = @"
`$ErrorActionPreference = 'SilentlyContinue'
`$ProgressPreference = 'SilentlyContinue'
try {
    `$nativeTypeDefinition = @'
using System;
using System.Runtime.InteropServices;
public static class OpeningTrainerNativeMethods {
    [DllImport("kernel32.dll")]
    public static extern uint SetErrorMode(uint uMode);
}
'@
    Add-Type -TypeDefinition `$nativeTypeDefinition -ErrorAction SilentlyContinue | Out-Null
    `$semFailCriticalErrors = 0x0001
    `$semNoGpFaultErrorBox = 0x0002
    `$semNoOpenFileErrorBox = 0x8000
    `$errorModeFlags = [uint32](`$semFailCriticalErrors -bor `$semNoGpFaultErrorBox -bor `$semNoOpenFileErrorBox)
    [OpeningTrainerNativeMethods]::SetErrorMode(`$errorModeFlags) | Out-Null
    [System.Environment]::SetEnvironmentVariable('PYINSTALLER_RESET_ENVIRONMENT', '1', 'Process')
    `$json = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$relaunchPayloadBase64'))
    `$payload = ConvertFrom-Json -InputObject `$json
    `$delaySeconds = [int]`$payload.delay_seconds
    if (`$delaySeconds -lt 1) { `$delaySeconds = 1 }
    Start-Sleep -Seconds `$delaySeconds
    `$argsArray = @()
    if (`$payload.args -is [System.Array]) {
        `$argsArray = @(`$payload.args | ForEach-Object { [string]`$_ })
    } elseif (-not [string]::IsNullOrWhiteSpace([string]`$payload.args)) {
        `$argsArray = @([string]`$payload.args)
    }
    Start-Process -FilePath ([string]`$payload.exe) -ArgumentList `$argsArray -WindowStyle Hidden -ErrorAction SilentlyContinue | Out-Null
} catch {
} finally {
    Remove-Item -LiteralPath `$PSCommandPath -Force -ErrorAction SilentlyContinue
}
"@
        [System.IO.File]::WriteAllText($relaunchTrampolinePath, $relaunchTrampolineScript, [System.Text.UTF8Encoding]::new($false))
        try {
            Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -ArgumentList @(
                '-NoProfile',
                '-NonInteractive',
                '-ExecutionPolicy', 'Bypass',
                '-File', $relaunchTrampolinePath
            ) -ErrorAction Stop | Out-Null
            Write-Log "RELAUNCH_RESULT result=scheduled_detached_trampoline exe=$RelaunchExePath delay_seconds=$delaySeconds trampoline=$relaunchTrampolinePath restart_env=PYINSTALLER_RESET_ENVIRONMENT:1 error_mode_suppression=SetErrorMode:SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX popup_suppressor_timeout_seconds=$suppressorTimeoutSeconds popup_suppressor_poll_milliseconds=$suppressorPollMilliseconds popup_signature=title=Error_any popup_suppressor_start=helper_begin"
        } catch {
            Write-Log "RELAUNCH_RESULT result=schedule_failed exe=$RelaunchExePath delay_seconds=$delaySeconds trampoline=$relaunchTrampolinePath error=$($_.Exception.Message)"
        }
    } else {
        Write-Log "RELAUNCH_RESULT result=skipped missing_exe=$RelaunchExePath"
    }
        Write-Log "UPDATER_FINAL_RESULT result=success"
    }
    catch {
        if ($phase -eq 'wait_for_mutable_root_release' -or $phase -eq 'swap_begin' -or $phase -eq 'swap_move_app_to_prev') {
            try {
                $blockers = Get-TrainerProcessCandidates -PathPrefix $mutableRoot
                Write-Log "MUTABLE_ROOT_FINAL_BLOCKER_SCAN mutable_root=$mutableRoot blockers=$($blockers.Count) phase=$phase"
                foreach ($proc in $blockers) {
                    Write-Log "MUTABLE_ROOT_FINAL_BLOCKER $(Format-TrainerProcessCandidate -Candidate $proc)"
                }
            } catch {
                Write-Log "MUTABLE_ROOT_FINAL_BLOCKER_SCAN_FAILED mutable_root=$mutableRoot phase=$phase error=$($_.Exception.Message)"
            }
        }
        Write-Log "UPDATER_FINAL_RESULT result=failure phase=$phase app_state_root=$AppStateRoot updater_root=$updaterRoot log_path=$logPath manifest_ref=$ManifestPathOrUrl error=$($_.Exception.Message)"
        throw
    }
}
catch {
    $fatal = $_.Exception.Message
    Write-BootstrapFailure "BOOTSTRAP_FATAL update_attempt_id=$UpdateAttemptId helper_pid=$PID error=$fatal script_path=$PSCommandPath cwd=$((Get-Location).Path) app_state_root=$AppStateRoot manifest_ref=$ManifestPathOrUrl wait_pid=$WaitForPid"
    Write-BootstrapLine "BOOTSTRAP_FATAL update_attempt_id=$UpdateAttemptId helper_pid=$PID error=$fatal"
    throw
}
