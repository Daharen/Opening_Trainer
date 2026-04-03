param(
    [Parameter(Mandatory = $true)]
    [string]$RealHelperPath,
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

$updaterRoot = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'OpeningTrainer\updater'
if ([string]::IsNullOrWhiteSpace($updaterRoot)) {
    $updaterRoot = Join-Path $AppStateRoot 'updater'
}
$wrapperLogPath = Join-Path $updaterRoot 'apply_update.wrapper.log'
$wrapperFailurePath = Join-Path $updaterRoot 'apply_update.wrapper.failure.log'

function Write-WrapperLine {
    param([string]$Message)
    New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
    Add-Content -LiteralPath $wrapperLogPath -Value ("{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message) -Encoding utf8
}

function Write-WrapperFailure {
    param([string]$Message)
    New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
    Add-Content -LiteralPath $wrapperFailurePath -Value ("{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message) -Encoding utf8
}

if ([string]::IsNullOrWhiteSpace($UpdateAttemptId)) {
    $UpdateAttemptId = [guid]::NewGuid().ToString('N')
}

$wrapperPath = $PSCommandPath
$resolvedHelperPath = [System.IO.Path]::GetFullPath($RealHelperPath)
$resolvedAppStateRoot = [System.IO.Path]::GetFullPath($AppStateRoot)
$rawArgs = if ($args -and $args.Count -gt 0) { $args -join ' ' } else { '' }

Write-WrapperLine "WRAPPER_ENTERED update_attempt_id=$UpdateAttemptId wrapper_pid=$PID wrapper_script_path=$wrapperPath cwd=$((Get-Location).Path) ps_version=$($PSVersionTable.PSVersion) helper_path=$resolvedHelperPath app_state_root=$resolvedAppStateRoot manifest_ref=$ManifestPathOrUrl relaunch_exe_path=$RelaunchExePath raw_relaunch_args=$RelaunchArgs wait_pid=$WaitForPid raw_args=$rawArgs"

if ([string]::IsNullOrWhiteSpace($ManifestPathOrUrl)) {
    $message = "WRAPPER_PARAM_INVALID update_attempt_id=$UpdateAttemptId param=ManifestPathOrUrl reason=empty"
    Write-WrapperFailure $message
    throw $message
}
if (-not (Test-Path -LiteralPath $resolvedHelperPath -PathType Leaf)) {
    $message = "WRAPPER_REAL_HELPER_MISSING update_attempt_id=$UpdateAttemptId real_helper_path=$resolvedHelperPath"
    Write-WrapperFailure $message
    throw $message
}

$parsedRelaunchArgs = @('--runtime-mode', 'consumer')
try {
    $candidate = ConvertFrom-Json -InputObject $RelaunchArgs -ErrorAction Stop
    if ($candidate -is [System.Array]) {
        $parsedRelaunchArgs = @($candidate | ForEach-Object { [string]$_ })
    } elseif ($candidate -is [string]) {
        $parsedRelaunchArgs = @([string]$candidate)
    }
} catch {
    Write-WrapperFailure "WRAPPER_RELAUNCH_ARGS_PARSE_FAILED update_attempt_id=$UpdateAttemptId raw_relaunch_args=$RelaunchArgs error=$($_.Exception.Message)"
}
$relaunchArgsJson = ConvertTo-Json -InputObject $parsedRelaunchArgs -Compress -Depth 8

$helperStdoutPath = Join-Path $updaterRoot 'apply_update.wrapper.helper.stdout.log'
$helperStderrPath = Join-Path $updaterRoot 'apply_update.wrapper.helper.stderr.log'
if (Test-Path -LiteralPath $helperStdoutPath) { Remove-Item -LiteralPath $helperStdoutPath -Force }
if (Test-Path -LiteralPath $helperStderrPath) { Remove-Item -LiteralPath $helperStderrPath -Force }

try {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $resolvedHelperPath `
        -ManifestPathOrUrl $ManifestPathOrUrl `
        -AppStateRoot $resolvedAppStateRoot `
        -WaitForPid $WaitForPid `
        -RelaunchExePath $RelaunchExePath `
        -RelaunchArgs $relaunchArgsJson `
        -UpdateAttemptId $UpdateAttemptId `
        1>>$helperStdoutPath 2>>$helperStderrPath
    $helperExit = $LASTEXITCODE
    if ($null -eq $helperExit) { $helperExit = 0 }
    Write-WrapperLine "WRAPPER_REAL_HELPER_COMPLETED update_attempt_id=$UpdateAttemptId helper_exit_code=$helperExit helper_stdout_path=$helperStdoutPath helper_stderr_path=$helperStderrPath"
    if ($helperExit -ne 0) {
        Write-WrapperFailure "WRAPPER_REAL_HELPER_NONZERO_EXIT update_attempt_id=$UpdateAttemptId helper_exit_code=$helperExit"
        exit $helperExit
    }
    exit 0
}
catch {
    $errorText = $_ | Out-String
    Write-WrapperFailure "WRAPPER_REAL_HELPER_EXCEPTION update_attempt_id=$UpdateAttemptId real_helper_path=$resolvedHelperPath error=$errorText"
    exit 1
}
