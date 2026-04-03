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
    $line = "{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message
    Add-Content -LiteralPath $wrapperLogPath -Value $line -Encoding utf8
}

function Write-WrapperFailure {
    param([string]$Message)
    New-Item -ItemType Directory -Path $updaterRoot -Force | Out-Null
    $line = "{0} {1}" -f ([DateTime]::UtcNow.ToString('o')), $Message
    Add-Content -LiteralPath $wrapperFailurePath -Value $line -Encoding utf8
}

if ([string]::IsNullOrWhiteSpace($UpdateAttemptId)) {
    $UpdateAttemptId = [guid]::NewGuid().ToString('N')
}

$rawArgs = ''
if ($args -and $args.Count -gt 0) {
    $rawArgs = ($args -join ' ')
}

$boundParamsRaw = ''
try {
    $boundParamsRaw = ($PSBoundParameters | ConvertTo-Json -Compress -Depth 6)
} catch {
    $boundParamsRaw = "bound_parameter_json_failure=$($_.Exception.Message)"
}

$resolvedHelperPath = [System.IO.Path]::GetFullPath($RealHelperPath)
Write-WrapperLine "WRAPPER_ENTERED entered-wrapper=true update_attempt_id=$UpdateAttemptId wrapper_pid=$PID wrapper_script_path=$PSCommandPath cwd=$((Get-Location).Path) powershell_path=$($PSHOME) ps_version=$($PSVersionTable.PSVersion) raw_args=$rawArgs raw_parameters=$boundParamsRaw real_helper_path=$resolvedHelperPath app_state_root=$AppStateRoot relaunch_exe_path=$RelaunchExePath wait_pid=$WaitForPid manifest_ref=$ManifestPathOrUrl"

if (-not (Test-Path -LiteralPath $resolvedHelperPath -PathType Leaf)) {
    $message = "WRAPPER_REAL_HELPER_MISSING update_attempt_id=$UpdateAttemptId real_helper_path=$resolvedHelperPath"
    Write-WrapperFailure $message
    throw $message
}

$helperStdoutPath = Join-Path $updaterRoot 'apply_update.wrapper.helper.stdout.log'
$helperStderrPath = Join-Path $updaterRoot 'apply_update.wrapper.helper.stderr.log'
if (Test-Path -LiteralPath $helperStdoutPath) { Remove-Item -LiteralPath $helperStdoutPath -Force }
if (Test-Path -LiteralPath $helperStderrPath) { Remove-Item -LiteralPath $helperStderrPath -Force }

try {
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $resolvedHelperPath `
        -ManifestPathOrUrl $ManifestPathOrUrl `
        -AppStateRoot $AppStateRoot `
        -WaitForPid $WaitForPid `
        -RelaunchExePath $RelaunchExePath `
        -RelaunchArgs $RelaunchArgs `
        -UpdateAttemptId $UpdateAttemptId `
        1>>$helperStdoutPath 2>>$helperStderrPath
    $helperExit = $LASTEXITCODE
    if ($null -eq $helperExit) {
        $helperExit = 0
    }
    Write-WrapperLine "WRAPPER_REAL_HELPER_COMPLETED update_attempt_id=$UpdateAttemptId real_helper_path=$resolvedHelperPath helper_exit_code=$helperExit helper_stdout_path=$helperStdoutPath helper_stderr_path=$helperStderrPath"
    if (Test-Path -LiteralPath (Join-Path $updaterRoot 'apply_update.bootstrap.log')) {
        Write-WrapperLine "WRAPPER_OBSERVED_HELPER_BOOTSTRAP_LOG update_attempt_id=$UpdateAttemptId"
    }
    if (Test-Path -LiteralPath (Join-Path $updaterRoot 'apply_update.log')) {
        Write-WrapperLine "WRAPPER_OBSERVED_HELPER_APPLY_LOG update_attempt_id=$UpdateAttemptId"
    }
    if ($helperExit -ne 0) {
        $stderrTail = ''
        if (Test-Path -LiteralPath $helperStderrPath) {
            $stderrTail = (Get-Content -LiteralPath $helperStderrPath -Tail 40 -ErrorAction SilentlyContinue) -join ' | '
        }
        Write-WrapperFailure "WRAPPER_REAL_HELPER_NONZERO_EXIT update_attempt_id=$UpdateAttemptId helper_exit_code=$helperExit stderr_tail=$stderrTail"
        exit $helperExit
    }
    exit 0
}
catch {
    $errorText = $_ | Out-String
    Write-WrapperFailure "WRAPPER_REAL_HELPER_EXCEPTION update_attempt_id=$UpdateAttemptId real_helper_path=$resolvedHelperPath error=$errorText"
    exit 1
}
