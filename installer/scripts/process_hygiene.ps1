Set-StrictMode -Version Latest

function Normalize-ProcessPath {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return '' }
    try {
        return [System.IO.Path]::GetFullPath($Path).TrimEnd('\\').ToLowerInvariant()
    }
    catch {
        return $Path.Trim().TrimEnd('\\').ToLowerInvariant()
    }
}

function Get-TrainerProcessCandidates {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathPrefix,
        [string]$CommandLineContains = ''
    )

    $normalizedPrefix = Normalize-ProcessPath -Path $PathPrefix
    if ([string]::IsNullOrWhiteSpace($normalizedPrefix)) {
        return @()
    }

    $matches = @()
    $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue
    foreach ($proc in $processes) {
        $exePath = [string]$proc.ExecutablePath
        if ([string]::IsNullOrWhiteSpace($exePath)) { continue }
        $normalizedExe = Normalize-ProcessPath -Path $exePath
        if (-not $normalizedExe.StartsWith($normalizedPrefix)) { continue }
        $commandLine = [string]$proc.CommandLine
        if (-not [string]::IsNullOrWhiteSpace($CommandLineContains) -and $commandLine -notlike "*$CommandLineContains*") {
            continue
        }
        $matches += [pscustomobject]@{
            pid = [int]$proc.ProcessId
            name = [string]$proc.Name
            executable_path = $exePath
            command_line = $commandLine
        }
    }
    return @($matches | Sort-Object pid)
}

function Format-TrainerProcessCandidate {
    param($Candidate)
    return "pid=$($Candidate.pid) name=$($Candidate.name) exe=$($Candidate.executable_path) cmd=$($Candidate.command_line)"
}

function Stop-TrainerProcessesForRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathPrefix,
        [scriptblock]$Logger,
        [string]$Label = 'trainer_process_cleanup',
        [string]$CommandLineContains = '',
        [switch]$ExcludeCurrentProcess,
        [int]$WaitTimeoutSeconds = 15,
        [int]$PollMilliseconds = 250
    )

    $log = {
        param([string]$Message)
        if ($null -ne $Logger) {
            & $Logger $Message
        }
    }

    $candidates = @(Get-TrainerProcessCandidates -PathPrefix $PathPrefix -CommandLineContains $CommandLineContains)
    if ($ExcludeCurrentProcess.IsPresent) {
        $candidates = @($candidates | Where-Object { $_.pid -ne $PID })
    }
    & $log "PROCESS_HYGIENE_ENUM label=$Label root=$PathPrefix count=$($candidates.Count)"
    foreach ($candidate in $candidates) {
        & $log "PROCESS_HYGIENE_MATCH label=$Label $(Format-TrainerProcessCandidate -Candidate $candidate)"
    }
    if ($candidates.Count -eq 0) {
        return [pscustomobject]@{
            found = @()
            terminated = @()
            remaining = @()
        }
    }

    $terminated = @()
    foreach ($candidate in $candidates) {
        try {
            Stop-Process -Id $candidate.pid -ErrorAction Stop
            & $log "PROCESS_HYGIENE_STOP_RESULT label=$Label pid=$($candidate.pid) result=graceful_sent"
        }
        catch {
            & $log "PROCESS_HYGIENE_STOP_RESULT label=$Label pid=$($candidate.pid) result=graceful_failed error=$($_.Exception.Message)"
        }
    }

    Start-Sleep -Milliseconds $PollMilliseconds

    foreach ($candidate in $candidates) {
        $isRunning = $null -ne (Get-Process -Id $candidate.pid -ErrorAction SilentlyContinue)
        if ($isRunning) {
            try {
                Stop-Process -Id $candidate.pid -Force -ErrorAction Stop
                & $log "PROCESS_HYGIENE_STOP_RESULT label=$Label pid=$($candidate.pid) result=force_sent"
            }
            catch {
                & $log "PROCESS_HYGIENE_STOP_RESULT label=$Label pid=$($candidate.pid) result=force_failed error=$($_.Exception.Message)"
            }
        }
        else {
            $terminated += $candidate
        }
    }

    $deadline = (Get-Date).AddSeconds($WaitTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $remainingPids = @($candidates | Where-Object { $null -ne (Get-Process -Id $_.pid -ErrorAction SilentlyContinue) })
        if ($remainingPids.Count -eq 0) {
            break
        }
        Start-Sleep -Milliseconds $PollMilliseconds
    }

    $remaining = @($candidates | Where-Object { $null -ne (Get-Process -Id $_.pid -ErrorAction SilentlyContinue) })
    foreach ($candidate in $remaining) {
        & $log "PROCESS_HYGIENE_REMAINING label=$Label $(Format-TrainerProcessCandidate -Candidate $candidate)"
    }
    foreach ($candidate in $candidates) {
        if ($remaining -notcontains $candidate) {
            $terminated += $candidate
        }
    }

    return [pscustomobject]@{
        found = $candidates
        terminated = @($terminated | Sort-Object pid -Unique)
        remaining = $remaining
    }
}
