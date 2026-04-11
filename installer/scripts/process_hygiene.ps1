Set-StrictMode -Version Latest

function Test-PathUnderRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Root
    )

    try {
        $normalizedPath = [System.IO.Path]::GetFullPath($Path).TrimEnd('\')
        $normalizedRoot = [System.IO.Path]::GetFullPath($Root).TrimEnd('\')
    }
    catch {
        return $false
    }

    if ([string]::IsNullOrWhiteSpace($normalizedPath) -or [string]::IsNullOrWhiteSpace($normalizedRoot)) {
        return $false
    }
    return $normalizedPath.StartsWith($normalizedRoot, [System.StringComparison]::OrdinalIgnoreCase)
}

function Get-ProcessesByExecutableRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [int[]]$ExcludeProcessIds = @()
    )

    $rows = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue
    if (-not $rows) {
        return @()
    }

    $matched = @()
    foreach ($row in $rows) {
        $pid = [int]$row.ProcessId
        if ($ExcludeProcessIds -contains $pid) {
            continue
        }
        $exePath = [string]$row.ExecutablePath
        if ([string]::IsNullOrWhiteSpace($exePath)) {
            continue
        }
        if (-not (Test-PathUnderRoot -Path $exePath -Root $Root)) {
            continue
        }
        $matched += [pscustomobject]@{
            pid = $pid
            name = [string]$row.Name
            executable = $exePath
            command_line = [string]$row.CommandLine
        }
    }
    return $matched | Sort-Object pid
}

function Invoke-ProcessCleanupForRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [scriptblock]$Log,
        [int[]]$ExcludeProcessIds = @(),
        [int]$MaxPasses = 6,
        [int]$PollMilliseconds = 350
    )

    & $Log "PROCESS_CLEANUP_BEGIN root=$Root max_passes=$MaxPasses poll_ms=$PollMilliseconds exclude_pids=$($ExcludeProcessIds -join ',')"
    $terminated = @()
    for ($pass = 1; $pass -le $MaxPasses; $pass++) {
        $matches = @(Get-ProcessesByExecutableRoot -Root $Root -ExcludeProcessIds $ExcludeProcessIds)
        if (-not $matches -or $matches.Count -eq 0) {
            & $Log "PROCESS_CLEANUP_PASS pass=$pass state=clean"
            break
        }
        $summary = ($matches | ForEach-Object { "pid=$($_.pid);name=$($_.name);exe=$($_.executable)" }) -join " | "
        & $Log "PROCESS_CLEANUP_PASS pass=$pass state=found count=$($matches.Count) processes=$summary"
        foreach ($proc in $matches) {
            try {
                Stop-Process -Id $proc.pid -ErrorAction Stop
                & $Log "PROCESS_STOP_ATTEMPT pass=$pass pid=$($proc.pid) mode=graceful result=issued"
            }
            catch {
                & $Log "PROCESS_STOP_ATTEMPT pass=$pass pid=$($proc.pid) mode=graceful result=failed error=$($_.Exception.Message)"
            }
        }
        Start-Sleep -Milliseconds $PollMilliseconds
        $stillRunning = @(Get-ProcessesByExecutableRoot -Root $Root -ExcludeProcessIds $ExcludeProcessIds)
        foreach ($proc in $stillRunning) {
            try {
                Stop-Process -Id $proc.pid -Force -ErrorAction Stop
                & $Log "PROCESS_STOP_ATTEMPT pass=$pass pid=$($proc.pid) mode=force result=issued"
                $terminated += $proc.pid
            }
            catch {
                & $Log "PROCESS_STOP_ATTEMPT pass=$pass pid=$($proc.pid) mode=force result=failed error=$($_.Exception.Message)"
            }
        }
        Start-Sleep -Milliseconds $PollMilliseconds
    }

    $remaining = @(Get-ProcessesByExecutableRoot -Root $Root -ExcludeProcessIds $ExcludeProcessIds)
    $terminatedUnique = @($terminated | Sort-Object -Unique)
    if ($remaining.Count -gt 0) {
        $remainingSummary = ($remaining | ForEach-Object { "pid=$($_.pid);name=$($_.name);exe=$($_.executable)" }) -join " | "
        & $Log "PROCESS_CLEANUP_COMPLETE state=blocked terminated_pids=$($terminatedUnique -join ',') remaining=$remainingSummary"
    }
    else {
        & $Log "PROCESS_CLEANUP_COMPLETE state=clean terminated_pids=$($terminatedUnique -join ',')"
    }
    return [pscustomobject]@{
        Remaining = $remaining
        TerminatedProcessIds = $terminatedUnique
    }
}
