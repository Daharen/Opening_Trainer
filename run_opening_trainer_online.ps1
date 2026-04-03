param(
    [ValidateSet("Auto", "OpenOnly", "Dev")]
    [string]$Mode = "Auto",
    [string]$Branch = "main",
    [switch]$AsChild
)

$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$WrapperLogDir = Join-Path $ScriptRoot "logs"
$WrapperLogPath = Join-Path $WrapperLogDir "opening_trainer_online_wrapper.log"

function Ensure-WrapperLogDir {
    if (-not (Test-Path -LiteralPath $WrapperLogDir)) {
        New-Item -ItemType Directory -Path $WrapperLogDir -Force | Out-Null
    }
}

function Write-WrapperLog {
    param([string]$Message)

    Ensure-WrapperLogDir
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] $Message"
    Write-Host $line
    Add-Content -LiteralPath $WrapperLogPath -Value $line
}

function Invoke-GitLogged {
    param(
        [string]$RepoPath,
        [string[]]$Arguments
    )

    $commandText = "git " + ($Arguments -join " ")
    Write-WrapperLog "[git] Running: $commandText"

    $previousLocation = Get-Location
    try {
        Set-Location -LiteralPath $RepoPath
        $output = & git @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        Set-Location -LiteralPath $previousLocation
    }

    $outputText = if ($null -eq $output) { "" } else { ($output | Out-String) }
    if (-not [string]::IsNullOrWhiteSpace($outputText)) {
        foreach ($line in ($outputText -split "`r?`n")) {
            if (-not [string]::IsNullOrWhiteSpace($line)) {
                Write-WrapperLog "[git] $line"
            }
        }
    }

    if ($exitCode -ne 0) {
        $message = "Git command failed (exit=$exitCode): $commandText"
        if (-not [string]::IsNullOrWhiteSpace($outputText)) {
            $message += "`n$outputText"
        }
        throw $message
    }

    return $outputText
}

function Get-GitValueOrEmpty {
    param(
        [string]$RepoPath,
        [string[]]$Arguments
    )

    $value = Invoke-GitLogged -RepoPath $RepoPath -Arguments $Arguments
    if ([string]::IsNullOrWhiteSpace($value)) {
        return ""
    }

    return $value.Trim()
}

function Get-FilteredStatusLines {
    param([string]$RepoPath)

    $statusRaw = Invoke-GitLogged -RepoPath $RepoPath -Arguments @("status", "--short")
    if ([string]::IsNullOrWhiteSpace($statusRaw)) {
        return @()
    }

    $filtered = @()
    foreach ($line in ($statusRaw -split "`r?`n")) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        if ($line -match '^\?\?\s+\.venv(/|$)') {
            continue
        }

        if ($line -match '^\?\?\s+logs(/|$)') {
            continue
        }

        $filtered += $line
    }

    return $filtered
}

function Ensure-RepoSynchronized {
    param(
        [string]$RepoPath,
        [string]$TargetBranch
    )

    $remoteRef = "refs/remotes/origin/$TargetBranch"

    Invoke-GitLogged -RepoPath $RepoPath -Arguments @("fetch", "--all", "--prune")

    try {
        Invoke-GitLogged -RepoPath $RepoPath -Arguments @("show-ref", "--verify", "--quiet", $remoteRef)
    }
    catch {
        throw "Remote ref missing after fetch: $remoteRef. Ensure origin/$TargetBranch exists. $($_.Exception.Message)"
    }

    Invoke-GitLogged -RepoPath $RepoPath -Arguments @("reset", "--hard", "HEAD")
    Invoke-GitLogged -RepoPath $RepoPath -Arguments @("clean", "-ffd", "-e", ".venv", "-e", "logs")

    $checkoutSucceeded = $true
    try {
        Invoke-GitLogged -RepoPath $RepoPath -Arguments @("checkout", "-f", $TargetBranch)
    }
    catch {
        $checkoutSucceeded = $false
        Write-WrapperLog "Primary checkout path failed for branch '$TargetBranch'. Attempting detached recovery path."
        Write-WrapperLog "Primary checkout failure details: $($_.Exception.Message)"
    }

    if (-not $checkoutSucceeded) {
        Invoke-GitLogged -RepoPath $RepoPath -Arguments @("checkout", "--force", "--detach", $remoteRef)
        Invoke-GitLogged -RepoPath $RepoPath -Arguments @("branch", "-f", $TargetBranch, $remoteRef)
        Invoke-GitLogged -RepoPath $RepoPath -Arguments @("checkout", "-f", $TargetBranch)
    }

    Invoke-GitLogged -RepoPath $RepoPath -Arguments @("reset", "--hard", $remoteRef)
    Invoke-GitLogged -RepoPath $RepoPath -Arguments @("clean", "-ffd", "-e", ".venv", "-e", "logs")

    $filteredStatus = Get-FilteredStatusLines -RepoPath $RepoPath
    if ($filteredStatus.Count -gt 0) {
        Write-WrapperLog "Filtered status check found changes:" 
        foreach ($line in $filteredStatus) {
            Write-WrapperLog "[status] $line"
        }
        throw "Repository is not clean after synchronization."
    }

    $head = Get-GitValueOrEmpty -RepoPath $RepoPath -Arguments @("rev-parse", "--verify", "HEAD")
    $remoteHead = Get-GitValueOrEmpty -RepoPath $RepoPath -Arguments @("rev-parse", "--verify", $remoteRef)

    if ([string]::IsNullOrWhiteSpace($head) -or [string]::IsNullOrWhiteSpace($remoteHead) -or $head -ne $remoteHead) {
        Write-WrapperLog "Repository congruence failed."
        Write-WrapperLog "Current HEAD: $head"
        Write-WrapperLog "Target remote ref hash ($remoteRef): $remoteHead"
        $statusLines = Get-FilteredStatusLines -RepoPath $RepoPath
        if ($statusLines.Count -eq 0) {
            Write-WrapperLog "Filtered git status --short: <clean>"
        }
        else {
            Write-WrapperLog "Filtered git status --short:"
            foreach ($line in $statusLines) {
                Write-WrapperLog "[status] $line"
            }
        }

        throw "Repository HEAD does not match $remoteRef."
    }

    Write-WrapperLog "Repository congruence verified."
}

function Get-RepoRoot {
    param([string]$BasePath)

    $repoChild = Join-Path $BasePath "repo"
    if (Test-Path -LiteralPath $repoChild) {
        return $repoChild
    }

    return $BasePath
}

if (-not $AsChild) {
    Write-WrapperLog "Parent wrapper process starting hidden child handoff (Mode=$Mode, Branch=$Branch)."
    $quotedScript = '"' + $MyInvocation.MyCommand.Path + '"'
    $arguments = "-NoProfile -ExecutionPolicy Bypass -File $quotedScript -Mode $Mode -Branch $Branch -AsChild"
    Start-Process -FilePath "powershell.exe" -WindowStyle Hidden -ArgumentList $arguments | Out-Null
    exit 0
}

$repoRoot = Get-RepoRoot -BasePath $ScriptRoot
Write-WrapperLog "Hidden child wrapper started. Repo root resolved to: $repoRoot"

if ($Mode -ne "OpenOnly") {
    Ensure-RepoSynchronized -RepoPath $repoRoot -TargetBranch $Branch
}
else {
    Write-WrapperLog "Mode=OpenOnly requested: skipping repo synchronization."
}

$repoRunScript = Join-Path $repoRoot "run.ps1"
if (-not (Test-Path -LiteralPath $repoRunScript)) {
    throw "Expected repo launcher not found: $repoRunScript"
}

$repoAction = switch ($Mode) {
    "Dev" { "Menu" }
    "OpenOnly" { "Run" }
    default { "Auto" }
}

Write-WrapperLog "Launching repo run.ps1 with Action=$repoAction"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $repoRunScript -Action $repoAction
$repoExitCode = $LASTEXITCODE
Write-WrapperLog "Repo launcher exited with code $repoExitCode"
exit $repoExitCode
