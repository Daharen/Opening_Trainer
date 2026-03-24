param(
    [ValidateSet("Auto", "Menu", "Run", "DevRun", "Test", "Compile", "Validate", "All")]
    [string]$Action = "Auto"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = (Get-Location).Path
}

$WorkspaceRoot = Split-Path -Parent $RepoRoot
if ([string]::IsNullOrWhiteSpace($WorkspaceRoot)) {
    $WorkspaceRoot = $RepoRoot
}

$LogsDir = Join-Path $RepoRoot "logs"
$LogFile = Join-Path $LogsDir "repo_run_log.txt"
$WorkspaceLogsDir = Join-Path $WorkspaceRoot "logs"
$BundleStateFile = Join-Path $WorkspaceLogsDir "repo_last_corpus_bundle.txt"
$SessionLogsDir = Join-Path $WorkspaceLogsDir "sessions"
$SessionId = $env:OPENING_TRAINER_SESSION_ID
if ([string]::IsNullOrWhiteSpace($SessionId)) {
    $SessionId = (Get-Date -Format "yyyyMMddTHHmmssZ") + "-" + $PID
    $env:OPENING_TRAINER_SESSION_ID = $SessionId
}
$SessionLogPath = Join-Path $SessionLogsDir ("session_{0}.log" -f $SessionId)
$env:OPENING_TRAINER_SESSION_LOG_DIR = $SessionLogsDir
$env:OPENING_TRAINER_SESSION_LOG_PATH = $SessionLogPath

function Ensure-Logs {
    if (-not (Test-Path $LogsDir)) {
        New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null
    }
}

function Log {
    param([string]$Message)

    Ensure-Logs
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$timestamp] $Message"
    Write-Host $line
    Add-Content -Path $LogFile -Value $line
    Write-SessionLogLine -Tag "launcher" -Message $Message
}

function Require-Command {
    param([string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found in PATH: $Name"
    }
}

function Ensure-PythonVenv {
    param(
        [string]$RepoRoot,
        [string]$PythonExe
    )

    $VenvDir    = Join-Path $RepoRoot ".venv"
    $VenvPython = Join-Path $VenvDir "Scripts\python.exe"
    $VenvPip    = Join-Path $VenvDir "Scripts\pip.exe"

    if (-not (Test-Path $VenvPython)) {
        Log "Creating virtual environment at $VenvDir"
        & $PythonExe -m venv $VenvDir
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create virtual environment."
        }
    }

    Log "Upgrading pip in repo-local virtual environment..."
    & $VenvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to upgrade pip in virtual environment."
    }

    return @{
        VenvDir    = $VenvDir
        VenvPython = $VenvPython
        VenvPip    = $VenvPip
    }
}

function Install-PythonDependencies {
    param(
        [string]$RepoRoot,
        [string]$VenvPip
    )

    $RequirementsTxt = Join-Path $RepoRoot "requirements.txt"
    $Pyproject       = Join-Path $RepoRoot "pyproject.toml"
    $SetupPy         = Join-Path $RepoRoot "setup.py"

    if (Test-Path $RequirementsTxt) {
        Log "Installing Python dependencies from requirements.txt..."
        & $VenvPip install -r $RequirementsTxt
        if ($LASTEXITCODE -ne 0) {
            throw "pip install -r requirements.txt failed."
        }
        return
    }

    if ((Test-Path $Pyproject) -or (Test-Path $SetupPy)) {
        Log "Installing Python project in editable mode..."
        & $VenvPip install -e $RepoRoot
        if ($LASTEXITCODE -ne 0) {
            throw "pip install -e repo failed."
        }
        return
    }

    Log "No Python dependency file found. Continuing without dependency install."
}

function Ensure-Pytest {
    param(
        [string]$VenvPython,
        [string]$VenvPip
    )

    $oldErrorActionPreference = $ErrorActionPreference
    try {
        $script:ErrorActionPreference = "Continue"

        & $VenvPython -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('pytest') else 1)" *> $null
        $hasPytest = ($LASTEXITCODE -eq 0)
    }
    finally {
        $script:ErrorActionPreference = $oldErrorActionPreference
    }

    if ($hasPytest) {
        return
    }

    Log "pytest not found in repo-local virtual environment. Installing pytest..."
    & $VenvPip install pytest
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install pytest."
    }
}

function Ensure-WorkspaceLogs {
    if (-not (Test-Path $WorkspaceLogsDir)) {
        New-Item -ItemType Directory -Force -Path $WorkspaceLogsDir | Out-Null
    }
    if (-not (Test-Path $SessionLogsDir)) {
        New-Item -ItemType Directory -Force -Path $SessionLogsDir | Out-Null
    }
}

function Write-SessionLogLine {
    param(
        [string]$Tag,
        [string]$Message
    )

    Ensure-WorkspaceLogs
    $timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ")
    $line = "[$timestamp] [$Tag] $Message"
    Add-Content -LiteralPath $SessionLogPath -Value $line
}

function Prune-SessionLogs {
    Ensure-WorkspaceLogs
    $files = Get-ChildItem -LiteralPath $SessionLogsDir -Filter "session_*.log" -File | Sort-Object LastWriteTime -Descending
    if ($files.Count -le 5) {
        return
    }
    $files | Select-Object -Skip 5 | ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
}

function Test-CorpusBundleDirectory {
    param([string]$BundlePath)

    if ([string]::IsNullOrWhiteSpace($BundlePath)) {
        return @{ IsValid = $false; Reason = "path not found"; ResolvedPath = $null }
    }

    $candidate = [System.IO.Path]::GetFullPath($BundlePath)

    if (-not (Test-Path $candidate)) {
        return @{ IsValid = $false; Reason = "path not found"; ResolvedPath = $candidate }
    }

    $item = Get-Item -LiteralPath $candidate
    if (-not $item.PSIsContainer) {
        return @{ IsValid = $false; Reason = "path is not a directory"; ResolvedPath = $candidate }
    }

    $manifestPath = Join-Path $candidate "manifest.json"
    if (-not (Test-Path $manifestPath -PathType Leaf)) {
        return @{ IsValid = $false; Reason = "missing manifest"; ResolvedPath = $candidate }
    }

    try {
        $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json -AsHashtable
    }
    catch {
        return @{ IsValid = $false; Reason = "manifest is not valid JSON"; ResolvedPath = $candidate }
    }

    $payloadFormat = $null
    if ($manifest.ContainsKey("payload_format") -and -not [string]::IsNullOrWhiteSpace([string]$manifest.payload_format)) {
        $payloadFormat = ([string]$manifest.payload_format).Trim().ToLowerInvariant()
    }

    $sqliteRelativePath = if ($manifest.ContainsKey("sqlite_corpus_file")) {
        [string]$manifest.sqlite_corpus_file
    }
    elseif ($manifest.ContainsKey("corpus_sqlite_file")) {
        [string]$manifest.corpus_sqlite_file
    }
    elseif ($manifest.ContainsKey("payload_file")) {
        [string]$manifest.payload_file
    }
    else {
        "data/corpus.sqlite"
    }
    $aggregateRelativePath = if ($manifest.ContainsKey("aggregate_position_file")) {
        [string]$manifest.aggregate_position_file
    }
    else {
        "data/aggregated_position_move_counts.jsonl"
    }

    $sqlitePath = Join-Path $candidate $sqliteRelativePath
    $aggregatePath = Join-Path $candidate $aggregateRelativePath

    if ($payloadFormat -eq "sqlite") {
        if (-not (Test-Path $sqlitePath -PathType Leaf)) {
            return @{ IsValid = $false; Reason = "manifest payload_format=sqlite but sqlite payload is missing"; ResolvedPath = $candidate }
        }
        return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
    }

    if ($payloadFormat -eq "jsonl") {
        if (-not (Test-Path $aggregatePath -PathType Leaf)) {
            return @{ IsValid = $false; Reason = "manifest payload_format=jsonl but aggregate payload is missing"; ResolvedPath = $candidate }
        }
        return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
    }

    if ($payloadFormat) {
        return @{ IsValid = $false; Reason = "unsupported payload_format '$payloadFormat'"; ResolvedPath = $candidate }
    }

    if (Test-Path $sqlitePath -PathType Leaf) {
        return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
    }

    if (Test-Path $aggregatePath -PathType Leaf) {
        return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
    }

    return @{ IsValid = $false; Reason = "missing bundle payload; expected data/corpus.sqlite or data/aggregated_position_move_counts.jsonl"; ResolvedPath = $candidate }
}

function Get-DiscoveredCorpusBundles {
    $artifactsDir = Join-Path $WorkspaceRoot "artifacts"
    $results = @()

    if (-not (Test-Path $artifactsDir -PathType Container)) {
        return $results
    }

    foreach ($directory in Get-ChildItem -LiteralPath $artifactsDir -Directory | Sort-Object Name) {
        $validation = Test-CorpusBundleDirectory -BundlePath $directory.FullName
        if ($validation.IsValid) {
            $results += [PSCustomObject]@{
                Name = $directory.Name
                Path = $validation.ResolvedPath
            }
        }
    }

    return $results
}

function Get-LastCorpusBundleSelection {
    Ensure-WorkspaceLogs

    if (-not (Test-Path $BundleStateFile -PathType Leaf)) {
        return $null
    }

    $storedPath = (Get-Content -LiteralPath $BundleStateFile -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    if ([string]::IsNullOrWhiteSpace($storedPath)) {
        return $null
    }

    $validation = Test-CorpusBundleDirectory -BundlePath $storedPath
    if ($validation.IsValid) {
        return $validation.ResolvedPath
    }

    return $null
}

function Save-LastCorpusBundleSelection {
    param([string]$BundlePath)

    Ensure-WorkspaceLogs
    Set-Content -LiteralPath $BundleStateFile -Value $BundlePath
}

function Select-CorpusBundleDirectory {
    $artifactsDir = Join-Path $WorkspaceRoot "artifacts"
    $discoveredBundles = @(Get-DiscoveredCorpusBundles)
    $lastSelection = Get-LastCorpusBundleSelection

    while ($true) {
        Write-Host ""
        Write-Host "Optional corpus bundle selection before trainer launch"
        Write-Host "Choose a numbered bundle from workspace-root artifacts, enter L to reuse the last bundle, enter C to paste a custom path, or press Enter/S to skip."
        Write-Host "A valid bundle directory must contain manifest.json and a supported payload."
        Write-Host "Selector detection order: manifest payload_format -> data/corpus.sqlite -> data/aggregated_position_move_counts.jsonl (legacy)."

        if ($lastSelection) {
            Write-Host "L) Reuse last selected bundle [$lastSelection]"
        }

        if ($discoveredBundles.Count -gt 0) {
            Write-Host ""
            Write-Host "Discovered bundle directories under $($WorkspaceRoot):"
            for ($i = 0; $i -lt $discoveredBundles.Count; $i++) {
                Write-Host ("{0}) {1}" -f ($i + 1), $discoveredBundles[$i].Path)
            }
        }
        else {
            Write-Host ""
            Write-Host "No valid bundle directories were discovered under $artifactsDir."
        }

        Write-Host "C) Paste custom bundle path"
        Write-Host "S) Skip bundle selection for this run"
        Write-Host ""

        $selection = (Read-Host "Bundle selection").Trim()
        if ([string]::IsNullOrWhiteSpace($selection) -or $selection.ToUpperInvariant() -eq "S") {
            return $null
        }

        if ($selection.ToUpperInvariant() -eq "L") {
            if (-not $lastSelection) {
                Write-Host "Last used bundle is not available. Please choose another option." -ForegroundColor Yellow
                continue
            }

            return $lastSelection
        }

        if ($selection.ToUpperInvariant() -eq "C") {
            $customPath = (Read-Host "Enter custom bundle path (absolute or relative to $RepoRoot)").Trim()
            if ([string]::IsNullOrWhiteSpace($customPath)) {
                return $null
            }

            $validation = Test-CorpusBundleDirectory -BundlePath $customPath
            if ($validation.IsValid) {
                return $validation.ResolvedPath
            }

            Write-Host "Bundle validation failed: $($validation.Reason)." -ForegroundColor Yellow
            $retry = (Read-Host "Press Enter to retry bundle selection or type S to skip").Trim()
            if ($retry.ToUpperInvariant() -eq "S") {
                return $null
            }
            continue
        }

        $selectedIndex = 0
        if ([int]::TryParse($selection, [ref]$selectedIndex)) {
            if ($selectedIndex -ge 1 -and $selectedIndex -le $discoveredBundles.Count) {
                $candidatePath = $discoveredBundles[$selectedIndex - 1].Path
                $validation = Test-CorpusBundleDirectory -BundlePath $candidatePath
                if ($validation.IsValid) {
                    return $validation.ResolvedPath
                }

                Write-Host "Bundle validation failed: $($validation.Reason)." -ForegroundColor Yellow
                $retry = (Read-Host "Press Enter to retry bundle selection or type S to skip").Trim()
                if ($retry.ToUpperInvariant() -eq "S") {
                    return $null
                }
                continue
            }
        }

        Write-Host "Unknown bundle selection: $selection" -ForegroundColor Yellow
    }
}

function Invoke-PythonEntrypoint {
    param(
        [string]$RepoRoot,
        [string]$VenvPython,
        [string]$CorpusBundleDir = $null,
        [bool]$MirrorConsole = $true
    )

    $resolved = Resolve-PythonEntrypoint -RepoRoot $RepoRoot -CorpusBundleDir $CorpusBundleDir
    $fullPath = $resolved.Path
    $launchArgs = @("-u", $fullPath) + $resolved.ExtraArgs

    Log "Launching Python entrypoint: $fullPath"
    $env:PYTHONUNBUFFERED = "1"
    $env:PYTHONIOENCODING = "utf-8"
    $env:OPENING_TRAINER_CONSOLE_MIRROR = if ($MirrorConsole) { "1" } else { "0" }
    Write-SessionLogLine -Tag "startup" -Message "Launching trainer entrypoint $fullPath"
    & $VenvPython @launchArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Python entrypoint failed: $fullPath"
    }
}

function Resolve-PythonEntrypoint {
    param(
        [string]$RepoRoot,
        [string]$CorpusBundleDir = $null
    )

    $CandidateFiles = @(
        "main.py",
        "run_trainer.py",
        "app.py",
        "opening_trainer.py",
        "src\main.py",
        "src\app.py"
    )

    foreach ($relativePath in $CandidateFiles) {
        $fullPath = Join-Path $RepoRoot $relativePath
        if (-not (Test-Path $fullPath)) {
            continue
        }

        $extraArgs = @()
        if (-not [string]::IsNullOrWhiteSpace($CorpusBundleDir)) {
            $extraArgs += @("--corpus-bundle-dir", $CorpusBundleDir)
            Write-Host "Trainer launch will use corpus bundle: $CorpusBundleDir"
            Log "Trainer launch corpus bundle: $CorpusBundleDir"
            Save-LastCorpusBundleSelection -BundlePath $CorpusBundleDir
        }
        else {
            Write-Host "Trainer launch will proceed without an explicitly selected corpus bundle."
            Log "Trainer launch corpus bundle: skipped"
        }

        return @{
            Path = $fullPath
            ExtraArgs = $extraArgs
        }
    }

    throw "No Python entrypoint found. Checked: $($CandidateFiles -join ', ')"
}

function Invoke-PythonEntrypointDetached {
    param(
        [string]$RepoRoot,
        [string]$VenvPython,
        [string]$CorpusBundleDir = $null
    )

    $resolved = Resolve-PythonEntrypoint -RepoRoot $RepoRoot -CorpusBundleDir $CorpusBundleDir
    $fullPath = $resolved.Path
    $scriptArgs = @($fullPath) + $resolved.ExtraArgs
    $pythonwPath = Join-Path (Split-Path -Parent $VenvPython) "pythonw.exe"
    $launcherExe = if (Test-Path $pythonwPath) { $pythonwPath } else { $VenvPython }
    $usesPythonw = [System.StringComparer]::OrdinalIgnoreCase.Equals([System.IO.Path]::GetFileName($launcherExe), "pythonw.exe")

    $env:PYTHONUNBUFFERED = "1"
    $env:PYTHONIOENCODING = "utf-8"
    $env:OPENING_TRAINER_CONSOLE_MIRROR = "0"
    Write-SessionLogLine -Tag "startup" -Message "Ordinary launch handoff prepared (detached executable=$launcherExe)"

    try {
        $startInfo = @{
            FilePath = $launcherExe
            ArgumentList = $scriptArgs
            WorkingDirectory = $RepoRoot
            ErrorAction = "Stop"
        }
        if (-not $usesPythonw) {
            $startInfo.WindowStyle = "Hidden"
        }
        $process = Start-Process @startInfo -PassThru
    }
    catch {
        Write-SessionLogLine -Tag "error" -Message "Ordinary launch spawn failed: $($_.Exception.Message)"
        throw
    }

    if ($null -eq $process -or $process.Id -le 0) {
        Write-SessionLogLine -Tag "error" -Message "Ordinary launch spawn failed: no process id returned."
        throw "Detached launch failed: process was not created."
    }

    Write-SessionLogLine -Tag "startup" -Message "Ordinary launch handoff complete (pid=$($process.Id))."
    Log "Detached ordinary launch started (pid=$($process.Id)) via $([System.IO.Path]::GetFileName($launcherExe))."
}

function Invoke-TestSuite {
    param([string]$VenvPython)

    Log "Running pytest..."
    & $VenvPython -m pytest -q
    if ($LASTEXITCODE -ne 0) {
        throw "pytest failed."
    }
}

function Invoke-CompileValidation {
    param([string]$VenvPython)

    if (Test-Path (Join-Path $RepoRoot "src")) {
        Log "Running compile validation for src..."
        & $VenvPython -m compileall src
    }
    else {
        Log "Running compile validation for repo root..."
        & $VenvPython -m compileall .
    }

    if ($LASTEXITCODE -ne 0) {
        throw "compileall failed."
    }
}

function Resolve-Action {
    param([string]$RequestedAction)

    if ($RequestedAction -ne "Menu") {
        return $RequestedAction
    }

    Write-Host ""
    Write-Host "Opening Trainer repo runner"
    Write-Host "1) Run trainer (ordinary non-interactive)"
    Write-Host "2) Developer run trainer (with optional console corpus selection)"
    Write-Host "3) Run tests"
    Write-Host "4) Compile validation"
    Write-Host "5) Validate (tests + compile)"
    Write-Host "6) All (validate + developer run trainer)"
    Write-Host "Q) Quit"
    Write-Host ""

    $choice = (Read-Host "Select action").Trim().ToUpperInvariant()

    switch ($choice) {
        "1" { return "Auto" }
        "2" { return "DevRun" }
        "3" { return "Test" }
        "4" { return "Compile" }
        "5" { return "Validate" }
        "6" { return "All" }
        "Q" { return "Quit" }
        default { throw "Unknown selection: $choice" }
    }
}

Ensure-WorkspaceLogs
Prune-SessionLogs
Write-SessionLogLine -Tag "launcher" -Message "Bootstrap: run.ps1 starting action $Action"
Ensure-Logs
"" | Set-Content $LogFile

Log "===== Opening Trainer: Repo Run ====="
Write-SessionLogLine -Tag "startup" -Message "Session id: $SessionId"
Log "RepoRoot: $RepoRoot"

Set-Location $RepoRoot

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "python was not found in PATH."
}

$venv = Ensure-PythonVenv -RepoRoot $RepoRoot -PythonExe $pythonCmd.Source
Install-PythonDependencies -RepoRoot $RepoRoot -VenvPip $venv.VenvPip

$ResolvedAction = Resolve-Action -RequestedAction $Action
Log "Action: $ResolvedAction"

switch ($ResolvedAction) {
    "Auto" {
        Log "Launching ordinary non-interactive path (validate + run, corpus skip)."
        Ensure-Pytest -VenvPython $venv.VenvPython -VenvPip $venv.VenvPip
        Invoke-TestSuite -VenvPython $venv.VenvPython
        Invoke-CompileValidation -VenvPython $venv.VenvPython
        Invoke-PythonEntrypointDetached -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $null
    }

    "Run" {
        Log "Launching desktop-first trainer without pre-GUI console corpus selection."
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -MirrorConsole $true
    }

    "DevRun" {
        $selectedBundle = Select-CorpusBundleDirectory
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $selectedBundle -MirrorConsole $true
    }

    "Test" {
        Ensure-Pytest -VenvPython $venv.VenvPython -VenvPip $venv.VenvPip
        Invoke-TestSuite -VenvPython $venv.VenvPython
    }

    "Compile" {
        Invoke-CompileValidation -VenvPython $venv.VenvPython
    }

    "Validate" {
        Ensure-Pytest -VenvPython $venv.VenvPython -VenvPip $venv.VenvPip
        Invoke-TestSuite -VenvPython $venv.VenvPython
        Invoke-CompileValidation -VenvPython $venv.VenvPython
    }

    "All" {
        Ensure-Pytest -VenvPython $venv.VenvPython -VenvPip $venv.VenvPip
        Invoke-TestSuite -VenvPython $venv.VenvPython
        Invoke-CompileValidation -VenvPython $venv.VenvPython
        $selectedBundle = Select-CorpusBundleDirectory
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $selectedBundle -MirrorConsole $true
    }

    "Quit" {
        Log "User quit without running an action."
    }

    default {
        throw "Unhandled action: $ResolvedAction"
    }
}

Prune-SessionLogs
Log "Done."
