param(
    [ValidateSet("Auto", "Menu", "Run", "DevRun", "Test", "Compile", "Validate", "All", "AutoSafe", "DevFast", "DevFull")]
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
$env:OPENING_TRAINER_RUNTIME_MODE = "dev"
$AutoSafeValidationTimeoutSeconds = 90
$DevFastValidationTimeoutSeconds = 150
$DevFullValidationTimeoutSeconds = 600
$AppMutexName = "Local\OpeningTrainer.App." + [Math]::Abs($RepoRoot.ToLowerInvariant().GetHashCode())
$BootMutexName = "Local\OpeningTrainer.Boot." + [Math]::Abs($RepoRoot.ToLowerInvariant().GetHashCode())
$env:OPENING_TRAINER_APP_MUTEX_NAME = $AppMutexName

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

    Log "Validation tool bootstrap: checking pytest availability in repo-local virtual environment..."
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
        Log "Validation tool bootstrap: pytest already available in repo-local virtual environment."
        return
    }

    Log "Validation tool bootstrap: pytest missing in repo-local virtual environment; installing pytest..."
    & $VenvPip install pytest
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to install pytest."
    }
    Log "Validation tool bootstrap: pytest install completed."
}

function Resolve-PytestRunner {
    param(
        [string]$VenvDir,
        [string]$VenvPython
    )

    $pytestEntryPoints = @(
        (Join-Path $VenvDir "Scripts\py.test.exe"),
        (Join-Path $VenvDir "Scripts\pytest.exe")
    )

    foreach ($entryPoint in $pytestEntryPoints) {
        if (Test-Path $entryPoint -PathType Leaf) {
            Write-SessionLogLine -Tag "validation" -Message "pytest entrypoint reused | chosen executable path=$entryPoint"
            Log "Validation pytest runner: pytest entrypoint reused; chosen executable path: $entryPoint"
            return @{
                FilePath = $entryPoint
                ArgumentsPrefix = @()
            }
        }
    }

    Write-SessionLogLine -Tag "validation" -Message "python -m pytest fallback used | chosen executable path=$VenvPython"
    Log "Validation pytest runner: python -m pytest fallback used; chosen executable path: $VenvPython"
    return @{
        FilePath = $VenvPython
        ArgumentsPrefix = @("-m", "pytest")
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
    $sqliteZstPath = "$sqlitePath.zst"

    if ($payloadFormat -eq "sqlite") {
        if (-not (Test-Path $sqlitePath -PathType Leaf) -and -not (Test-Path $sqliteZstPath -PathType Leaf)) {
            return @{ IsValid = $false; Reason = "manifest payload_format=sqlite but sqlite payload is missing (.sqlite or .sqlite.zst)"; ResolvedPath = $candidate }
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

    if ((Test-Path $sqlitePath -PathType Leaf) -or (Test-Path $sqliteZstPath -PathType Leaf)) {
        return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
    }

    if (Test-Path $aggregatePath -PathType Leaf) {
        return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
    }

    return @{ IsValid = $false; Reason = "missing bundle payload; expected data/corpus.sqlite(.zst) or data/aggregated_position_move_counts.jsonl"; ResolvedPath = $candidate }
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
        Write-Host "Selector detection order: manifest payload_format -> data/corpus.sqlite(.zst) -> data/aggregated_position_move_counts.jsonl (legacy)."

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
    return $process
}

function Show-StartupSplash {
    if ($env:OS -ne "Windows_NT") {
        return $null
    }
    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing
    [System.Windows.Forms.Application]::EnableVisualStyles()
    $form = New-Object System.Windows.Forms.Form
    $form.Text = "Opening Trainer"
    $form.Width = 420
    $form.Height = 190
    $form.StartPosition = "CenterScreen"
    $form.FormBorderStyle = "FixedDialog"
    $form.MaximizeBox = $false
    $form.MinimizeBox = $false
    $form.TopMost = $true

    $title = New-Object System.Windows.Forms.Label
    $title.Text = "Opening Trainer is starting"
    $title.Font = New-Object System.Drawing.Font("Segoe UI", 12, [System.Drawing.FontStyle]::Bold)
    $title.AutoSize = $true
    $title.Left = 18
    $title.Top = 18
    $form.Controls.Add($title)

    $stage = New-Object System.Windows.Forms.Label
    $stage.Name = "StageLabel"
    $stage.Text = "Initializing environment"
    $stage.AutoSize = $true
    $stage.Left = 18
    $stage.Top = 60
    $form.Controls.Add($stage)

    $detail = New-Object System.Windows.Forms.Label
    $detail.Name = "DetailLabel"
    $detail.Text = ""
    $detail.Width = 370
    $detail.Height = 46
    $detail.Left = 18
    $detail.Top = 82
    $form.Controls.Add($detail)

    $bar = New-Object System.Windows.Forms.ProgressBar
    $bar.Style = "Marquee"
    $bar.MarqueeAnimationSpeed = 26
    $bar.Width = 370
    $bar.Height = 18
    $bar.Left = 18
    $bar.Top = 132
    $form.Controls.Add($bar)
    $form.Show()
    [System.Windows.Forms.Application]::DoEvents()
    return $form
}

function Update-StartupSplash {
    param(
        [object]$Splash,
        [string]$Stage,
        [string]$Detail = ""
    )
    if ($null -eq $Splash) {
        return
    }
    ($Splash.Controls | Where-Object { $_.Name -eq "StageLabel" } | Select-Object -First 1).Text = $Stage
    ($Splash.Controls | Where-Object { $_.Name -eq "DetailLabel" } | Select-Object -First 1).Text = $Detail
    [System.Windows.Forms.Application]::DoEvents()
}

function Close-StartupSplash {
    param([object]$Splash)
    if ($null -eq $Splash) {
        return
    }
    $Splash.Close()
}

function Try-OpenExistingMutex {
    param([string]$Name)
    try {
        [Threading.Mutex]::OpenExisting($Name) | Out-Null
        return $true
    }
    catch [System.Threading.WaitHandleCannotBeOpenedException] {
        return $false
    }
}

function Wait-ForStartupHandoff {
    param(
        [string]$SessionLogPath,
        [int]$ChildPid,
        [object]$Splash
    )
    $deadline = (Get-Date).AddSeconds(45)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path -LiteralPath $SessionLogPath) {
            $recent = Get-Content -LiteralPath $SessionLogPath -Tail 120 -ErrorAction SilentlyContinue
            if ($recent -match "GUI_READY:") {
                Write-SessionLogLine -Tag "startup" -Message "Launcher observed GUI-ready success."
                return "ready"
            }
            if ($recent -match "GUI_STARTUP_FAILED:") {
                Write-SessionLogLine -Tag "error" -Message "Launcher observed GUI startup failure."
                return "failed"
            }
            if ($recent -match "INSTANCE_DUPLICATE:") {
                Write-SessionLogLine -Tag "startup" -Message "Launcher observed duplicate-instance signal."
                $ownerInfo = $recent | Where-Object { $_ -match "APP_DUPLICATE_OWNER_INFO_AVAILABLE:" } | Select-Object -Last 1
                if ($ownerInfo) {
                    Write-SessionLogLine -Tag "startup" -Message "Launcher found duplicate owner diagnostics."
                }
                return "duplicate"
            }
        }
        $child = Get-Process -Id $ChildPid -ErrorAction SilentlyContinue
        if ($null -eq $child) {
            return "exited"
        }
        Start-Sleep -Milliseconds 150
        if ($null -ne $Splash) {
            [System.Windows.Forms.Application]::DoEvents()
        }
    }
    return "timeout"
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

function Stop-ValidationChildProcesses {
    param(
        [int]$ValidationPid = 0,
        [string]$Reason = "failure",
        [bool]$IncludeStockfishSweep = $false
    )

    Write-SessionLogLine -Tag "validation" -Message "VALIDATION_CHILD_PROCESS_CLEANUP_BEGIN reason=$Reason pid=$ValidationPid stockfish_sweep=$IncludeStockfishSweep"
    Log "Validation child-process cleanup begin (reason=$Reason, pid=$ValidationPid, stockfish_sweep=$IncludeStockfishSweep)."

    if ($ValidationPid -gt 0) {
        try {
            if ($env:OS -eq "Windows_NT") {
                & taskkill /PID $ValidationPid /T /F *> $null
            }
            else {
                Stop-Process -Id $ValidationPid -Force -ErrorAction SilentlyContinue
            }
        }
        catch {
            Log "Validation cleanup note: unable to terminate process tree for pid=$ValidationPid ($($_.Exception.Message))"
        }
    }

    if ($IncludeStockfishSweep) {
        try {
            $stockfish = Get-Process -ErrorAction SilentlyContinue | Where-Object { $_.ProcessName -match "^stockfish($|_)" }
            foreach ($process in $stockfish) {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            }
            if ($stockfish) {
                Log "Validation cleanup sweep terminated stockfish-like processes: $($stockfish.Count)"
            }
        }
        catch {
            Log "Validation cleanup note: stockfish sweep failed ($($_.Exception.Message))"
        }
    }

    Write-SessionLogLine -Tag "validation" -Message "VALIDATION_CHILD_PROCESS_CLEANUP_COMPLETE reason=$Reason"
    Log "Validation child-process cleanup complete (reason=$Reason)."
}

function Invoke-ValidationCommand {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments,
        [int]$TimeoutSeconds,
        [bool]$CleanupStockfishOnFailure = $false
    )

    $argLine = if ($Arguments) { $Arguments -join " " } else { "" }
    $commandText = "$FilePath $argLine".Trim()
    Write-SessionLogLine -Tag "validation" -Message "VALIDATION_COMMAND_BEGIN name=$Name command=$commandText timeout_s=$TimeoutSeconds"
    Log "Validation command [$Name]: $commandText (timeout=${TimeoutSeconds}s)"

    $process = $null
    try {
        $process = Start-Process -FilePath $FilePath -ArgumentList $Arguments -WorkingDirectory $RepoRoot -PassThru
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            Write-SessionLogLine -Tag "validation" -Message "VALIDATION_PROFILE_TIMEOUT command=$Name timeout_s=$TimeoutSeconds"
            Stop-ValidationChildProcesses -ValidationPid $process.Id -Reason "timeout:$Name" -IncludeStockfishSweep $true
            throw "Validation command '$Name' timed out after $TimeoutSeconds seconds."
        }

        if ($process.ExitCode -ne 0) {
            Stop-ValidationChildProcesses -ValidationPid $process.Id -Reason "failure:$Name" -IncludeStockfishSweep $CleanupStockfishOnFailure
            throw "Validation command '$Name' failed with exit code $($process.ExitCode)."
        }
    }
    catch {
        if ($process -and -not $process.HasExited) {
            Stop-ValidationChildProcesses -ValidationPid $process.Id -Reason "exception:$Name" -IncludeStockfishSweep $CleanupStockfishOnFailure
        }
        throw
    }
    finally {
        if ($process) {
            $process.Dispose()
        }
    }

    Write-SessionLogLine -Tag "validation" -Message "VALIDATION_COMMAND_PASS name=$Name"
}

function Invoke-ValidationProfile {
    param(
        [string]$ProfileName,
        [string]$VenvPython
    )

    Write-SessionLogLine -Tag "validation" -Message "VALIDATION_PROFILE_$($ProfileName.ToUpperInvariant())_BEGIN"
    switch ($ProfileName) {
        "AutoSafe" {
            Ensure-Pytest -VenvPython $VenvPython -VenvPip $venv.VenvPip
            $pytestRunner = Resolve-PytestRunner -VenvDir $venv.VenvDir -VenvPython $VenvPython
            Invoke-ValidationCommand -Name "compileall_src" -FilePath $VenvPython -Arguments @("-m", "compileall", "src") -TimeoutSeconds $AutoSafeValidationTimeoutSeconds
            Invoke-ValidationCommand -Name "autosafe_pytest_subset" -FilePath $pytestRunner.FilePath -Arguments @($pytestRunner.ArgumentsPrefix + @("-q", "src/tests/test_launch_paths.py", "src/tests/test_gui_app.py", "src/tests/test_shutdown_and_single_instance.py", "src/tests/test_session_logging.py")) -TimeoutSeconds $AutoSafeValidationTimeoutSeconds -CleanupStockfishOnFailure $true
        }
        "DevFast" {
            Ensure-Pytest -VenvPython $VenvPython -VenvPip $venv.VenvPip
            $pytestRunner = Resolve-PytestRunner -VenvDir $venv.VenvDir -VenvPython $VenvPython
            Invoke-ValidationCommand -Name "compileall_src" -FilePath $VenvPython -Arguments @("-m", "compileall", "src") -TimeoutSeconds $DevFastValidationTimeoutSeconds
            Invoke-ValidationCommand -Name "devfast_pytest_subset" -FilePath $pytestRunner.FilePath -Arguments @($pytestRunner.ArgumentsPrefix + @("-q", "src/tests/test_launch_paths.py", "src/tests/test_gui_app.py", "src/tests/test_shutdown_and_single_instance.py", "src/tests/test_session_logging.py", "src/tests/test_smoke.py")) -TimeoutSeconds $DevFastValidationTimeoutSeconds -CleanupStockfishOnFailure $true
        }
        "DevFull" {
            Ensure-Pytest -VenvPython $VenvPython -VenvPip $venv.VenvPip
            $pytestRunner = Resolve-PytestRunner -VenvDir $venv.VenvDir -VenvPython $VenvPython
            Invoke-ValidationCommand -Name "pytest_full" -FilePath $pytestRunner.FilePath -Arguments @($pytestRunner.ArgumentsPrefix + @("-q")) -TimeoutSeconds $DevFullValidationTimeoutSeconds -CleanupStockfishOnFailure $true
            Invoke-ValidationCommand -Name "compileall_src" -FilePath $VenvPython -Arguments @("-m", "compileall", "src") -TimeoutSeconds $DevFullValidationTimeoutSeconds
        }
        default {
            throw "Unknown validation profile: $ProfileName"
        }
    }
    Write-SessionLogLine -Tag "validation" -Message "VALIDATION_PROFILE_$($ProfileName.ToUpperInvariant())_PASS"
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
    Write-Host "3) Run tests (DevFull profile)"
    Write-Host "4) Compile validation"
    Write-Host "5) Validate (DevFast profile)"
    Write-Host "6) All (DevFast validate + developer run trainer)"
    Write-Host "7) Validate AutoSafe profile"
    Write-Host "8) Validate DevFull profile"
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
        "7" { return "AutoSafe" }
        "8" { return "DevFull" }
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
        $bootMutex = $null
        $splash = Show-StartupSplash
        Update-StartupSplash -Splash $splash -Stage "Initializing environment"
        if (Try-OpenExistingMutex -Name $BootMutexName) {
            Write-SessionLogLine -Tag "startup" -Message "Duplicate launch rejected: ordinary instance is still starting."
            Update-StartupSplash -Splash $splash -Stage "Opening Trainer is already starting" -Detail "An existing ordinary launch is still booting."
            Start-Sleep -Seconds 3
            Close-StartupSplash -Splash $splash
            break
        }
        if (Try-OpenExistingMutex -Name $AppMutexName) {
            Write-SessionLogLine -Tag "startup" -Message "Duplicate launch rejected: ordinary instance is already running."
            Update-StartupSplash -Splash $splash -Stage "Opening Trainer is already running" -Detail "Check the taskbar for the active window."
            Start-Sleep -Seconds 3
            Close-StartupSplash -Splash $splash
            break
        }
        $bootMutex = New-Object Threading.Mutex($true, $BootMutexName)
        try {
            Log "Launching ordinary non-interactive path (AutoSafe validate + run, corpus skip)."
            Update-StartupSplash -Splash $splash -Stage "Validating runtime"
            Write-SessionLogLine -Tag "validation" -Message "Validation profile selected: AutoSafe | timeout protection armed: ${AutoSafeValidationTimeoutSeconds}s"
            Invoke-ValidationProfile -ProfileName "AutoSafe" -VenvPython $venv.VenvPython
            Update-StartupSplash -Splash $splash -Stage "Launching trainer"
            $child = Invoke-PythonEntrypointDetached -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $null
            Update-StartupSplash -Splash $splash -Stage "Opening GUI" -Detail "Waiting for trainer readiness..."
            $handoff = Wait-ForStartupHandoff -SessionLogPath $SessionLogPath -ChildPid $child.Id -Splash $splash
            if ($handoff -eq "ready") {
                Close-StartupSplash -Splash $splash
            }
            elseif ($handoff -eq "duplicate") {
                $ownerInfo = $null
                if (Test-Path -LiteralPath $SessionLogPath) {
                    $ownerInfo = Get-Content -LiteralPath $SessionLogPath -Tail 200 -ErrorAction SilentlyContinue | Where-Object { $_ -match "APP_DUPLICATE_OWNER_INFO_AVAILABLE:" } | Select-Object -Last 1
                }
                if ($ownerInfo) {
                    $detail = ($ownerInfo -split "APP_DUPLICATE_OWNER_INFO_AVAILABLE:\s*", 2 | Select-Object -Last 1).Trim()
                    Update-StartupSplash -Splash $splash -Stage "Opening Trainer is already running" -Detail ("Owner: " + $detail)
                }
                else {
                    Update-StartupSplash -Splash $splash -Stage "Opening Trainer is already running" -Detail "A launch is already active."
                }
                Start-Sleep -Seconds 4
                Close-StartupSplash -Splash $splash
            }
            else {
                Update-StartupSplash -Splash $splash -Stage "Startup failed" -Detail "See session log:`n$SessionLogPath`nDeveloper path: Launch_Opening_Trainer_Dev.cmd"
                Write-SessionLogLine -Tag "error" -Message "Startup handoff ended with state=$handoff"
                Start-Sleep -Seconds 7
                Close-StartupSplash -Splash $splash
            }
        }
        catch {
            Write-SessionLogLine -Tag "validation" -Message "VALIDATION_PROFILE_AUTOSAFE_FAIL reason=$($_.Exception.Message)"
            Update-StartupSplash -Splash $splash -Stage "Validation failed" -Detail "AutoSafe validation failed or timed out.`nSee session log:`n$SessionLogPath"
            Start-Sleep -Seconds 7
            Close-StartupSplash -Splash $splash
            throw
        }
        finally {
            if ($bootMutex -ne $null) {
                $bootMutex.ReleaseMutex() | Out-Null
                $bootMutex.Dispose()
            }
        }
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
        Invoke-ValidationProfile -ProfileName "DevFull" -VenvPython $venv.VenvPython
    }

    "Compile" {
        Invoke-CompileValidation -VenvPython $venv.VenvPython
    }

    "Validate" {
        Invoke-ValidationProfile -ProfileName "DevFast" -VenvPython $venv.VenvPython
    }

    "All" {
        Invoke-ValidationProfile -ProfileName "DevFast" -VenvPython $venv.VenvPython
        $selectedBundle = Select-CorpusBundleDirectory
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $selectedBundle -MirrorConsole $true
    }

    "AutoSafe" {
        Invoke-ValidationProfile -ProfileName "AutoSafe" -VenvPython $venv.VenvPython
    }

    "DevFast" {
        Invoke-ValidationProfile -ProfileName "DevFast" -VenvPython $venv.VenvPython
    }

    "DevFull" {
        Invoke-ValidationProfile -ProfileName "DevFull" -VenvPython $venv.VenvPython
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
