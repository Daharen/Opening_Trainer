param(
    [ValidateSet("Menu", "Run", "Test", "Compile", "Validate", "All")]
    [string]$Action = "Menu"
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

    $aggregatePath = Join-Path $candidate "data\aggregated_position_move_counts.jsonl"
    if (-not (Test-Path $aggregatePath -PathType Leaf)) {
        return @{ IsValid = $false; Reason = "missing aggregated payload"; ResolvedPath = $candidate }
    }

    return @{ IsValid = $true; Reason = $null; ResolvedPath = $candidate }
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
        Write-Host "A valid bundle directory must contain manifest.json and data\aggregated_position_move_counts.jsonl."

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
        if (Test-Path $fullPath) {
            $launchArgs = @("-u", $fullPath)
            if (-not [string]::IsNullOrWhiteSpace($CorpusBundleDir)) {
                $launchArgs += @("--corpus-bundle-dir", $CorpusBundleDir)
                Write-Host "Trainer launch will use corpus bundle: $CorpusBundleDir"
                Log "Trainer launch corpus bundle: $CorpusBundleDir"
                Save-LastCorpusBundleSelection -BundlePath $CorpusBundleDir
            }
            else {
                Write-Host "Trainer launch will proceed without an explicitly selected corpus bundle."
                Log "Trainer launch corpus bundle: skipped"
            }

            Log "Launching Python entrypoint: $fullPath"
            $env:PYTHONUNBUFFERED = "1"
            $env:PYTHONIOENCODING = "utf-8"
            & $VenvPython @launchArgs
            if ($LASTEXITCODE -ne 0) {
                throw "Python entrypoint failed: $fullPath"
            }
            return
        }
    }

    throw "No Python entrypoint found. Checked: $($CandidateFiles -join ', ')"
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
    Write-Host "1) Run trainer"
    Write-Host "2) Run tests"
    Write-Host "3) Compile validation"
    Write-Host "4) Validate (tests + compile)"
    Write-Host "5) All (validate + run trainer)"
    Write-Host "Q) Quit"
    Write-Host ""

    $choice = (Read-Host "Select action").Trim().ToUpperInvariant()

    switch ($choice) {
        "1" { return "Run" }
        "2" { return "Test" }
        "3" { return "Compile" }
        "4" { return "Validate" }
        "5" { return "All" }
        "Q" { return "Quit" }
        default { throw "Unknown selection: $choice" }
    }
}

Ensure-Logs
"" | Set-Content $LogFile

Log "===== Opening Trainer: Repo Run ====="
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
    "Run" {
        $selectedBundle = Select-CorpusBundleDirectory
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $selectedBundle
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
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython -CorpusBundleDir $selectedBundle
    }

    "Quit" {
        Log "User quit without running an action."
    }

    default {
        throw "Unhandled action: $ResolvedAction"
    }
}

Log "Done."
