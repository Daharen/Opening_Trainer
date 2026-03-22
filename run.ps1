param(
    [ValidateSet("Menu", "Run", "Test", "Compile", "Validate", "All")]
    [string]$Action = "Menu"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = (Get-Location).Path
}

$LogsDir = Join-Path $RepoRoot "logs"
$LogFile = Join-Path $LogsDir "repo_run_log.txt"

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

function Invoke-PythonEntrypoint {
    param(
        [string]$RepoRoot,
        [string]$VenvPython
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
            Log "Launching Python entrypoint: $fullPath"
            $env:PYTHONUNBUFFERED = "1"
            $env:PYTHONIOENCODING = "utf-8"
            & $VenvPython -u $fullPath
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
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython
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
        Invoke-PythonEntrypoint -RepoRoot $RepoRoot -VenvPython $venv.VenvPython
    }

    "Quit" {
        Log "User quit without running an action."
    }

    default {
        throw "Unhandled action: $ResolvedAction"
    }
}

Log "Done."
