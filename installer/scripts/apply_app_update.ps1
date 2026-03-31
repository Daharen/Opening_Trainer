param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestPath,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $false)]
    [string]$RelaunchArgs = '--runtime-mode consumer'
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) '..\..')).Path
$pythonExe = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonExe)) {
    throw "Python interpreter not found at $pythonExe"
}

& $pythonExe -m opening_trainer.main --apply-update $ManifestPath
if ($LASTEXITCODE -ne 0) {
    throw "Updater helper failed with exit code $LASTEXITCODE"
}
