$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..\..')).Path
$installScript = Join-Path $repoRoot 'installer\scripts\install_consumer_app.ps1'
$helperScript = Join-Path $repoRoot 'installer\scripts\apply_app_update.ps1'

if (-not (Test-Path -LiteralPath $installScript -PathType Leaf)) {
    throw "Missing script under validation: $installScript"
}
if (-not (Test-Path -LiteralPath $helperScript -PathType Leaf)) {
    throw "Missing updater helper script: $helperScript"
}

$tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("opening-trainer-app-validate-" + [System.Guid]::NewGuid().ToString('N'))
$bootstrapRoot = Join-Path $tempRoot 'bootstrap_payload'
$appStateRoot = Join-Path $tempRoot 'OpeningTrainer'
$defaultAppRoot = Join-Path $appStateRoot 'App'
$contentRoot = Join-Path $tempRoot 'OpeningTrainerContent'
$logPath = Join-Path $appStateRoot 'install_consumer_app.log'

try {
    New-Item -ItemType Directory -Path $bootstrapRoot -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $bootstrapRoot 'updater') -Force | Out-Null

    Set-Content -LiteralPath (Join-Path $bootstrapRoot 'OpeningTrainer.exe') -Value 'validation-exe-placeholder' -Encoding utf8
    Set-Content -LiteralPath (Join-Path $bootstrapRoot 'payload_identity.json') -Value '{"marker_schema_version":1}' -Encoding utf8
    Copy-Item -LiteralPath $helperScript -Destination (Join-Path $bootstrapRoot 'updater\apply_app_update.ps1') -Force

    Write-Host "Validating install_consumer_app.ps1 via direct invocation"
    Write-Host "Script: $installScript"
    Write-Host "Temp root: $tempRoot"

    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $installScript `
        -BootstrapRoot $bootstrapRoot `
        -AppStateRoot $appStateRoot `
        -DefaultAppRoot $defaultAppRoot `
        -Channel 'dev' `
        -AppVersion '0.1.0-validation' `
        -BuildId 'validation-build' `
        -PayloadFilename 'OpeningTrainer-app.zip' `
        -DefaultManifestUrl 'https://example.invalid/manifest.json' `
        -UpdaterHelperScriptPath $helperScript `
        -ContentRoot $contentRoot `
        -LogPath $logPath

    $requiredFiles = @(
        (Join-Path $appStateRoot 'installed_app_manifest.json'),
        (Join-Path $defaultAppRoot 'OpeningTrainer.exe'),
        (Join-Path $defaultAppRoot 'payload_identity.json'),
        (Join-Path $defaultAppRoot 'updater\apply_app_update.ps1'),
        (Join-Path $appStateRoot 'updater\apply_app_update.ps1'),
        (Join-Path $appStateRoot 'updater\updater_config.json')
    )

    foreach ($required in $requiredFiles) {
        if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
            throw "Validation failed: missing expected output file $required"
        }
    }

    Write-Host "Validation passed: install_consumer_app.ps1 direct invocation produced required outputs."
}
finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}
