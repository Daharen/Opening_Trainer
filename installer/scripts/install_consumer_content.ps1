param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestPath,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $true)]
    [string]$ContentRoot,
    [Parameter(Mandatory = $false)]
    [string]$LogPath,
    [Parameter(Mandatory = $false)]
    [string]$LocalArchivePath
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path -Path $AppStateRoot -ChildPath 'install_consumer_content.log'
}

function Write-InstallLog {
    param([string]$Message)
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff'
    $line = "[$timestamp] $Message"
    Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
}

function Write-JsonFileNoBom {
    param(
        [string]$Path,
        [object]$Payload
    )

    $json = $Payload | ConvertTo-Json -Depth 10
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($Path, $json, $utf8NoBom)
    Write-InstallLog "JSON write (utf8-no-bom): $Path"
}

function Set-Phase {
    param(
        [string]$Phase,
        [int]$Percent = 0
    )
    Write-Host "==> $Phase"
    Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status $Phase -PercentComplete $Percent
    Write-InstallLog "PHASE: $Phase"
}

function Get-MissingRequiredEntries {
    param(
        [string]$Root,
        [object[]]$RequiredEntryGroups
    )

    $missing = @()
    foreach ($group in $RequiredEntryGroups) {
        $candidates = @($group | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } | ForEach-Object { [string]$_ })
        if (-not $candidates -or $candidates.Count -eq 0) {
            continue
        }
        $groupSatisfied = $false
        foreach ($relativePath in $candidates) {
            $requiredPath = Join-Path -Path $Root -ChildPath $relativePath
            if (Test-Path -LiteralPath $requiredPath) {
                $groupSatisfied = $true
                break
            }
        }
        if (-not $groupSatisfied) {
            $missing += ($candidates -join ' | ')
        }
    }

    return $missing
}

function Get-RequiredEntryGroups {
    param(
        [string[]]$RequiredEntries,
        [object]$RequiredEntryAlternates
    )

    $groups = @()
    foreach ($entry in $RequiredEntries) {
        if ([string]::IsNullOrWhiteSpace([string]$entry)) {
            continue
        }

        $normalizedEntry = [string]$entry
        $alternates = @()

        if (
            $RequiredEntryAlternates -and
            $RequiredEntryAlternates.PSObject -and
            $RequiredEntryAlternates.PSObject.Properties.Name -contains $normalizedEntry
        ) {
            $alternates = @(
                $RequiredEntryAlternates.$normalizedEntry |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) } |
                ForEach-Object { [string]$_ }
            )
        }

        $group = @($normalizedEntry) + $alternates
        $groups += ,$group
    }

    return $groups
}

function Test-RequiredEntries {
    param(
        [string]$Root,
        [object[]]$RequiredEntryGroups
    )

    $missing = Get-MissingRequiredEntries -Root $Root -RequiredEntryGroups $RequiredEntryGroups
    return $missing.Count -eq 0
}

function Download-FileWithProgress {
    param(
        [string]$Url,
        [string]$DestinationPath
    )

    Add-Type -AssemblyName System.Net.Http
    $handler = [System.Net.Http.HttpClientHandler]::new()
    $client = [System.Net.Http.HttpClient]::new($handler)

    try {
        $response = $client.GetAsync($Url, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
        $response.EnsureSuccessStatusCode()

        $contentLength = $response.Content.Headers.ContentLength
        $responseStream = $response.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
        $fileStream = [System.IO.File]::Open($DestinationPath, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)

        try {
            $buffer = New-Object byte[] (1024 * 1024)
            $totalRead = 0L
            $lastPrint = Get-Date

            while (($read = $responseStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                $fileStream.Write($buffer, 0, $read)
                $totalRead += $read

                $now = Get-Date
                if (($now - $lastPrint).TotalMilliseconds -ge 250) {
                    if ($contentLength -and $contentLength -gt 0) {
                        $percent = [math]::Floor(($totalRead * 100.0) / $contentLength)
                        $downloadedMB = [math]::Round($totalRead / 1MB, 2)
                        $totalMB = [math]::Round($contentLength / 1MB, 2)
                        $status = "$downloadedMB MB / $totalMB MB"
                        Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status 'Downloading content package' -PercentComplete $percent
                        Write-Host ("Downloading content package: {0}% ({1})" -f $percent, $status)
                    }
                    else {
                        $downloadedMB = [math]::Round($totalRead / 1MB, 2)
                        Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status "Downloading content package ($downloadedMB MB)" -PercentComplete 0
                        Write-Host "Downloading content package: $downloadedMB MB"
                    }
                    $lastPrint = $now
                }
            }

            if ($contentLength -and $contentLength -gt 0) {
                Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status 'Downloading content package' -PercentComplete 100
            }

            Write-InstallLog "Download complete. Bytes downloaded: $totalRead"
        }
        finally {
            $fileStream.Dispose()
            $responseStream.Dispose()
        }
    }
    finally {
        $client.Dispose()
        $handler.Dispose()
    }
}

function Copy-NormalizedContent {
    param(
        [string]$SourceRoot,
        [string]$DestinationRoot,
        [object[]]$RequiredEntryGroups
    )

    if (Test-Path -LiteralPath $DestinationRoot) {
        Remove-Item -LiteralPath $DestinationRoot -Recurse -Force
    }

    New-Item -ItemType Directory -Path $DestinationRoot -Force | Out-Null
    Copy-Item -Path (Join-Path $SourceRoot '*') -Destination $DestinationRoot -Recurse -Force

    $missing = Get-MissingRequiredEntries -Root $DestinationRoot -RequiredEntryGroups $RequiredEntryGroups
    if ($missing.Count -gt 0) {
        throw "Required content entry groups missing after copy to '$DestinationRoot': $($missing -join ', ')"
    }
}

function Write-RuntimeConfig {
    param(
        [string]$RuntimeConfigPath,
        [string]$EffectiveContentRoot
    )

    $enginePath = Join-Path -Path $EffectiveContentRoot -ChildPath 'stockfish\stockfish-windows-x86-64-avx2.exe'
    if (-not (Test-Path -LiteralPath $enginePath)) {
        $fallbackEnginePath = Join-Path -Path $EffectiveContentRoot -ChildPath 'stockfish\stockfish-windows-x86-64.exe'
        if (Test-Path -LiteralPath $fallbackEnginePath) {
            $enginePath = $fallbackEnginePath
        }
    }

    $runtimeConfig = [ordered]@{
        predecessor_master_db_path = (Join-Path $EffectiveContentRoot 'canonical_predecessor_master.sqlite.zst')
        opening_book_path = (Join-Path $EffectiveContentRoot 'opening_book.bin')
        engine_executable_path = $enginePath
        strict_assets = $false
        opponent_fallback_mode = 'current_bundle_only'
    }

    Write-JsonFileNoBom -Path $RuntimeConfigPath -Payload $runtimeConfig
    Write-InstallLog "Runtime configuration written: $RuntimeConfigPath"
}

function Write-InstalledManifest {
    param(
        [string]$InstalledManifestPath,
        [pscustomobject]$Manifest,
        [string]$Source,
        [string]$WrapperHandling,
        [string]$ArchiveChecksum
    )

    $installed = [ordered]@{
        manifest_version = $Manifest.manifest_version
        content_version = $Manifest.content_version
        archive_filename = $Manifest.archive_filename
        archive_sha256 = $Manifest.archive_sha256
        source_url = $Manifest.download_url
        install_source = $Source
        installed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        wrapper_handling_result = $WrapperHandling
    }

    if (-not [string]::IsNullOrWhiteSpace($ArchiveChecksum)) {
        $installed['archive_sha256_actual'] = $ArchiveChecksum
    }

    Write-JsonFileNoBom -Path $InstalledManifestPath -Payload $installed
    Write-InstallLog "Installed content manifest written: $InstalledManifestPath"
}

New-Item -ItemType Directory -Path $AppStateRoot -Force | Out-Null
New-Item -ItemType Directory -Path $ContentRoot -Force | Out-Null
New-Item -ItemType Directory -Path ([System.IO.Path]::GetDirectoryName($LogPath)) -Force | Out-Null

Write-InstallLog 'Installer bootstrap started.'
Write-InstallLog "ManifestPath=$ManifestPath"
Write-InstallLog "WINDOWS_USER=$([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)"
try {
    $principal = [System.Security.Principal.WindowsPrincipal]::new([System.Security.Principal.WindowsIdentity]::GetCurrent())
    Write-InstallLog "IS_ADMIN=$($principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator))"
}
catch {
    Write-InstallLog "IS_ADMIN=unknown reason=$($_.Exception.Message)"
}
Write-InstallLog "LOCALAPPDATA=$env:LOCALAPPDATA"
Write-InstallLog "USERPROFILE=$env:USERPROFILE"
Write-InstallLog "AppStateRoot=$AppStateRoot"
Write-InstallLog "ContentRoot=$ContentRoot"
Write-InstallLog "MutableAppRoot=$(Join-Path -Path $AppStateRoot -ChildPath 'App')"
Write-InstallLog "LocalArchivePath=$LocalArchivePath"

if (-not (Test-Path -LiteralPath $ManifestPath)) {
    Write-InstallLog "ERROR: Manifest file not found: $ManifestPath"
    throw "Manifest file not found: $ManifestPath"
}

$downloadTarget = $null
$extractStagingRoot = $null

try {
    Set-Phase -Phase 'Loading content manifest' -Percent 5
    $manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json

    $downloadUrl = [string]$manifest.download_url
    $archiveFileName = [string]$manifest.archive_filename
    $expectedSha256 = [string]$manifest.archive_sha256
    $requiredEntries = @($manifest.required_entries)
    $requiredEntryAlternates = $manifest.required_entry_alternates
    $wrapperFolderName = [string]$manifest.wrapper_folder_name
    $installedManifestFileName = [string]$manifest.installed_manifest_filename

    if ([string]::IsNullOrWhiteSpace($downloadUrl) -and [string]::IsNullOrWhiteSpace($LocalArchivePath)) {
        throw 'Manifest does not define a non-empty download_url and no -LocalArchivePath was supplied.'
    }
    if ([string]::IsNullOrWhiteSpace($archiveFileName)) {
        throw 'Manifest does not define archive_filename.'
    }
    if (-not $requiredEntries -or $requiredEntries.Count -eq 0) {
        throw 'Manifest required_entries is empty.'
    }
    $requiredEntryGroups = @(Get-RequiredEntryGroups -RequiredEntries $requiredEntries -RequiredEntryAlternates $requiredEntryAlternates)
    if (-not $requiredEntryGroups -or $requiredEntryGroups.Count -eq 0) {
        throw 'Manifest required entry groups are empty.'
    }
    if ([string]::IsNullOrWhiteSpace($installedManifestFileName)) {
        $installedManifestFileName = 'installed_content_manifest.json'
    }

    $installedManifestPath = Join-Path -Path $AppStateRoot -ChildPath $installedManifestFileName
    $runtimeConfigPath = Join-Path -Path $AppStateRoot -ChildPath 'runtime.consumer.json'

    Set-Phase -Phase 'Checking existing content' -Percent 10
    Write-InstallLog 'Checking existing installed content state.'

    $canReuseCurrentRoot = Test-RequiredEntries -Root $ContentRoot -RequiredEntryGroups $requiredEntryGroups
    $wrapperPath = $null
    if (-not [string]::IsNullOrWhiteSpace($wrapperFolderName)) {
        $wrapperPath = Join-Path -Path $ContentRoot -ChildPath $wrapperFolderName
    }
    $canMigrateWrapper = $false
    if ($wrapperPath -and (Test-Path -LiteralPath $wrapperPath)) {
        $canMigrateWrapper = Test-RequiredEntries -Root $wrapperPath -RequiredEntryGroups $requiredEntryGroups
    }

    $installedManifestMatches = $false
    $installedManifestPresent = $false
    if (Test-Path -LiteralPath $installedManifestPath) {
        $installedManifestPresent = $true
        try {
            Write-InstallLog "JSON read path (installed manifest): $installedManifestPath"
            $installedManifest = Get-Content -LiteralPath $installedManifestPath -Raw | ConvertFrom-Json
            $installedManifestMatches = [string]$installedManifest.content_version -eq [string]$manifest.content_version -and [string]$installedManifest.archive_filename -eq [string]$manifest.archive_filename
        }
        catch {
            Write-InstallLog "Installed manifest could not be parsed and will be ignored: $($_.Exception.Message)"
        }
    }
    else {
        Write-InstallLog "Installed manifest is missing: $installedManifestPath"
    }

    Write-InstallLog "Reuse probe result: flat_root_detected=$canReuseCurrentRoot wrapper_detected=$canMigrateWrapper installed_manifest_present=$installedManifestPresent installed_manifest_matches=$installedManifestMatches"

    if ($canReuseCurrentRoot) {
        Set-Phase -Phase 'Reusing installed content' -Percent 45
        if ($installedManifestMatches) {
            Write-InstallLog 'Reusing existing content root because required entries and installed manifest both match.'
        }
        else {
            Write-InstallLog 'Reusing existing content root based on direct required-entry validation; installed manifest is absent, stale, or mismatched.'
        }
        Write-RuntimeConfig -RuntimeConfigPath $runtimeConfigPath -EffectiveContentRoot $ContentRoot
        Write-InstalledManifest -InstalledManifestPath $installedManifestPath -Manifest $manifest -Source 'existing-content' -WrapperHandling 'already-flat' -ArchiveChecksum ''
        Set-Phase -Phase 'Finalizing install' -Percent 100
        Write-InstallLog 'Installation bootstrap completed successfully via local content reuse.'
        Write-Host 'Opening Trainer content install complete (reused existing content).'
        return
    }

    if ($canMigrateWrapper) {
        Set-Phase -Phase 'Reusing installed content' -Percent 35
        Write-InstallLog "Detected wrapper-folder content candidate at $wrapperPath"

        Set-Phase -Phase 'Migrating wrapper-folder content' -Percent 55
        Write-Host 'Migrating wrapper-folder content into canonical install root...'
        $migrationRoot = Join-Path -Path $env:TEMP -ChildPath "OpeningTrainerMigrate_$([guid]::NewGuid().ToString('N'))"
        New-Item -ItemType Directory -Path $migrationRoot -Force | Out-Null
        try {
            Copy-NormalizedContent -SourceRoot $wrapperPath -DestinationRoot $migrationRoot -RequiredEntryGroups $requiredEntryGroups
            Copy-NormalizedContent -SourceRoot $migrationRoot -DestinationRoot $ContentRoot -RequiredEntryGroups $requiredEntryGroups
        }
        finally {
            if (Test-Path -LiteralPath $migrationRoot) {
                Remove-Item -LiteralPath $migrationRoot -Recurse -Force
            }
        }

        Write-RuntimeConfig -RuntimeConfigPath $runtimeConfigPath -EffectiveContentRoot $ContentRoot
        Write-InstalledManifest -InstalledManifestPath $installedManifestPath -Manifest $manifest -Source 'existing-wrapper-content' -WrapperHandling 'flattened-wrapper-folder' -ArchiveChecksum ''
        Set-Phase -Phase 'Finalizing install' -Percent 100
        Write-InstallLog 'Installation bootstrap completed successfully via wrapper-folder migration.'
        Write-Host 'Opening Trainer content install complete (migrated existing wrapper-folder content).'
        return
    }

    if (Test-Path -LiteralPath $ContentRoot) {
        $missing = Get-MissingRequiredEntries -Root $ContentRoot -RequiredEntryGroups $requiredEntryGroups
        if ($missing.Count -gt 0 -and $missing.Count -lt $requiredEntries.Count) {
            Write-InstallLog "Existing content was detected but incomplete. Missing entries: $($missing -join ', ')"
            Write-Host "Existing content detected but incomplete; missing: $($missing -join ', ')"
        }
    }
    Write-InstallLog 'Reuse not accepted; proceeding to archive acquisition path.'

    if ($LocalArchivePath) {
        if (-not (Test-Path -LiteralPath $LocalArchivePath)) {
            throw "Local archive override path not found: $LocalArchivePath"
        }
        Set-Phase -Phase 'Using local archive override' -Percent 20
        $downloadTarget = (Resolve-Path -LiteralPath $LocalArchivePath).Path
        Write-InstallLog "Using local archive override: $downloadTarget"
    }
    else {
        $downloadTarget = Join-Path -Path $env:TEMP -ChildPath $archiveFileName
        if (Test-Path -LiteralPath $downloadTarget) {
            Remove-Item -LiteralPath $downloadTarget -Force
        }

        Set-Phase -Phase 'Downloading content package' -Percent 20
        Write-InstallLog 'Download begin.'
        Download-FileWithProgress -Url $downloadUrl -DestinationPath $downloadTarget
        Write-InstallLog 'Download end.'

        if (-not (Test-Path -LiteralPath $downloadTarget)) {
            throw "Content download failed: archive was not created at $downloadTarget"
        }
    }

    Set-Phase -Phase 'Verifying archive' -Percent 65
    $actualHash = (Get-FileHash -LiteralPath $downloadTarget -Algorithm SHA256).Hash.ToLowerInvariant()
    Write-InstallLog "Computed archive hash: $actualHash"
    if (-not [string]::IsNullOrWhiteSpace($expectedSha256)) {
        $expectedHashNormalized = $expectedSha256.ToLowerInvariant()
        Write-InstallLog "Checksum expected=$expectedHashNormalized actual=$actualHash"
        if ($actualHash -ne $expectedHashNormalized) {
            throw "Content checksum mismatch. Expected $expectedHashNormalized but got $actualHash"
        }
    }
    else {
        Write-InstallLog 'Checksum comparison skipped because archive_sha256 is empty in manifest.'
    }

    Set-Phase -Phase 'Extracting content' -Percent 75
    $extractStagingRoot = Join-Path -Path $env:TEMP -ChildPath "OpeningTrainerExtract_$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Directory -Path $extractStagingRoot -Force | Out-Null
    Write-InstallLog "Extraction begin: $downloadTarget -> $extractStagingRoot"
    Expand-Archive -LiteralPath $downloadTarget -DestinationPath $extractStagingRoot -Force

    $sourceRoot = $extractStagingRoot
    if (-not (Test-RequiredEntries -Root $sourceRoot -RequiredEntryGroups $requiredEntryGroups)) {
        if ($wrapperPath -and (Test-Path -LiteralPath (Join-Path -Path $extractStagingRoot -ChildPath $wrapperFolderName))) {
            $sourceRoot = Join-Path -Path $extractStagingRoot -ChildPath $wrapperFolderName
            Write-InstallLog "Detected wrapper folder inside archive: $sourceRoot"
        }
        else {
            $childDirectories = @(Get-ChildItem -LiteralPath $extractStagingRoot -Directory -Force)
            if ($childDirectories.Count -eq 1 -and (Test-RequiredEntries -Root $childDirectories[0].FullName -RequiredEntryGroups $requiredEntryGroups)) {
                $sourceRoot = $childDirectories[0].FullName
                Write-InstallLog "Detected single wrapper content directory in archive: $sourceRoot"
            }
        }
    }

    Copy-NormalizedContent -SourceRoot $sourceRoot -DestinationRoot $ContentRoot -RequiredEntryGroups $requiredEntryGroups
    Write-InstallLog 'Extraction end.'

    Set-Phase -Phase 'Writing runtime configuration' -Percent 90
    Write-RuntimeConfig -RuntimeConfigPath $runtimeConfigPath -EffectiveContentRoot $ContentRoot

    Set-Phase -Phase 'Finalizing install' -Percent 100
    Write-InstalledManifest -InstalledManifestPath $installedManifestPath -Manifest $manifest -Source ($(if ($LocalArchivePath) { 'local-archive' } else { 'download' })) -WrapperHandling 'normalized-from-archive' -ArchiveChecksum $actualHash

    Write-InstallLog 'Installation bootstrap completed successfully.'
    Write-Host 'Opening Trainer content install complete.'
}
catch {
    $errorMessage = $_.Exception.Message
    Write-InstallLog "ERROR: $errorMessage"
    Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status 'Failed' -PercentComplete 100
    Write-Host "Opening Trainer content install failed: $errorMessage"
    throw
}
finally {
    Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Completed

    if ($downloadTarget -and -not $LocalArchivePath -and (Test-Path -LiteralPath $downloadTarget)) {
        Remove-Item -LiteralPath $downloadTarget -Force
    }
    if ($extractStagingRoot -and (Test-Path -LiteralPath $extractStagingRoot)) {
        Remove-Item -LiteralPath $extractStagingRoot -Recurse -Force
    }
}
