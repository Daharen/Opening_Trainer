param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestPath,
    [Parameter(Mandatory = $true)]
    [string]$AppStateRoot,
    [Parameter(Mandatory = $true)]
    [string]$ContentRoot,
    [Parameter(Mandatory = $false)]
    [string]$LogPath
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $LogPath = Join-Path -Path $AppStateRoot -ChildPath 'install.log'
}

function Write-InstallLog {
    param([string]$Message)
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff'
    $line = "[$timestamp] $Message"
    Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
}

function Set-Phase {
    param([string]$Phase)
    Write-Host "==> $Phase"
    Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status $Phase -PercentComplete 0
    Write-InstallLog "PHASE: $Phase"
}

function Test-RequiredEntries {
    param([string]$Root)

    $requiredRelativePaths = @(
        'canonical_predecessor_master.sqlite',
        'opening_book.bin',
        'opening_book_names.zip',
        'stockfish',
        'Timing Conditioned Corpus Bundles'
    )

    foreach ($relativePath in $requiredRelativePaths) {
        $requiredPath = Join-Path -Path $Root -ChildPath $relativePath
        if (-not (Test-Path -LiteralPath $requiredPath)) {
            return $false
        }
    }

    return $true
}

function Get-ExtractedRoot {
    param(
        [string]$ExtractRoot,
        [string]$FinalRoot
    )

    if (Test-RequiredEntries -Root $ExtractRoot) {
        return $ExtractRoot
    }

    $childDirectories = @(Get-ChildItem -LiteralPath $ExtractRoot -Directory -Force)
    if ($childDirectories.Count -eq 1) {
        $candidate = $childDirectories[0].FullName
        if (Test-RequiredEntries -Root $candidate) {
            Write-InstallLog "Detected single wrapper content directory; normalizing from $candidate"
            return $candidate
        }
    }

    throw "Extracted content is incomplete. Expected required content under '$FinalRoot' (or one nested wrapper folder), but one or more required paths were missing."
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

New-Item -ItemType Directory -Path $AppStateRoot -Force | Out-Null
New-Item -ItemType Directory -Path $ContentRoot -Force | Out-Null
New-Item -ItemType Directory -Path ([System.IO.Path]::GetDirectoryName($LogPath)) -Force | Out-Null

Write-InstallLog 'Installer bootstrap started.'
Write-InstallLog "ManifestPath=$ManifestPath"
Write-InstallLog "AppStateRoot=$AppStateRoot"
Write-InstallLog "ContentRoot=$ContentRoot"

if (-not (Test-Path -LiteralPath $ManifestPath)) {
    Write-InstallLog "ERROR: Manifest file not found: $ManifestPath"
    throw "Manifest file not found: $ManifestPath"
}

Set-Phase -Phase 'Loading content manifest'
$manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
$downloadUrl = [string]$manifest.download_url
$archiveFileName = [string]$manifest.archive_filename
$sha256 = [string]$manifest.sha256

if ([string]::IsNullOrWhiteSpace($downloadUrl)) {
    Write-InstallLog 'ERROR: Manifest does not define a non-empty download_url.'
    throw 'Manifest does not define a non-empty download_url.'
}

if ([string]::IsNullOrWhiteSpace($archiveFileName)) {
    $archiveFileName = 'opening_trainer_content_seed.zip'
}

Write-InstallLog "Resolved download URL: $downloadUrl"
Write-InstallLog "Resolved archive filename: $archiveFileName"

$downloadTarget = Join-Path -Path $env:TEMP -ChildPath $archiveFileName
$extractStagingRoot = Join-Path -Path $env:TEMP -ChildPath "OpeningTrainerExtract_$([guid]::NewGuid().ToString('N'))"

if (Test-Path -LiteralPath $downloadTarget) {
    Remove-Item -LiteralPath $downloadTarget -Force
}
if (Test-Path -LiteralPath $extractStagingRoot) {
    Remove-Item -LiteralPath $extractStagingRoot -Recurse -Force
}

try {
    Set-Phase -Phase 'Downloading content package'
    Write-InstallLog 'Download begin.'
    Download-FileWithProgress -Url $downloadUrl -DestinationPath $downloadTarget
    Write-InstallLog 'Download end.'

    if (-not (Test-Path -LiteralPath $downloadTarget)) {
        Write-InstallLog "ERROR: Content download failed; archive not created at $downloadTarget"
        throw "Content download failed: archive was not created at $downloadTarget"
    }

    Set-Phase -Phase 'Verifying package'
    if (-not [string]::IsNullOrWhiteSpace($sha256)) {
        $actualHash = (Get-FileHash -LiteralPath $downloadTarget -Algorithm SHA256).Hash.ToLowerInvariant()
        $expectedHash = $sha256.ToLowerInvariant()
        Write-InstallLog "Checksum expected=$expectedHash actual=$actualHash"
        if ($actualHash -ne $expectedHash) {
            Write-InstallLog 'ERROR: Content checksum mismatch.'
            throw "Content checksum mismatch. Expected $expectedHash but got $actualHash"
        }
    }
    else {
        Write-InstallLog 'Checksum verification skipped because manifest SHA-256 is empty.'
    }

    Set-Phase -Phase 'Extracting content'
    Write-InstallLog "Extraction begin: $downloadTarget -> $extractStagingRoot"
    New-Item -ItemType Directory -Path $extractStagingRoot -Force | Out-Null
    Expand-Archive -LiteralPath $downloadTarget -DestinationPath $extractStagingRoot -Force

    $normalizedRoot = Get-ExtractedRoot -ExtractRoot $extractStagingRoot -FinalRoot $ContentRoot

    Write-Host 'Extracting content: finalizing files...'
    Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status 'Extracting content' -PercentComplete 80

    if (Test-Path -LiteralPath $ContentRoot) {
        Remove-Item -LiteralPath $ContentRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Path $ContentRoot -Force | Out-Null
    Copy-Item -Path (Join-Path $normalizedRoot '*') -Destination $ContentRoot -Recurse -Force

    if (-not (Test-RequiredEntries -Root $ContentRoot)) {
        Write-InstallLog 'ERROR: Required extracted content missing after normalize/copy step.'
        throw "Extracted content is incomplete after copy to '$ContentRoot'. One or more required files/directories are missing."
    }

    Write-InstallLog 'Extraction end.'

    Set-Phase -Phase 'Writing runtime configuration'
    $runtimeConfigPath = Join-Path -Path $AppStateRoot -ChildPath 'runtime.consumer.json'
    $enginePath = Join-Path -Path $ContentRoot -ChildPath 'stockfish\stockfish-windows-x86-64-avx2.exe'

    if (-not (Test-Path -LiteralPath $enginePath)) {
        $fallbackEnginePath = Join-Path -Path $ContentRoot -ChildPath 'stockfish\stockfish-windows-x86-64.exe'
        if (Test-Path -LiteralPath $fallbackEnginePath) {
            $enginePath = $fallbackEnginePath
        }
    }

    $runtimeConfig = [ordered]@{
        corpus_bundle_dir = (Join-Path $ContentRoot 'Timing Conditioned Corpus Bundles')
        predecessor_master_db_path = (Join-Path $ContentRoot 'canonical_predecessor_master.sqlite')
        opening_book_path = (Join-Path $ContentRoot 'opening_book.bin')
        engine_executable_path = $enginePath
        strict_assets = $true
        opponent_fallback_mode = 'current_bundle_only'
    }

    $runtimeConfig | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $runtimeConfigPath -Encoding UTF8
    Write-InstallLog "Runtime configuration written: $runtimeConfigPath"

    Set-Phase -Phase 'Finalizing installation'
    Write-Progress -Id 1 -Activity 'Opening Trainer content install' -Status 'Finalizing installation' -PercentComplete 100
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

    if (Test-Path -LiteralPath $downloadTarget) {
        Remove-Item -LiteralPath $downloadTarget -Force
    }
    if (Test-Path -LiteralPath $extractStagingRoot) {
        Remove-Item -LiteralPath $extractStagingRoot -Recurse -Force
    }
}
