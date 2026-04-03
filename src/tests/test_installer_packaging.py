from __future__ import annotations

import json
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_consumer_content_manifest_has_required_fields() -> None:
    manifest_path = _repo_root() / "installer" / "consumer_content_manifest.json"

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["manifest_version"] >= 1
    assert isinstance(payload["content_version"], str) and payload["content_version"]
    assert isinstance(payload["download_url"], str) and payload["download_url"].startswith("https://")
    assert "s3" in payload["download_url"].lower()
    assert isinstance(payload["archive_filename"], str) and payload["archive_filename"]
    assert isinstance(payload["manifest_url"], str) and payload["manifest_url"].startswith("https://")
    assert "s3" in payload["manifest_url"].lower()
    assert "archive_sha256" in payload
    assert isinstance(payload["update_channel"], str) and payload["update_channel"]
    assert isinstance(payload["install_root_mode"], str) and payload["install_root_mode"]
    assert isinstance(payload["wrapper_folder_name"], str) and payload["wrapper_folder_name"]
    assert isinstance(payload["installed_manifest_filename"], str) and payload["installed_manifest_filename"]
    assert isinstance(payload["required_entries"], list) and payload["required_entries"]


def test_inno_script_anchors_consumer_roots_and_uninstall() -> None:
    iss_path = _repo_root() / "installer" / "opening_trainer_installer.iss"
    script = iss_path.read_text(encoding="utf-8")

    assert "OutputBaseFilename=OpeningTrainerSetup" in script
    assert "OutputDir=dist" in script
    assert 'Source: "..\\dist\\consumer\\*"; DestDir: "{app}\\bootstrap_payload"' in script
    assert 'Source: "..\\dist\\consumer_app_payload\\OpeningTrainer-app.zip"' in script
    assert 'Source: "consumer_content_manifest.json"' in script
    assert 'Source: "app_update_manifest.json"' in script
    assert 'Source: "scripts\\install_consumer_content.ps1"' in script
    assert 'Source: "scripts\\install_consumer_app.ps1"' in script
    assert 'Source: "scripts\\apply_app_update.ps1"' in script
    assert 'Source: "scripts\\invoke_apply_app_update.ps1"' in script
    assert "runhidden" not in script
    assert "SetupLogging=yes" in script
    assert "{localappdata}\\OpeningTrainer" in script
    assert "{localappdata}\\OpeningTrainerContent" in script
    assert 'Filename: "{localappdata}\\OpeningTrainer\\App\\{#MyAppExeName}"' in script
    assert 'WorkingDir: "{localappdata}\\OpeningTrainer\\App"' in script
    assert "install_consumer_app.ps1" in script
    assert "install_consumer_app.invoke.log" in script
    assert "install_consumer_content.invoke.log" in script
    assert "UpdaterHelperScriptPath" in script
    assert "app_state_updater_wrapper" in script
    assert "mutable_updater_wrapper" in script
    assert "DefaultManifestUrl" in script
    assert "{localappdata}\\OpeningTrainer\\App" in script
    assert "CurUninstallStepChanged" in script
    assert "Remove downloaded opening content" in script


def test_content_bootstrap_writes_consumer_runtime_config_and_logging() -> None:
    bootstrap_path = _repo_root() / "installer" / "scripts" / "install_consumer_content.ps1"
    script = bootstrap_path.read_text(encoding="utf-8")

    assert "runtime.consumer.json" in script
    assert "installed_content_manifest.json" in script
    assert "corpus_bundle_dir" not in script
    assert "predecessor_master_db_path" in script
    assert "opening_book_path" in script
    assert "engine_executable_path" in script
    assert "strict_assets = $false" in script
    assert "install_consumer_content.log" in script
    assert "Checking existing content" in script
    assert "Reusing installed content" in script
    assert "Migrating wrapper-folder content" in script
    assert "Downloading content package" in script
    assert "Verifying archive" in script
    assert "Extracting content" in script
    assert "Writing runtime configuration" in script
    assert "Finalizing install" in script
    assert "Download-FileWithProgress" in script
    assert "LocalArchivePath" in script
    assert "wrapper_folder_name" in script
    assert "required_entries" in script
    assert "Write-JsonFileNoBom" in script
    assert "WriteAllText" in script
    assert "utf8-no-bom" in script
    assert "JSON write (utf8-no-bom)" in script
    assert "installed_manifest_present=" in script
    assert "installed_manifest_matches=" in script
    assert "direct required-entry validation" in script
    assert "Reuse not accepted; proceeding to archive acquisition path." in script


def test_packaging_build_scripts_exist() -> None:
    repo_root = _repo_root()
    payload_script = repo_root / "installer" / "scripts" / "build_consumer_payload.ps1"
    app_payload_script = repo_root / "installer" / "scripts" / "build_consumer_app_payload.ps1"
    installer_script = repo_root / "installer" / "scripts" / "build_consumer_installer.ps1"
    validation_script = repo_root / "installer" / "scripts" / "validate_install_consumer_app.ps1"

    assert payload_script.exists()
    assert app_payload_script.exists()
    assert installer_script.exists()
    assert validation_script.exists()

    payload_text = payload_script.read_text(encoding="utf-8")
    installer_text = installer_script.read_text(encoding="utf-8")
    app_payload_text = app_payload_script.read_text(encoding="utf-8")

    assert "Join-Path $distRoot 'consumer'" in payload_text
    assert "PyInstaller" in payload_text
    assert "opening_trainer_consumer.spec" in payload_text
    assert ".venv\\Scripts\\python.exe" in payload_text
    assert "import chess" in payload_text
    assert "--show-runtime --runtime-mode dev" in payload_text
    assert "OPENING_TRAINER_ASSUME_INSTALLED" in payload_text
    assert "runtime mode inference smoke test" in payload_text
    assert "--probe-gui-bootstrap" in payload_text
    assert "--probe-real-gui-startup" in payload_text
    assert "DebugConsole" in payload_text
    assert "OpeningTrainer-app.zip" in app_payload_text
    assert "payload_identity.json" in app_payload_text
    assert "marker_schema_version" in app_payload_text
    assert "Compress-Archive" in app_payload_text
    assert "build_consumer_payload.ps1" in installer_text
    assert "build_consumer_app_payload.ps1" in installer_text
    assert "ISCC.exe" in installer_text
    assert "App payload zip is missing" in installer_text
    assert "SkipAppProvisioningValidation" in installer_text
    assert "validate_install_consumer_app.ps1" in installer_text
    assert "app_update_manifest.json" in installer_text
    assert "Assert-PowerShellScriptParses" in installer_text
    assert "error_id=$($_.ErrorId)" in installer_text
    assert "invoke_apply_app_update.ps1" in installer_text
    assert "staging" in app_payload_text
    assert "Copy-WithRetry" in app_payload_text


def test_app_update_manifest_schema() -> None:
    manifest_path = _repo_root() / "installer" / "app_update_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["manifest_version"] >= 1
    assert payload["channel"] in {"dev", "canary", "release"}
    assert isinstance(payload["app_version"], str) and payload["app_version"]
    assert isinstance(payload["build_id"], str) and payload["build_id"]
    assert isinstance(payload["payload_filename"], str) and payload["payload_filename"].endswith(".zip")
    assert isinstance(payload["payload_url"], str) and payload["payload_url"].startswith("https://")
    assert isinstance(payload["payload_sha256"], str) and payload["payload_sha256"]
    assert isinstance(payload["published_at_utc"], str) and payload["published_at_utc"]


def test_install_consumer_app_has_probe_and_fallback_policy() -> None:
    script = (_repo_root() / "installer" / "scripts" / "install_consumer_app.ps1").read_text(encoding="utf-8")

    assert "Test-AppRootWritable" in script
    assert "Move-Item" in script
    assert "Writable probe selected explicit override root" in script
    assert "Default mutable app root failed writable probe" in script
    assert "installed_app_manifest.json" in script
    assert "updater_config.json" in script
    assert "install_consumer_app.log" in script
    assert "INSTALL_CONSUMER_APP_START" in script
    assert "INSTALL_CONSUMER_APP_SUCCESS" in script
    assert "INSTALL_CONSUMER_APP_FAILURE" in script
    assert "source-precheck" in script
    assert "post-copy" in script
    assert "post-provision" in script
    assert "payload_identity.json" in script
    assert "build_id = $payloadIdentityBuildId" in script
    assert "UpdaterHelperScriptPath" in script
    assert "helperSourceCandidates" in script
    assert "Provisioned updater helper to app state" in script
    assert "HELPER_SOURCE_CANDIDATES" in script


def test_updater_helper_and_publish_script_exist() -> None:
    repo_root = _repo_root()
    helper_script = repo_root / "installer" / "scripts" / "apply_app_update.ps1"
    wrapper_script = repo_root / "installer" / "scripts" / "invoke_apply_app_update.ps1"
    publish_script = repo_root / "installer" / "scripts" / "publish_dev_update.ps1"

    assert helper_script.exists()
    assert wrapper_script.exists()
    assert publish_script.exists()

    helper_text = helper_script.read_text(encoding="utf-8")
    wrapper_text = wrapper_script.read_text(encoding="utf-8")
    publish_text = publish_script.read_text(encoding="utf-8")
    assert "Wait-ForProcessExit" in helper_text
    assert "payload_sha256" in helper_text
    assert "mutable_app_root" in helper_text
    assert "build_id" in helper_text
    assert "WRAPPER_ENTERED" in wrapper_text
    assert "WRAPPER_REAL_HELPER_EXCEPTION" in wrapper_text
    assert "WRAPPER_RELAUNCH_ARGS_PARSE_FAILED" in wrapper_text
    assert "build_consumer_app_payload.ps1" in publish_text
    assert "Get-FileHash" in publish_text
    assert "build_id" in publish_text


def test_updater_lane_files_do_not_contain_merge_conflict_markers() -> None:
    repo_root = _repo_root()
    repair_targets = [
        repo_root / "src" / "opening_trainer" / "updater.py",
        repo_root / "installer" / "scripts" / "build_consumer_installer.ps1",
        repo_root / "installer" / "scripts" / "invoke_apply_app_update.ps1",
        repo_root / "installer" / "scripts" / "apply_app_update.ps1",
    ]
    merge_markers = ("<<<<<<<", "=======", ">>>>>>>")

    for path in repair_targets:
        text = path.read_text(encoding="utf-8")
        assert not any(marker in text for marker in merge_markers), f"merge conflict marker found in {path}"


def test_content_bootstrap_reuse_does_not_require_installed_manifest_match() -> None:
    bootstrap_path = _repo_root() / "installer" / "scripts" / "install_consumer_content.ps1"
    script = bootstrap_path.read_text(encoding="utf-8")

    assert "if ($canReuseCurrentRoot) {" in script
    assert "if ($canReuseCurrentRoot -and $installedManifestMatches)" not in script
    assert "Installed manifest is missing" in script
