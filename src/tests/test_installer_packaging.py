from __future__ import annotations

import json
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_consumer_content_manifest_has_required_fields() -> None:
    manifest_path = _repo_root() / "installer" / "consumer_content_manifest.json"

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["manifest_version"] == 1
    assert isinstance(payload["content_version"], str) and payload["content_version"]
    assert isinstance(payload["download_url"], str) and payload["download_url"].startswith("https://")
    assert "s3" in payload["download_url"].lower()
    assert isinstance(payload["archive_filename"], str) and payload["archive_filename"]


def test_inno_script_anchors_consumer_roots_and_uninstall() -> None:
    iss_path = _repo_root() / "installer" / "opening_trainer_installer.iss"
    script = iss_path.read_text(encoding="utf-8")

    assert "OutputBaseFilename=OpeningTrainerSetup" in script
    assert "OutputDir=dist" in script
    assert 'Source: "..\\dist\\consumer\\*"' in script
    assert 'Source: "consumer_content_manifest.json"' in script
    assert 'Source: "scripts\\install_consumer_content.ps1"' in script
    assert "runhidden" not in script
    assert "SetupLogging=yes" in script
    assert "{localappdata}\\OpeningTrainer" in script
    assert "{localappdata}\\OpeningTrainerContent" in script
    assert "--runtime-mode consumer" in script
    assert "CurUninstallStepChanged" in script
    assert "Remove downloaded opening content" in script


def test_content_bootstrap_writes_consumer_runtime_config_and_logging() -> None:
    bootstrap_path = _repo_root() / "installer" / "scripts" / "install_consumer_content.ps1"
    script = bootstrap_path.read_text(encoding="utf-8")

    assert "runtime.consumer.json" in script
    assert "corpus_bundle_dir" in script
    assert "predecessor_master_db_path" in script
    assert "opening_book_path" in script
    assert "engine_executable_path" in script
    assert "install.log" in script
    assert "Downloading content package" in script
    assert "Verifying package" in script
    assert "Extracting content" in script
    assert "Writing runtime configuration" in script
    assert "Finalizing installation" in script
    assert "Download-FileWithProgress" in script


def test_packaging_build_scripts_exist() -> None:
    repo_root = _repo_root()
    payload_script = repo_root / "installer" / "scripts" / "build_consumer_payload.ps1"
    installer_script = repo_root / "installer" / "scripts" / "build_consumer_installer.ps1"

    assert payload_script.exists()
    assert installer_script.exists()

    payload_text = payload_script.read_text(encoding="utf-8")
    installer_text = installer_script.read_text(encoding="utf-8")

    assert "Join-Path $distRoot 'consumer'" in payload_text
    assert "PyInstaller" in payload_text
    assert "opening_trainer_consumer.spec" in payload_text
    assert "build_consumer_payload.ps1" in installer_text
    assert "ISCC.exe" in installer_text
