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
    assert isinstance(payload["archive_filename"], str) and payload["archive_filename"]


def test_inno_script_anchors_consumer_roots_and_uninstall() -> None:
    iss_path = _repo_root() / "installer" / "opening_trainer_installer.iss"
    script = iss_path.read_text(encoding="utf-8")

    assert "OutputBaseFilename=OpeningTrainerSetup" in script
    assert "{localappdata}\\OpeningTrainer" in script
    assert "{localappdata}\\OpeningTrainerContent" in script
    assert "--runtime-mode consumer" in script
    assert "CurUninstallStepChanged" in script
    assert "Remove downloaded opening content" in script


def test_content_bootstrap_writes_consumer_runtime_config() -> None:
    bootstrap_path = _repo_root() / "installer" / "scripts" / "install_consumer_content.ps1"
    script = bootstrap_path.read_text(encoding="utf-8")

    assert "runtime.consumer.json" in script
    assert "corpus_bundle_dir" in script
    assert "predecessor_master_db_path" in script
    assert "opening_book_path" in script
    assert "engine_executable_path" in script
