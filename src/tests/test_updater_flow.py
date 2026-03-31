from __future__ import annotations

import json
from pathlib import Path

from opening_trainer.install_layout import choose_mutable_app_root, write_installed_app_manifest
from opening_trainer.updater import check_for_update, resolve_manifest_path_or_url


def test_apply_helper_avoids_reserved_pid_variable_name():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert "param([int]$Pid" not in script
    assert "Wait-ForProcessExit -Pid" not in script


def test_mutable_app_root_fallback_order(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Local"))
    monkeypatch.setenv("USERPROFILE", str(tmp_path / "User"))

    chosen, probes = choose_mutable_app_root()

    assert chosen == tmp_path / "Local" / "OpeningTrainer" / "App"
    assert probes[0].ok is True


def test_installed_manifest_persists_mutable_root(tmp_path):
    app_state = tmp_path / "OpeningTrainer"
    target = tmp_path / "Local" / "OpeningTrainer" / "App"

    path = write_installed_app_manifest(
        app_state_root=app_state,
        app_version="1.2.3",
        channel="dev",
        mutable_app_root=target,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="abc123",
        bootstrap_version="1.0.0",
        build_id="commit-1",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["mutable_app_root"] == str(target)
    assert payload["channel"] == "dev"
    assert payload["build_id"] == "commit-1"


def test_update_manifest_comparison(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "commit-abc",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    app_state = tmp_path / "OpeningTrainer"
    write_installed_app_manifest(
        app_state_root=app_state,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=tmp_path / "app",
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="old",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )

    has_update, manifest, installed = check_for_update(str(manifest_path), app_state_root=app_state)
    assert has_update is True
    assert manifest.app_version == "2.0.0"
    assert manifest.build_id == "commit-abc"
    assert installed is not None


def test_update_available_when_version_equal_but_build_id_differs(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "commit-new",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    app_state = tmp_path / "OpeningTrainer"
    write_installed_app_manifest(
        app_state_root=app_state,
        app_version="2.0.0",
        channel="dev",
        mutable_app_root=tmp_path / "app",
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="old-sha",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )

    has_update, _manifest, _installed = check_for_update(str(manifest_path), app_state_root=app_state)
    assert has_update is True


def test_resolve_manifest_path_uses_installed_updater_config(tmp_path):
    app_state = tmp_path / "OpeningTrainer"
    updater_root = app_state / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    expected = "https://example.invalid/repo-main/app_update_manifest.json"
    (updater_root / "updater_config.json").write_text(
        json.dumps({"channel": "dev", "manifest_url": expected}),
        encoding="utf-8",
    )

    assert resolve_manifest_path_or_url(None, app_state_root=app_state) == expected
