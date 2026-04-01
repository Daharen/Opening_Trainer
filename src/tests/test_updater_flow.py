from __future__ import annotations

import json
from pathlib import Path

from opening_trainer.install_layout import choose_mutable_app_root, write_installed_app_manifest
import pytest

from opening_trainer.updater import (
    UpdaterInstallStateError,
    check_for_update,
    launch_updater_helper,
    resolve_manifest_path_or_url,
)


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
    mutable_root = tmp_path / "app"
    bundled_helper = mutable_root / "updater" / "apply_app_update.ps1"
    bundled_helper.parent.mkdir(parents=True, exist_ok=True)
    bundled_helper.write_text("Write-Host helper", encoding="utf-8")
    write_installed_app_manifest(
        app_state_root=app_state,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
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
    mutable_root = tmp_path / "app"
    bundled_helper = mutable_root / "updater" / "apply_app_update.ps1"
    bundled_helper.parent.mkdir(parents=True, exist_ok=True)
    bundled_helper.write_text("Write-Host helper", encoding="utf-8")
    write_installed_app_manifest(
        app_state_root=app_state,
        app_version="2.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
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


def test_launch_updater_helper_sets_safe_cwd_outside_mutable_root(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=app_state_root / "App",
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="old",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    helper_path = updater_root / "apply_app_update.ps1"
    helper_path.write_text("Write-Host helper", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    popen_calls: list[dict] = []
    log_messages: list[str] = []

    class DummyProcess:
        pass

    def _fake_popen(cmd, cwd=None):
        popen_calls.append({"cmd": cmd, "cwd": cwd})
        return DummyProcess()

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", _fake_popen)
    monkeypatch.setattr("opening_trainer.updater.log_line", lambda message, tag="startup": log_messages.append(message))

    launch_updater_helper(
        str(manifest_path),
        app_state_root=app_state_root,
        wait_for_pid=1234,
        relaunch_exe_path=None,
        relaunch_args=None,
    )

    assert len(popen_calls) == 1
    assert popen_calls[0]["cwd"] == str(app_state_root / "updater")
    assert any("UPDATER_HELPER_LAUNCH" in msg for msg in log_messages)


def test_launch_updater_helper_self_heals_helper_from_mutable_root(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    bundled_helper = mutable_root / "updater" / "apply_app_update.ps1"
    bundled_helper.parent.mkdir(parents=True, exist_ok=True)
    bundled_helper.write_text("Write-Host bundled-helper", encoding="utf-8")
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    popen_calls: list[dict] = []

    class DummyProcess:
        pass

    def _fake_popen(cmd, cwd=None):
        popen_calls.append({"cmd": cmd, "cwd": cwd})
        return DummyProcess()

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", _fake_popen)

    launch_updater_helper(str(manifest_path), app_state_root=app_state_root, wait_for_pid=1234)

    assert (app_state_root / "updater" / "apply_app_update.ps1").exists()
    assert len(popen_calls) == 1


def test_launch_updater_helper_self_heals_helper_from_bootstrap_installer(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    mutable_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    bootstrap_root = tmp_path / "Program Files" / "Opening Trainer" / "installer"
    bootstrap_root.mkdir(parents=True, exist_ok=True)
    (bootstrap_root / "apply_app_update.ps1").write_text("Write-Host installer-helper", encoding="utf-8")
    monkeypatch.setattr("opening_trainer.updater._bootstrap_root_candidates", lambda app_state_root: [bootstrap_root])

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", lambda cmd, cwd=None: object())
    launch_updater_helper(str(manifest_path), app_state_root=app_state_root, wait_for_pid=1234)

    assert (app_state_root / "updater" / "apply_app_update.ps1").read_text(encoding="utf-8") == "Write-Host installer-helper"


def test_check_for_update_raises_when_manifest_missing_and_not_recoverable(tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(UpdaterInstallStateError):
        check_for_update(str(manifest_path), app_state_root=app_state_root)


def test_apply_helper_logs_cwd_relocation_and_swap_retries():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert "HELPER_CWD_BEFORE_RELOCATE" in script
    assert "HELPER_CWD_AFTER_RELOCATE" in script
    assert "SWAP_TARGETS mutable_root=" in script
    assert "SWAP_MOVE_ATTEMPT attempt=" in script
    assert "SWAP_MOVE_ATTEMPT_FAILED attempt=" in script
