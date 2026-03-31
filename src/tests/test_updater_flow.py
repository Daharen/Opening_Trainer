from __future__ import annotations

import json
from pathlib import Path

from opening_trainer.install_layout import choose_mutable_app_root, write_installed_app_manifest
from opening_trainer.updater import check_for_update


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
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["mutable_app_root"] == str(target)
    assert payload["channel"] == "dev"


def test_update_manifest_comparison(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
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
    )

    has_update, manifest, installed = check_for_update(str(manifest_path), app_state_root=app_state)
    assert has_update is True
    assert manifest.app_version == "2.0.0"
    assert installed is not None
