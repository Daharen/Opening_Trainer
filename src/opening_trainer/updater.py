from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

from .install_layout import (
    choose_mutable_app_root,
    probe_mutable_app_root,
    read_installed_app_manifest,
    write_installed_app_manifest,
)


@dataclass(frozen=True)
class AppUpdateManifest:
    manifest_version: int
    channel: str
    app_version: str
    payload_filename: str
    payload_url: str
    payload_sha256: str
    published_at_utc: str
    minimum_bootstrap_version: str | None = None
    notes: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict) -> "AppUpdateManifest":
        return cls(
            manifest_version=int(payload["manifest_version"]),
            channel=str(payload["channel"]),
            app_version=str(payload["app_version"]),
            payload_filename=str(payload["payload_filename"]),
            payload_url=str(payload["payload_url"]),
            payload_sha256=str(payload["payload_sha256"]),
            published_at_utc=str(payload["published_at_utc"]),
            minimum_bootstrap_version=payload.get("minimum_bootstrap_version"),
            notes=payload.get("notes") or payload.get("release_summary"),
        )


def load_update_manifest(path_or_url: str) -> AppUpdateManifest:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        with urllib.request.urlopen(path_or_url) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    else:
        payload = json.loads(Path(path_or_url).read_text(encoding="utf-8"))
    return AppUpdateManifest.from_mapping(payload)


def check_for_update(manifest_path_or_url: str, *, app_state_root: Path) -> tuple[bool, AppUpdateManifest, dict | None]:
    manifest = load_update_manifest(manifest_path_or_url)
    installed = read_installed_app_manifest(app_state_root)
    if not installed:
        return True, manifest, None
    return str(installed.get("app_version")) != manifest.app_version, manifest, installed


def apply_update(
    manifest_path_or_url: str,
    *,
    app_state_root: Path,
    relaunch: bool = False,
    relaunch_args: list[str] | None = None,
) -> Path:
    manifest = load_update_manifest(manifest_path_or_url)
    installed = read_installed_app_manifest(app_state_root)
    mutable_app_root = Path(installed["mutable_app_root"]) if installed and installed.get("mutable_app_root") else choose_mutable_app_root()[0]
    probe = probe_mutable_app_root(mutable_app_root)
    if not probe.ok:
        raise RuntimeError(f"Mutable app root is no longer writable: {probe.detail}")

    staging_root = Path(tempfile.mkdtemp(prefix="opening_trainer_updater_"))
    payload_zip = staging_root / manifest.payload_filename
    unpack_root = staging_root / "payload"
    unpack_root.mkdir(parents=True, exist_ok=True)
    try:
        urllib.request.urlretrieve(manifest.payload_url, payload_zip)
        digest = hashlib.sha256(payload_zip.read_bytes()).hexdigest().lower()
        if digest != manifest.payload_sha256.lower():
            raise RuntimeError("Downloaded payload hash mismatch.")
        with zipfile.ZipFile(payload_zip, "r") as archive:
            archive.extractall(unpack_root)

        swap_root = mutable_app_root.parent / f"{mutable_app_root.name}.next"
        if swap_root.exists():
            shutil.rmtree(swap_root)
        shutil.copytree(unpack_root, swap_root)
        if mutable_app_root.exists():
            backup_root = mutable_app_root.parent / f"{mutable_app_root.name}.prev"
            if backup_root.exists():
                shutil.rmtree(backup_root)
            mutable_app_root.replace(backup_root)
        swap_root.replace(mutable_app_root)

        write_installed_app_manifest(
            app_state_root=app_state_root,
            app_version=manifest.app_version,
            channel=manifest.channel,
            mutable_app_root=mutable_app_root,
            payload_filename=manifest.payload_filename,
            payload_sha256=manifest.payload_sha256,
            bootstrap_version=(installed or {}).get("bootstrap_version"),
        )
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)

    launched = mutable_app_root / "OpeningTrainer.exe"
    if relaunch and launched.exists():
        subprocess.Popen([str(launched), *(relaunch_args or [])])
    return launched
