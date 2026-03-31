from __future__ import annotations

import json
import subprocess
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from .install_layout import (
    read_installed_app_manifest,
)

DEFAULT_UPDATE_CHANNEL = "dev"
DEFAULT_UPDATE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/daharen/Opening_Trainer/main/installer/app_update_manifest.json"
)
UPDATER_CONFIG_FILENAME = "updater_config.json"


@dataclass(frozen=True)
class AppUpdateManifest:
    manifest_version: int
    channel: str
    app_version: str
    build_id: str
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
            build_id=str(payload.get("build_id") or ""),
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


def updater_config_path(app_state_root: Path) -> Path:
    return app_state_root / "updater" / UPDATER_CONFIG_FILENAME


def load_updater_config(app_state_root: Path) -> dict:
    path = updater_config_path(app_state_root)
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    return {
        "config_version": 1,
        "channel": DEFAULT_UPDATE_CHANNEL,
        "manifest_url": DEFAULT_UPDATE_MANIFEST_URL,
    }


def resolve_manifest_path_or_url(manifest_path_or_url: str | None, *, app_state_root: Path) -> str:
    if manifest_path_or_url:
        return manifest_path_or_url
    return str(load_updater_config(app_state_root).get("manifest_url") or DEFAULT_UPDATE_MANIFEST_URL)


def _has_newer_build(installed: dict, manifest: AppUpdateManifest) -> bool:
    installed_channel = str(installed.get("channel") or "").strip().lower()
    if installed_channel and installed_channel != manifest.channel.strip().lower():
        return False
    installed_build_id = str(installed.get("build_id") or "").strip()
    installed_payload_sha = str(installed.get("payload_sha256") or "").strip().lower()
    if manifest.build_id and installed_build_id and installed_build_id != manifest.build_id:
        return True
    if manifest.payload_sha256 and installed_payload_sha and installed_payload_sha != manifest.payload_sha256.lower():
        return True
    return str(installed.get("app_version")) != manifest.app_version


def check_for_update(manifest_path_or_url: str, *, app_state_root: Path) -> tuple[bool, AppUpdateManifest, dict | None]:
    manifest = load_update_manifest(manifest_path_or_url)
    installed = read_installed_app_manifest(app_state_root)
    if not installed:
        return True, manifest, None
    return _has_newer_build(installed, manifest), manifest, installed


def launch_updater_helper(
    manifest_path_or_url: str | None,
    *,
    app_state_root: Path,
    wait_for_pid: int,
    relaunch_exe_path: Path | None = None,
    relaunch_args: list[str] | None = None,
) -> subprocess.Popen:
    helper_script = app_state_root / "updater" / "apply_app_update.ps1"
    if not helper_script.exists():
        raise RuntimeError(f"Updater helper script is missing: {helper_script}")
    manifest_ref = resolve_manifest_path_or_url(manifest_path_or_url, app_state_root=app_state_root)
    relaunch_exe = str(relaunch_exe_path) if relaunch_exe_path else ""
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(helper_script),
        "-ManifestPathOrUrl",
        manifest_ref,
        "-AppStateRoot",
        str(app_state_root),
        "-WaitForPid",
        str(wait_for_pid),
        "-RelaunchExePath",
        relaunch_exe,
    ]
    if relaunch_args:
        cmd.extend(["-RelaunchArgs", json.dumps(relaunch_args)])
    return subprocess.Popen(cmd)
