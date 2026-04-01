from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from .install_layout import (
    read_installed_app_manifest,
    write_installed_app_manifest,
)
from .session_logging import log_line

DEFAULT_UPDATE_CHANNEL = "dev"
DEFAULT_UPDATE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/daharen/Opening_Trainer/main/installer/app_update_manifest.json"
)
UPDATER_CONFIG_FILENAME = "updater_config.json"


class UpdaterInstallStateError(RuntimeError):
    """Raised when updater prerequisites are missing and cannot be recovered."""


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


def _updater_runtime_root(app_state_root: Path) -> Path:
    return app_state_root / "updater"


def _copy_file(source: Path, destination: Path, *, prerequisite: str) -> bool:
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        log_line(
            f"UPDATER_RECOVERY result=ok prerequisite={prerequisite} source={source} destination={destination}",
            tag="startup",
        )
        return True
    except OSError as exc:
        log_line(
            f"UPDATER_RECOVERY result=failed prerequisite={prerequisite} source={source} destination={destination} reason={exc}",
            tag="error",
        )
        return False


def _bootstrap_root_candidates(*, app_state_root: Path) -> list[Path]:
    local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    return [
        Path("C:/Program Files/Opening Trainer/installer"),
        local_app_data / "Programs" / "Opening Trainer" / "installer",
        Path(sys.executable).resolve().parent / "installer",
    ]


def _candidate_helper_sources(*, app_state_root: Path, mutable_root: Path) -> list[Path]:
    candidates = [mutable_root / "updater" / "apply_app_update.ps1"]
    for bootstrap_root in _bootstrap_root_candidates(app_state_root=app_state_root):
        candidates.append(bootstrap_root / "apply_app_update.ps1")
    return candidates


def _recover_installed_manifest(app_state_root: Path) -> dict | None:
    mutable_root = app_state_root / "App"
    expected_exe = mutable_root / "OpeningTrainer.exe"
    if not expected_exe.exists():
        log_line(
            "UPDATER_SELF_HEAL result=skipped prerequisite=installed_manifest reason=mutable_root_missing_exe",
            tag="startup",
        )
        return None
    config = load_updater_config(app_state_root)
    manifest_path = write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="unknown",
        channel=str(config.get("channel") or DEFAULT_UPDATE_CHANNEL),
        mutable_app_root=mutable_root,
        payload_filename="",
        payload_sha256=None,
        bootstrap_version=None,
        build_id="recovered-missing-manifest",
    )
    log_line(f"UPDATER_SELF_HEAL result=ok prerequisite=installed_manifest path={manifest_path}", tag="startup")
    return read_installed_app_manifest(app_state_root)


def _audit_updater_prerequisites(*, app_state_root: Path, installed_manifest: dict | None) -> None:
    mutable_root = Path(str((installed_manifest or {}).get("mutable_app_root") or app_state_root / "App")).expanduser()
    helper_target = _updater_runtime_root(app_state_root) / "apply_app_update.ps1"
    helper_candidates = _candidate_helper_sources(app_state_root=app_state_root, mutable_root=mutable_root)
    config_path = updater_config_path(app_state_root)
    log_line(
        "UPDATER_PREREQ_AUDIT "
        f"app_state_root={app_state_root} "
        f"mutable_app_root={mutable_root} "
        f"executable={Path(sys.executable).resolve()} "
        f"installed_manifest_path={app_state_root / 'installed_app_manifest.json'} "
        f"installed_manifest_exists={(app_state_root / 'installed_app_manifest.json').exists()} "
        f"helper_target={helper_target} helper_target_exists={helper_target.exists()} "
        f"updater_config_path={config_path} updater_config_exists={config_path.exists()}",
        tag="startup",
    )
    for index, candidate in enumerate(helper_candidates, start=1):
        log_line(
            f"UPDATER_PREREQ_AUDIT helper_candidate_index={index} path={candidate} exists={candidate.exists()}",
            tag="startup",
        )


def _resolve_helper_script(*, app_state_root: Path, installed_manifest: dict) -> Path | None:
    helper_target = _updater_runtime_root(app_state_root) / "apply_app_update.ps1"
    mutable_root = Path(str(installed_manifest.get("mutable_app_root") or "")).expanduser()
    if not str(mutable_root).strip():
        mutable_root = app_state_root / "App"
    candidates = [helper_target, *_candidate_helper_sources(app_state_root=app_state_root, mutable_root=mutable_root)]
    for index, candidate in enumerate(candidates, start=1):
        exists = candidate.exists()
        log_line(
            f"UPDATER_RESOLVE prerequisite=helper candidate_index={index} path={candidate} exists={exists}",
            tag="startup",
        )
        if not exists:
            continue
        if candidate == helper_target:
            return helper_target
        if _copy_file(candidate, helper_target, prerequisite="helper"):
            return helper_target
    return None


def ensure_updater_prerequisites(app_state_root: Path) -> tuple[dict, Path]:
    updater_root = _updater_runtime_root(app_state_root)
    updater_root.mkdir(parents=True, exist_ok=True)

    installed_manifest = read_installed_app_manifest(app_state_root)
    _audit_updater_prerequisites(app_state_root=app_state_root, installed_manifest=installed_manifest)
    log_line(
        "UPDATER_PREREQ_STATE "
        f"installed_manifest_present={installed_manifest is not None} "
        f"helper_present={(updater_root / 'apply_app_update.ps1').exists()}",
        tag="startup",
    )
    if installed_manifest is None:
        installed_manifest = _recover_installed_manifest(app_state_root)
        _audit_updater_prerequisites(app_state_root=app_state_root, installed_manifest=installed_manifest)
    if installed_manifest is None:
        raise UpdaterInstallStateError(
            f"Installation is missing required updater metadata: {app_state_root / 'installed_app_manifest.json'}"
        )

    helper_script = _resolve_helper_script(app_state_root=app_state_root, installed_manifest=installed_manifest)
    if helper_script is None or not helper_script.exists():
        raise UpdaterInstallStateError(
            f"Installation is missing required updater helper components: {updater_root / 'apply_app_update.ps1'}"
        )

    updater_config = updater_config_path(app_state_root)
    if not updater_config.exists():
        payload = {
            "config_version": 1,
            "channel": str(installed_manifest.get("channel") or DEFAULT_UPDATE_CHANNEL),
            "manifest_url": DEFAULT_UPDATE_MANIFEST_URL,
        }
        updater_config.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log_line(f"UPDATER_SELF_HEAL result=ok prerequisite=updater_config path={updater_config}", tag="startup")
    log_line(
        "UPDATER_PREREQ_READY "
        f"installed_manifest={app_state_root / 'installed_app_manifest.json'} "
        f"helper_script={helper_script}",
        tag="startup",
    )
    return installed_manifest, helper_script


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
    installed, _helper_script = ensure_updater_prerequisites(app_state_root)
    return _has_newer_build(installed, manifest), manifest, installed


def launch_updater_helper(
    manifest_path_or_url: str | None,
    *,
    app_state_root: Path,
    wait_for_pid: int,
    relaunch_exe_path: Path | None = None,
    relaunch_args: list[str] | None = None,
) -> subprocess.Popen:
    _installed, helper_script = ensure_updater_prerequisites(app_state_root)
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
    safe_launch_cwd = app_state_root / "updater"
    fallback_launch_cwd = app_state_root
    try:
        safe_launch_cwd.mkdir(parents=True, exist_ok=True)
        helper_cwd = safe_launch_cwd
    except OSError:
        fallback_launch_cwd.mkdir(parents=True, exist_ok=True)
        helper_cwd = fallback_launch_cwd
    log_line(
        "UPDATER_HELPER_LAUNCH "
        f"helper_script={helper_script} "
        f"cwd={helper_cwd} "
        f"app_state_root={app_state_root} "
        f"localappdata={os.getenv('LOCALAPPDATA', '')}",
        tag="startup",
    )
    popen_kwargs: dict[str, object] = {"cwd": str(helper_cwd)}
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    return subprocess.Popen(cmd, **popen_kwargs)
