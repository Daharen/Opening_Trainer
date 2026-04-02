from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from .install_layout import read_installed_app_manifest
from .session_logging import log_line

DEFAULT_UPDATE_CHANNEL = "dev"
DEFAULT_UPDATE_MANIFEST_URL = (
    "https://raw.githubusercontent.com/daharen/Opening_Trainer/main/installer/app_update_manifest.json"
)
UPDATER_CONFIG_FILENAME = "updater_config.json"
PAYLOAD_IDENTITY_FILENAME = "payload_identity.json"
INSTALL_DIAGNOSTIC_MARKER = "lane_installer_observability_v1"


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


def payload_identity_path(*, mutable_root: Path) -> Path:
    return mutable_root / PAYLOAD_IDENTITY_FILENAME


def read_payload_identity_marker(*, mutable_root: Path) -> dict | None:
    marker_path = payload_identity_path(mutable_root=mutable_root)
    if not marker_path.exists():
        return None
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def log_install_runtime_diagnostics(*, app_state_root: Path, phase: str) -> None:
    manifest_path = app_state_root / "installed_app_manifest.json"
    installed_manifest = read_installed_app_manifest(app_state_root)
    mutable_root = Path(str((installed_manifest or {}).get("mutable_app_root") or app_state_root / "App")).expanduser()
    app_state_helper = app_state_root / "updater" / "apply_app_update.ps1"
    mutable_helper = mutable_root / "updater" / "apply_app_update.ps1"
    updater_config = app_state_root / "updater" / UPDATER_CONFIG_FILENAME
    payload_marker = read_payload_identity_marker(mutable_root=mutable_root)
    payload_summary = (
        "missing"
        if payload_marker is None
        else f"marker_schema_version={payload_marker.get('marker_schema_version')} app_version={payload_marker.get('app_version')} build_id={payload_marker.get('build_id')} channel={payload_marker.get('channel')} payload_sha256={payload_marker.get('payload_sha256')}"
    )
    log_line(
        "INSTALL_RUNTIME_DIAGNOSTICS "
        f"phase={phase} marker={INSTALL_DIAGNOSTIC_MARKER} "
        f"executable={Path(sys.executable).resolve()} "
        f"app_state_root={app_state_root} "
        f"mutable_app_root={mutable_root} "
        f"installed_manifest_exists={manifest_path.exists()} "
        f"app_state_helper_exists={app_state_helper.exists()} "
        f"mutable_helper_exists={mutable_helper.exists()} "
        f"updater_config_exists={updater_config.exists()} "
        f"payload_identity={payload_summary}",
        tag="startup",
    )


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
        raise UpdaterInstallStateError(
            f"Installation is missing required updater metadata: {app_state_root / 'installed_app_manifest.json'}"
        )
    _log_installed_manifest_stale(app_state_root=app_state_root, installed_manifest=installed_manifest)

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


def _log_installed_manifest_stale(*, app_state_root: Path, installed_manifest: dict) -> None:
    mutable_root = Path(str(installed_manifest.get("mutable_app_root") or app_state_root / "App")).expanduser()
    payload_identity = read_payload_identity_marker(mutable_root=mutable_root)
    if payload_identity is None:
        return
    installed_version = str(installed_manifest.get("app_version") or "")
    installed_build = str(installed_manifest.get("build_id") or "")
    installed_channel = str(installed_manifest.get("channel") or "")
    payload_version = str(payload_identity.get("app_version") or "")
    payload_build = str(payload_identity.get("build_id") or "")
    payload_channel = str(payload_identity.get("channel") or "")
    if (
        installed_version == payload_version
        and installed_build == payload_build
        and installed_channel == payload_channel
    ):
        return
    log_line(
        "INSTALLED_MANIFEST_STALE "
        f"app_state_root={app_state_root} "
        f"mutable_app_root={mutable_root} "
        f"installed_version={installed_version} installed_build_id={installed_build} installed_channel={installed_channel} "
        f"payload_version={payload_version} payload_build_id={payload_build} payload_channel={payload_channel}",
        tag="error",
    )


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
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    launch_audit_path = updater_root / "launch_helper.audit.json"
    launch_failure_path = updater_root / "launch_helper.failure.log"
    launch_audit_payload = {
        "event": "UPDATER_HELPER_LAUNCH_ATTEMPT",
        "helper_script": str(helper_script),
        "manifest_ref": manifest_ref,
        "cwd": str(helper_cwd),
        "command": cmd,
        "app_state_root": str(app_state_root),
        "wait_for_pid": wait_for_pid,
    }
    launch_audit_path.write_text(json.dumps(launch_audit_payload, indent=2), encoding="utf-8")
    popen_kwargs: dict[str, object] = {"cwd": str(helper_cwd)}
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    try:
        process = subprocess.Popen(cmd, **popen_kwargs)
    except Exception as exc:
        launch_failure_path.write_text(
            (
                "UPDATER_HELPER_LAUNCH_FAILED "
                f"helper_script={helper_script} cwd={helper_cwd} app_state_root={app_state_root} "
                f"manifest_ref={manifest_ref} cmd={cmd} error={exc}"
            ),
            encoding="utf-8",
        )
        raise
    apply_log_path = updater_root / "apply_update.log"
    time.sleep(0.35)
    if not apply_log_path.exists():
        launch_failure_path.write_text(
            (
                "UPDATER_HELPER_LAUNCH_PATH_DEFECT "
                f"helper_script={helper_script} cwd={helper_cwd} app_state_root={app_state_root} "
                f"manifest_ref={manifest_ref} cmd={cmd} pid={process.pid} "
                "detail=helper_started_but_apply_update_log_missing_after_launch"
            ),
            encoding="utf-8",
        )
        log_line(
            "UPDATER_HELPER_LAUNCH_PATH_DEFECT "
            f"failure_artifact={launch_failure_path} apply_log={apply_log_path} pid={process.pid}",
            tag="error",
        )
    return process
