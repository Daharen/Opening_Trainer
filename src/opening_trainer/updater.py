from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
from base64 import b64encode
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
HELPER_SCRIPT_NAME = "apply_app_update.ps1"
HELPER_WRAPPER_SCRIPT_NAME = "invoke_apply_app_update.ps1"
BOOTSTRAP_ENTER_MARKER = "encoded_bootstrap_v1_entered"


@dataclass(frozen=True)
class HelperLaunchResult:
    process: subprocess.Popen
    update_attempt_id: str
    helper_bootstrap_proven: bool
    proof_artifact: str | None = None
    failure_detail: str | None = None


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
    app_state_helper = app_state_root / "updater" / HELPER_SCRIPT_NAME
    mutable_helper = mutable_root / "updater" / HELPER_SCRIPT_NAME
    app_state_wrapper = app_state_root / "updater" / HELPER_WRAPPER_SCRIPT_NAME
    mutable_wrapper = mutable_root / "updater" / HELPER_WRAPPER_SCRIPT_NAME
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
        f"app_state_wrapper_exists={app_state_wrapper.exists()} "
        f"mutable_wrapper_exists={mutable_wrapper.exists()} "
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


def _candidate_helper_sources(*, app_state_root: Path, mutable_root: Path, script_name: str) -> list[Path]:
    candidates = [mutable_root / "updater" / script_name]
    for bootstrap_root in _bootstrap_root_candidates(app_state_root=app_state_root):
        candidates.append(bootstrap_root / script_name)
    return candidates


def _audit_updater_prerequisites(*, app_state_root: Path, installed_manifest: dict | None) -> None:
    mutable_root = Path(str((installed_manifest or {}).get("mutable_app_root") or app_state_root / "App")).expanduser()
    helper_target = _updater_runtime_root(app_state_root) / HELPER_SCRIPT_NAME
    wrapper_target = _updater_runtime_root(app_state_root) / HELPER_WRAPPER_SCRIPT_NAME
    helper_candidates = _candidate_helper_sources(
        app_state_root=app_state_root,
        mutable_root=mutable_root,
        script_name=HELPER_SCRIPT_NAME,
    )
    wrapper_candidates = _candidate_helper_sources(
        app_state_root=app_state_root,
        mutable_root=mutable_root,
        script_name=HELPER_WRAPPER_SCRIPT_NAME,
    )
    config_path = updater_config_path(app_state_root)
    log_line(
        "UPDATER_PREREQ_AUDIT "
        f"app_state_root={app_state_root} "
        f"mutable_app_root={mutable_root} "
        f"executable={Path(sys.executable).resolve()} "
        f"installed_manifest_path={app_state_root / 'installed_app_manifest.json'} "
        f"installed_manifest_exists={(app_state_root / 'installed_app_manifest.json').exists()} "
        f"helper_target={helper_target} helper_target_exists={helper_target.exists()} "
        f"wrapper_target={wrapper_target} wrapper_target_exists={wrapper_target.exists()} "
        f"updater_config_path={config_path} updater_config_exists={config_path.exists()}",
        tag="startup",
    )
    for index, candidate in enumerate(helper_candidates, start=1):
        log_line(
            f"UPDATER_PREREQ_AUDIT helper_candidate_index={index} path={candidate} exists={candidate.exists()}",
            tag="startup",
        )
    for index, candidate in enumerate(wrapper_candidates, start=1):
        log_line(
            f"UPDATER_PREREQ_AUDIT wrapper_candidate_index={index} path={candidate} exists={candidate.exists()}",
            tag="startup",
        )


def _resolve_updater_script(*, app_state_root: Path, installed_manifest: dict, script_name: str, prerequisite: str) -> Path | None:
    helper_target = _updater_runtime_root(app_state_root) / script_name
    mutable_root = Path(str(installed_manifest.get("mutable_app_root") or "")).expanduser()
    if not str(mutable_root).strip():
        mutable_root = app_state_root / "App"
    candidates = [
        helper_target,
        *_candidate_helper_sources(app_state_root=app_state_root, mutable_root=mutable_root, script_name=script_name),
    ]
    for index, candidate in enumerate(candidates, start=1):
        exists = candidate.exists()
        log_line(
            f"UPDATER_RESOLVE prerequisite={prerequisite} candidate_index={index} path={candidate} exists={exists}",
            tag="startup",
        )
        if not exists:
            continue
        if candidate == helper_target:
            return helper_target
        if _copy_file(candidate, helper_target, prerequisite=prerequisite):
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
        f"helper_present={(updater_root / HELPER_SCRIPT_NAME).exists()} "
        f"wrapper_present={(updater_root / HELPER_WRAPPER_SCRIPT_NAME).exists()}",
        tag="startup",
    )
    if installed_manifest is None:
        raise UpdaterInstallStateError(
            f"Installation is missing required updater metadata: {app_state_root / 'installed_app_manifest.json'}"
        )
    _log_installed_manifest_stale(app_state_root=app_state_root, installed_manifest=installed_manifest)

    helper_script = _resolve_updater_script(
        app_state_root=app_state_root,
        installed_manifest=installed_manifest,
        script_name=HELPER_SCRIPT_NAME,
        prerequisite="helper",
    )
    if helper_script is None or not helper_script.exists():
        raise UpdaterInstallStateError(
            f"Installation is missing required updater helper components: {updater_root / HELPER_SCRIPT_NAME}"
        )
    wrapper_script = _resolve_updater_script(
        app_state_root=app_state_root,
        installed_manifest=installed_manifest,
        script_name=HELPER_WRAPPER_SCRIPT_NAME,
        prerequisite="wrapper",
    )
    if wrapper_script is None or not wrapper_script.exists():
        raise UpdaterInstallStateError(
            f"Installation is missing required updater helper components: {updater_root / HELPER_WRAPPER_SCRIPT_NAME}"
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


def _normalize_updater_path(path: Path) -> str:
    return str(path.expanduser().resolve(strict=False))


def _bootstrap_artifact_paths(updater_root: Path) -> dict[str, Path]:
    return {
        "bootstrap_launch_log": updater_root / "apply_update.bootstrap.launch.log",
        "bootstrap_failure_log": updater_root / "apply_update.bootstrap.failure.log",
        "wrapper_log": updater_root / "apply_update.wrapper.log",
        "wrapper_failure_log": updater_root / "apply_update.wrapper.failure.log",
        "helper_bootstrap_log": updater_root / "apply_update.bootstrap.log",
        "helper_apply_log": updater_root / "apply_update.log",
    }


def _build_encoded_bootstrap_command(
    *,
    wrapper_script: Path,
    helper_script: Path,
    manifest_ref: str,
    app_state_root: Path,
    wait_for_pid: int,
    relaunch_exe: str,
    relaunch_args_json: str,
    update_attempt_id: str,
    artifacts: dict[str, Path],
) -> tuple[str, str]:
    payload = {
        "wrapper_path": _normalize_updater_path(wrapper_script),
        "helper_path": _normalize_updater_path(helper_script),
        "manifest_ref": manifest_ref,
        "app_state_root": _normalize_updater_path(app_state_root),
        "wait_for_pid": int(wait_for_pid),
        "relaunch_exe_path": relaunch_exe,
        "relaunch_args_json": relaunch_args_json,
        "update_attempt_id": update_attempt_id,
        "bootstrap_launch_log_path": _normalize_updater_path(artifacts["bootstrap_launch_log"]),
        "bootstrap_failure_log_path": _normalize_updater_path(artifacts["bootstrap_failure_log"]),
        "wrapper_log_path": _normalize_updater_path(artifacts["wrapper_log"]),
        "wrapper_failure_log_path": _normalize_updater_path(artifacts["wrapper_failure_log"]),
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    quoted_payload = json.dumps(payload_json)
    script = f"""
$ErrorActionPreference = 'Stop'
$payload = ConvertFrom-Json -InputObject {quoted_payload}
$bootstrapLaunchPath = [string]$payload.bootstrap_launch_log_path
$bootstrapFailurePath = [string]$payload.bootstrap_failure_log_path
$wrapperPath = [string]$payload.wrapper_path
$helperPath = [string]$payload.helper_path
$manifestRef = [string]$payload.manifest_ref
$appStateRoot = [string]$payload.app_state_root
$updateAttemptId = [string]$payload.update_attempt_id
$relaunchExePath = [string]$payload.relaunch_exe_path
$rawRelaunchArgs = [string]$payload.relaunch_args_json
$waitForPid = [int]$payload.wait_for_pid
function Write-BootstrapLine {{
    param([string]$Path, [string]$Message)
    $folder = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($folder)) {{
        New-Item -ItemType Directory -Path $folder -Force | Out-Null
    }}
    Add-Content -LiteralPath $Path -Value ("{{0}} {{1}}" -f ([DateTime]::UtcNow.ToString('o')), $Message) -Encoding utf8
}}
try {{
    $psEdition = ''
    if ($PSVersionTable.PSEdition) {{ $psEdition = [string]$PSVersionTable.PSEdition }}
    Write-BootstrapLine -Path $bootstrapLaunchPath -Message ("marker={BOOTSTRAP_ENTER_MARKER} update_attempt_id={{0}} pid={{1}} cwd={{2}} ps_version={{3}} ps_edition={{4}} wrapper_path={{5}} helper_path={{6}} app_state_root={{7}} manifest_ref={{8}} relaunch_exe_path={{9}} raw_relaunch_args_payload={{10}} wait_pid={{11}}" -f $updateAttemptId, $PID, (Get-Location).Path, $PSVersionTable.PSVersion, $psEdition, $wrapperPath, $helperPath, $appStateRoot, $manifestRef, $relaunchExePath, $rawRelaunchArgs, $waitForPid)
    Write-BootstrapLine -Path $bootstrapLaunchPath -Message ("wrapper_exists={{0}} helper_exists={{1}}" -f (Test-Path -LiteralPath $wrapperPath -PathType Leaf), (Test-Path -LiteralPath $helperPath -PathType Leaf))
    if (-not (Test-Path -LiteralPath $wrapperPath -PathType Leaf)) {{
        Write-BootstrapLine -Path $bootstrapFailurePath -Message ("stage=bootstrap_wrapper_missing update_attempt_id={{0}} wrapper_path={{1}} helper_path={{2}} cwd={{3}}" -f $updateAttemptId, $wrapperPath, $helperPath, (Get-Location).Path)
        exit 42
    }}
    if (-not (Test-Path -LiteralPath $helperPath -PathType Leaf)) {{
        Write-BootstrapLine -Path $bootstrapFailurePath -Message ("stage=bootstrap_helper_missing update_attempt_id={{0}} wrapper_path={{1}} helper_path={{2}} cwd={{3}}" -f $updateAttemptId, $wrapperPath, $helperPath, (Get-Location).Path)
        exit 43
    }}
    & $wrapperPath -RealHelperPath $helperPath -ManifestPathOrUrl $manifestRef -AppStateRoot $appStateRoot -WaitForPid $waitForPid -RelaunchExePath $relaunchExePath -RelaunchArgs $rawRelaunchArgs -UpdateAttemptId $updateAttemptId
    $wrapperExit = $LASTEXITCODE
    if ($null -eq $wrapperExit) {{ $wrapperExit = 0 }}
    if ($wrapperExit -ne 0) {{
        Write-BootstrapLine -Path $bootstrapFailurePath -Message ("stage=bootstrap_wrapper_nonzero_exit update_attempt_id={{0}} exit_code={{1}} wrapper_path={{2}} helper_path={{3}} cwd={{4}}" -f $updateAttemptId, $wrapperExit, $wrapperPath, $helperPath, (Get-Location).Path)
        exit $wrapperExit
    }}
    Write-BootstrapLine -Path $bootstrapLaunchPath -Message ("stage=bootstrap_wrapper_handoff_success update_attempt_id={{0}} wrapper_path={{1}} helper_path={{2}}" -f $updateAttemptId, $wrapperPath, $helperPath)
    exit 0
}} catch {{
    $exType = ''
    if ($_.Exception -and $_.Exception.GetType()) {{ $exType = $_.Exception.GetType().FullName }}
    $stack = ''
    if ($_.ScriptStackTrace) {{ $stack = [string]$_.ScriptStackTrace }}
    Write-BootstrapLine -Path $bootstrapFailurePath -Message ("stage=bootstrap_wrapper_invocation_threw update_attempt_id={{0}} wrapper_path={{1}} helper_path={{2}} cwd={{3}} exception_type={{4}} exception_message={{5}} stack={{6}}" -f $updateAttemptId, $wrapperPath, $helperPath, (Get-Location).Path, $exType, $_.Exception.Message, $stack)
    exit 41
}}
""".strip()
    encoded = b64encode(script.encode("utf-16le")).decode("ascii")
    return script, encoded


def launch_updater_helper(
    manifest_path_or_url: str | None,
    *,
    app_state_root: Path,
    wait_for_pid: int,
    relaunch_exe_path: Path | None = None,
    relaunch_args: list[str] | None = None,
) -> HelperLaunchResult:
    _installed, helper_script = ensure_updater_prerequisites(app_state_root)
    helper_wrapper_script = helper_script.parent / HELPER_WRAPPER_SCRIPT_NAME
    manifest_ref = resolve_manifest_path_or_url(manifest_path_or_url, app_state_root=app_state_root)
    relaunch_exe = str(relaunch_exe_path) if relaunch_exe_path else ""
    relaunch_args_json = json.dumps(relaunch_args or ["--runtime-mode", "consumer"])
    update_attempt_id = uuid.uuid4().hex
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
        f"helper_wrapper_script={helper_wrapper_script} "
        f"helper_script={helper_script} "
        f"cwd={helper_cwd} "
        f"app_state_root={app_state_root} "
        f"localappdata={os.getenv('LOCALAPPDATA', '')}",
        tag="startup",
    )
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    artifacts = _bootstrap_artifact_paths(updater_root)
    inline_bootstrap_script, encoded_bootstrap_command = _build_encoded_bootstrap_command(
        wrapper_script=helper_wrapper_script,
        helper_script=helper_script,
        manifest_ref=manifest_ref,
        app_state_root=app_state_root,
        wait_for_pid=wait_for_pid,
        relaunch_exe=relaunch_exe,
        relaunch_args_json=relaunch_args_json,
        update_attempt_id=update_attempt_id,
        artifacts=artifacts,
    )
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-EncodedCommand",
        encoded_bootstrap_command,
    ]
    launch_audit_path = updater_root / "launch_helper.audit.json"
    launch_failure_path = updater_root / "launch_helper.failure.log"
    launch_audit_payload = {
        "event": "UPDATER_HELPER_LAUNCH_ATTEMPT",
        "launch_mode": "encoded_bootstrap_v1",
        "helper_script": str(helper_script),
        "helper_wrapper_script": str(helper_wrapper_script),
        "manifest_ref": manifest_ref,
        "cwd": str(helper_cwd),
        "command": cmd,
        "inline_bootstrap_script": inline_bootstrap_script,
        "encoded_command_length": len(encoded_bootstrap_command),
        "app_state_root": str(app_state_root),
        "wait_for_pid": wait_for_pid,
        "relaunch_args_json": relaunch_args_json,
        "expected_artifacts": {key: str(value) for key, value in artifacts.items()},
        "update_attempt_id": update_attempt_id,
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
                f"manifest_ref={manifest_ref} update_attempt_id={update_attempt_id} cmd={cmd} error={exc}"
            ),
            encoding="utf-8",
        )
        raise
    apply_log_path = artifacts["helper_apply_log"]
    helper_bootstrap_log_path = artifacts["helper_bootstrap_log"]
    bootstrap_launch_log_path = artifacts["bootstrap_launch_log"]
    bootstrap_failure_path = artifacts["bootstrap_failure_log"]
    wrapper_log_path = artifacts["wrapper_log"]
    wrapper_failure_path = artifacts["wrapper_failure_log"]

    def _artifact_matches_attempt(path: Path) -> bool:
        if not path.exists():
            return False
        try:
            return f"update_attempt_id={update_attempt_id}" in path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return False

    helper_pid = getattr(process, "pid", "unknown")

    def _process_has_exited() -> bool:
        poll_method = getattr(process, "poll", None)
        if not callable(poll_method):
            return False
        return poll_method() is not None

    proof_path: Path | None = None
    helper_proof_path: Path | None = None
    deadline = time.monotonic() + 4.0
    while time.monotonic() < deadline:
        for candidate in (
            bootstrap_launch_log_path,
            bootstrap_failure_path,
            wrapper_log_path,
            wrapper_failure_path,
            apply_log_path,
            helper_bootstrap_log_path,
        ):
            if _artifact_matches_attempt(candidate):
                proof_path = candidate
                if candidate in (apply_log_path, helper_bootstrap_log_path):
                    helper_proof_path = candidate
                break
        if helper_proof_path is not None:
            break
        if _process_has_exited():
            break
        time.sleep(0.2)

    if helper_proof_path is not None:
        return HelperLaunchResult(
            process=process,
            update_attempt_id=update_attempt_id,
            helper_bootstrap_proven=True,
            proof_artifact=str(helper_proof_path),
        )

    process_exited = _process_has_exited()
    bootstrap_proven = _artifact_matches_attempt(bootstrap_launch_log_path) or _artifact_matches_attempt(bootstrap_failure_path)
    wrapper_proven = _artifact_matches_attempt(wrapper_log_path) or _artifact_matches_attempt(wrapper_failure_path)
    helper_proven = _artifact_matches_attempt(helper_bootstrap_log_path) or _artifact_matches_attempt(apply_log_path)
    detail = "helper_process_started_but_no_matching_proof"
    if wrapper_proven and not helper_proven and process_exited:
        detail = "wrapper_proven_process_exited_without_helper_bootstrap"
    elif process_exited and not bootstrap_proven:
        detail = "bootstrap_not_proven_process_exited_without_matching_proof"
    elif bootstrap_proven and not wrapper_proven and _artifact_matches_attempt(bootstrap_failure_path):
        try:
            bootstrap_failure_text = bootstrap_failure_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            bootstrap_failure_text = ""
        if "stage=bootstrap_wrapper_missing" in bootstrap_failure_text:
            detail = "bootstrap_proven_wrapper_missing"
        elif "stage=bootstrap_helper_missing" in bootstrap_failure_text:
            detail = "bootstrap_proven_helper_missing"
        elif "stage=bootstrap_wrapper_invocation_threw" in bootstrap_failure_text:
            detail = "bootstrap_proven_wrapper_invocation_threw"
        elif "stage=bootstrap_wrapper_nonzero_exit" in bootstrap_failure_text:
            detail = "bootstrap_proven_wrapper_nonzero_exit"
        else:
            detail = "bootstrap_proven_wrapper_invocation_threw"
    elif not process_exited and not bootstrap_proven:
        detail = "bootstrap_not_proven_process_pending_without_matching_proof"
    elif not process_exited and wrapper_proven and not helper_proven:
        detail = "wrapper_proven_helper_bootstrap_not_proven_yet"
    launch_failure_path.write_text(
        (
            "UPDATER_HELPER_LAUNCH_PATH_DEFECT "
            f"helper_wrapper_script={helper_wrapper_script} "
            f"helper_script={helper_script} cwd={helper_cwd} app_state_root={app_state_root} "
            f"manifest_ref={manifest_ref} update_attempt_id={update_attempt_id} cmd={cmd} pid={helper_pid} "
            f"detail={detail} "
            f"apply_log_path={apply_log_path} helper_bootstrap_log_path={helper_bootstrap_log_path} bootstrap_launch_log_path={bootstrap_launch_log_path} bootstrap_failure_path={bootstrap_failure_path} "
            f"wrapper_log_path={wrapper_log_path} wrapper_failure_path={wrapper_failure_path}"
        ),
        encoding="utf-8",
    )
    log_line(
        "UPDATER_HELPER_LAUNCH_PATH_DEFECT "
        f"failure_artifact={launch_failure_path} update_attempt_id={update_attempt_id} apply_log={apply_log_path} "
        f"helper_bootstrap_log={helper_bootstrap_log_path} bootstrap_launch_log={bootstrap_launch_log_path} bootstrap_failure_log={bootstrap_failure_path} "
        f"wrapper_log={wrapper_log_path} wrapper_failure_log={wrapper_failure_path} "
        f"detail={detail} pid={helper_pid}",
        tag="error",
    )
    return HelperLaunchResult(
        process=process,
        update_attempt_id=update_attempt_id,
        helper_bootstrap_proven=False,
        proof_artifact=str(proof_path or launch_failure_path),
        failure_detail=detail,
    )
