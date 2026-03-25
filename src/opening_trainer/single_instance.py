from __future__ import annotations

import atexit
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .session_logging import SESSION_ID_ENV, SESSION_LOG_PATH_ENV

INSTANCE_MUTEX_ENV = "OPENING_TRAINER_APP_MUTEX_NAME"
INSTANCE_DIAGNOSTICS_PATH_ENV = "OPENING_TRAINER_INSTANCE_DIAGNOSTICS_PATH"

_guard_lock = threading.Lock()
_active_guard = None


class _WindowsMutexGuard:
    def __init__(self, name: str):
        import ctypes

        self._ctypes = ctypes
        self._kernel32 = ctypes.windll.kernel32
        self.name = name
        self.handle = self._kernel32.CreateMutexW(None, False, name)
        if not self.handle:
            raise OSError("CreateMutexW failed.")

    def acquired(self) -> bool:
        already_exists_error = 183
        return self._ctypes.GetLastError() != already_exists_error

    def release(self) -> None:
        if self.handle:
            self._kernel32.CloseHandle(self.handle)
            self.handle = None


def _default_mutex_name() -> str:
    user = os.getenv("USERNAME") or os.getenv("USER") or "user"
    return f"Local\\OpeningTrainer.{user}.app"


def _default_diagnostics_path() -> Path:
    return Path("logs") / "opening_trainer_instance.json"


def _diagnostics_path() -> Path:
    configured = os.getenv(INSTANCE_DIAGNOSTICS_PATH_ENV)
    if configured:
        return Path(configured)
    return _default_diagnostics_path()


@dataclass(frozen=True)
class InstanceDiagnostics:
    pid: int
    session_id: str | None
    startup_utc: str
    session_log_path: str | None
    parent_pid: int | None
    window_title: str | None = None


def acquire_single_instance_guard() -> bool:
    global _active_guard
    with _guard_lock:
        if _active_guard is not None:
            return True
        if os.name != "nt":
            return True
        name = os.getenv(INSTANCE_MUTEX_ENV) or _default_mutex_name()
        guard = _WindowsMutexGuard(name)
        if not guard.acquired():
            guard.release()
            return False
        cleanup_stale_instance_diagnostics()
        _active_guard = guard
        atexit.register(release_single_instance_guard)
        return True


def release_single_instance_guard() -> None:
    global _active_guard
    with _guard_lock:
        if _active_guard is None:
            return
        _active_guard.release()
        _active_guard = None


def write_instance_diagnostics(window_title: str | None = None) -> Path:
    payload = InstanceDiagnostics(
        pid=os.getpid(),
        session_id=os.getenv(SESSION_ID_ENV),
        startup_utc=datetime.now(timezone.utc).isoformat(),
        session_log_path=os.getenv(SESSION_LOG_PATH_ENV),
        parent_pid=os.getppid() if hasattr(os, "getppid") else None,
        window_title=window_title,
    )
    path = _diagnostics_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload.__dict__, indent=2), encoding="utf-8")
    return path


def remove_instance_diagnostics() -> None:
    path = _diagnostics_path()
    if not path.exists():
        return
    try:
        path.unlink()
    except OSError:
        return


def read_instance_diagnostics() -> InstanceDiagnostics | None:
    path = _diagnostics_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    try:
        return InstanceDiagnostics(
            pid=int(payload.get("pid")),
            session_id=payload.get("session_id"),
            startup_utc=str(payload.get("startup_utc")),
            session_log_path=payload.get("session_log_path"),
            parent_pid=int(payload["parent_pid"]) if payload.get("parent_pid") is not None else None,
            window_title=payload.get("window_title"),
        )
    except Exception:
        return None


def cleanup_stale_instance_diagnostics() -> bool:
    path = _diagnostics_path()
    if not path.exists():
        return False
    remove_instance_diagnostics()
    return True
