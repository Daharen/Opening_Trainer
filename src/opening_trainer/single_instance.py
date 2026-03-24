from __future__ import annotations

import atexit
import os
import threading

INSTANCE_MUTEX_ENV = "OPENING_TRAINER_APP_MUTEX_NAME"

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
