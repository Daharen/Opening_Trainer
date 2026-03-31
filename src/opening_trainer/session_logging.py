from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import threading
from typing import Callable

RING_BUFFER_MAX_LINES = 1_000
SESSION_FILE_MAX_LINES = 10_000
MAX_SESSION_FILES = 5
DEFAULT_TAG = "startup"
SESSION_ID_ENV = "OPENING_TRAINER_SESSION_ID"
SESSION_LOG_PATH_ENV = "OPENING_TRAINER_SESSION_LOG_PATH"
SESSION_LOG_DIR_ENV = "OPENING_TRAINER_SESSION_LOG_DIR"
CONSOLE_MIRROR_ENV = "OPENING_TRAINER_CONSOLE_MIRROR"

LOG_TAGS = {"launcher", "startup", "corpus", "evaluation", "review", "error"}


@dataclass(frozen=True)
class SessionLogEntry:
    timestamp: str
    tag: str
    message: str
    line: str


class SessionLogger:
    def __init__(self, session_id: str, log_path: Path, mirror_to_console: bool = False):
        self.session_id = session_id
        self.log_path = log_path
        self.mirror_to_console = mirror_to_console
        self._lock = threading.RLock()
        self._ring: deque[str] = deque(maxlen=RING_BUFFER_MAX_LINES)
        self._persisted: deque[str] = deque(maxlen=SESSION_FILE_MAX_LINES)
        self._subscribers: dict[int, Callable[[str], None]] = {}
        self._next_subscriber_id = 1
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._load_existing_lines()

    def _load_existing_lines(self) -> None:
        if not self.log_path.exists():
            self.log_path.touch()
            return
        lines = self.log_path.read_text(encoding="utf-8").splitlines()
        for line in lines[-SESSION_FILE_MAX_LINES:]:
            self._persisted.append(line)
            self._ring.append(line)
        if len(lines) > SESSION_FILE_MAX_LINES:
            self._rewrite_persisted()

    def bootstrap_lines(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._persisted)

    def visible_lines(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._ring)

    def clear_visible_buffer(self) -> None:
        with self._lock:
            self._ring.clear()

    def append(self, message: str, tag: str = DEFAULT_TAG) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        normalized_tag = tag if tag in LOG_TAGS else DEFAULT_TAG
        line = f"[{timestamp}] [{normalized_tag}] {message}"
        callbacks: list[Callable[[str], None]] = []
        with self._lock:
            self._ring.append(line)
            self._persisted.append(line)
            callbacks = list(self._subscribers.values())
            if len(self._persisted) >= SESSION_FILE_MAX_LINES:
                self._rewrite_persisted()
            else:
                self.log_path.parent.mkdir(parents=True, exist_ok=True)
                with self.log_path.open("a", encoding="utf-8") as handle:
                    handle.write(line + "\n")
        if self.mirror_to_console:
            print(line, flush=True)
        for callback in callbacks:
            try:
                callback(line)
            except Exception:
                continue
        return line

    def _rewrite_persisted(self) -> None:
        payload = "\n".join(self._persisted)
        if payload:
            payload += "\n"
        self.log_path.write_text(payload, encoding="utf-8")

    def subscribe(self, callback: Callable[[str], None]) -> Callable[[], None]:
        with self._lock:
            subscriber_id = self._next_subscriber_id
            self._next_subscriber_id += 1
            self._subscribers[subscriber_id] = callback

        def unsubscribe() -> None:
            with self._lock:
                self._subscribers.pop(subscriber_id, None)

        return unsubscribe


_active_logger: SessionLogger | None = None


def _default_session_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}"


def _default_log_dir() -> Path:
    env_value = os.getenv(SESSION_LOG_DIR_ENV)
    if env_value:
        return Path(env_value)
    return Path("logs") / "sessions"


def _session_log_path(session_id: str) -> Path:
    explicit = os.getenv(SESSION_LOG_PATH_ENV)
    if explicit:
        return Path(explicit)
    return _default_log_dir() / f"session_{session_id}.log"


def _prune_old_session_files(log_dir: Path, active_log_path: Path | None = None) -> None:
    if not log_dir.exists():
        return
    candidates = sorted(log_dir.glob("session_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if active_log_path is not None:
        candidates = [path for path in candidates if path != active_log_path]
        max_non_active = max(0, MAX_SESSION_FILES - 1)
    else:
        max_non_active = MAX_SESSION_FILES
    for old_file in candidates[max_non_active:]:
        try:
            old_file.unlink()
        except OSError:
            continue


def get_session_logger() -> SessionLogger:
    global _active_logger
    if _active_logger is None:
        session_id = os.getenv(SESSION_ID_ENV) or _default_session_id()
        os.environ.setdefault(SESSION_ID_ENV, session_id)
        log_path = _session_log_path(session_id)
        mirror = os.getenv(CONSOLE_MIRROR_ENV, "0").strip().lower() in {"1", "true", "yes", "on"}
        _active_logger = SessionLogger(session_id=session_id, log_path=log_path, mirror_to_console=mirror)
        _prune_old_session_files(log_path.parent, active_log_path=log_path)
    return _active_logger


def log_line(message: str, tag: str = DEFAULT_TAG) -> str:
    return get_session_logger().append(message=message, tag=tag)


def reset_logger_for_tests() -> None:
    global _active_logger
    _active_logger = None
