from __future__ import annotations

import hashlib
import io
import logging
import os
import shutil
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path

from .zstd_compat import open_binary_reader

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SQLitePayloadResolution:
    requested_path: Path
    active_path: Path
    used_plain_sqlite: bool
    used_compressed_sqlite: bool
    mounted_path: Path | None


class SQLitePayloadResolutionError(RuntimeError):
    def __init__(self, code: str, detail: str, requested_path: Path):
        self.code = code
        self.detail = detail
        self.requested_path = requested_path
        super().__init__(f"{code}: {detail}")


class MountedSQLiteLease:
    def __init__(self, manager: "MountedSQLiteManager", mount_path: Path):
        self._manager = manager
        self.mount_path = mount_path
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._manager._release(self.mount_path)


class MountedSQLiteManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ref_counts: dict[Path, int] = {}
        self._current_pid = os.getpid()
        self._root = Path.home() / ".opening_trainer" / "sqlite_mounts"
        self._owner_dir = self._root / f"pid-{self._current_pid}"
        self._initialized = False

    def resolve(self, requested_path: str | Path) -> tuple[SQLitePayloadResolution, MountedSQLiteLease | None]:
        requested = Path(requested_path)
        plain, compressed = self._plain_and_compressed_candidates(requested)
        _LOGGER.info("sqlite payload resolution requested=%s plain_candidate=%s compressed_candidate=%s", requested, plain, compressed)

        if plain.exists() and plain.is_file():
            _LOGGER.info("sqlite payload resolved via plain sqlite path=%s", plain)
            return (
                SQLitePayloadResolution(
                    requested_path=requested,
                    active_path=plain,
                    used_plain_sqlite=True,
                    used_compressed_sqlite=False,
                    mounted_path=None,
                ),
                None,
            )

        if compressed.exists() and compressed.is_file():
            mounted = self._mount_compressed(compressed)
            with self._lock:
                self._ref_counts[mounted] = self._ref_counts.get(mounted, 0) + 1
            _LOGGER.info("sqlite payload resolved via zst mount source=%s mounted=%s", compressed, mounted)
            return (
                SQLitePayloadResolution(
                    requested_path=requested,
                    active_path=mounted,
                    used_plain_sqlite=False,
                    used_compressed_sqlite=True,
                    mounted_path=mounted,
                ),
                MountedSQLiteLease(self, mounted),
            )

        raise SQLitePayloadResolutionError(
            code="sqlite_payload_missing",
            detail=f"No usable SQLite payload found. plain={plain} compressed={compressed}",
            requested_path=requested,
        )

    def _plain_and_compressed_candidates(self, requested: Path) -> tuple[Path, Path]:
        text = requested.name
        if text.endswith(".sqlite.zst"):
            plain = requested.with_name(text[: -len(".zst")])
            return plain, requested
        if text.endswith(".sqlite"):
            return requested, requested.with_name(text + ".zst")
        return requested, requested.with_name(text + ".zst")

    def _ensure_initialized(self) -> None:
        with self._lock:
            if self._initialized:
                return
            self._owner_dir.mkdir(parents=True, exist_ok=True)
            self._cleanup_stale_mounts_locked()
            self._initialized = True

    def _cleanup_stale_mounts_locked(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        for entry in self._root.iterdir():
            if not entry.is_dir() or entry.name == self._owner_dir.name or not entry.name.startswith("pid-"):
                continue
            pid_text = entry.name.split("pid-", 1)[1]
            try:
                pid = int(pid_text)
            except ValueError:
                shutil.rmtree(entry, ignore_errors=True)
                continue
            if self._pid_is_alive(pid):
                continue
            shutil.rmtree(entry, ignore_errors=True)

    def _pid_is_alive(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    def _mount_compressed(self, compressed_path: Path) -> Path:
        self._ensure_initialized()
        mount_path = self._owner_dir / f"{self._mount_key(compressed_path)}.sqlite"
        partial_path = mount_path.with_suffix(".sqlite.partial")

        if mount_path.exists():
            _LOGGER.info("sqlite mount candidate already exists; validating mounted=%s source=%s", mount_path, compressed_path)
            self._validate_sqlite(mount_path)
            return mount_path

        try:
            _LOGGER.info("mounting sqlite zst source=%s partial=%s target=%s", compressed_path, partial_path, mount_path)
            with compressed_path.open("rb") as source, open_binary_reader(source) as reader, partial_path.open("wb") as output:
                shutil.copyfileobj(reader, output, length=1024 * 1024)
            self._validate_sqlite(partial_path)
            partial_path.replace(mount_path)
            _LOGGER.info("sqlite zst mount ready mounted=%s source=%s", mount_path, compressed_path)
            return mount_path
        except Exception as exc:
            partial_path.unlink(missing_ok=True)
            mount_path.unlink(missing_ok=True)
            code = "sqlite_decompression_failed"
            if isinstance(exc, SQLitePayloadResolutionError):
                code = exc.code
            _LOGGER.warning("sqlite zst mount failed source=%s code=%s detail=%s", compressed_path, code, exc)
            raise SQLitePayloadResolutionError(code=code, detail=str(exc), requested_path=compressed_path) from exc

    def _validate_sqlite(self, path: Path) -> None:
        try:
            connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            connection.execute("PRAGMA schema_version").fetchone()
            connection.close()
        except sqlite3.Error as exc:
            raise SQLitePayloadResolutionError(
                code="sqlite_validation_failed",
                detail=f"Mounted file is not a readable SQLite database: {exc}",
                requested_path=path,
            ) from exc

    def _mount_key(self, source: Path) -> str:
        stat = source.stat()
        digest = hashlib.sha256()
        digest.update(str(source.resolve()).encode("utf-8", errors="replace"))
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        return digest.hexdigest()[:24]

    def _release(self, mount_path: Path) -> None:
        with self._lock:
            current = self._ref_counts.get(mount_path, 0)
            if current <= 1:
                self._ref_counts.pop(mount_path, None)
                mount_path.unlink(missing_ok=True)
                _LOGGER.info("sqlite mount released and removed mounted=%s", mount_path)
                return
            self._ref_counts[mount_path] = current - 1
            _LOGGER.info("sqlite mount lease released mounted=%s remaining_refs=%d", mount_path, self._ref_counts[mount_path])


_DEFAULT_MANAGER = MountedSQLiteManager()


def get_mounted_sqlite_manager() -> MountedSQLiteManager:
    return _DEFAULT_MANAGER
