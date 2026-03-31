from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

APP_STATE_DIRNAME = "OpeningTrainer"
DEFAULT_MUTABLE_APP_ROOT_SUFFIX = Path("OpeningTrainer") / "App"
SECONDARY_MUTABLE_APP_ROOT_SUFFIX = Path("OpeningTrainer") / "App"
INSTALLED_APP_MANIFEST_FILENAME = "installed_app_manifest.json"


@dataclass(frozen=True)
class ProbeResult:
    root: Path
    ok: bool
    detail: str


def _local_app_data_root() -> Path:
    return Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))


def default_mutable_app_root() -> Path:
    return _local_app_data_root() / DEFAULT_MUTABLE_APP_ROOT_SUFFIX


def secondary_mutable_app_root() -> Path:
    return Path(os.getenv("USERPROFILE", str(Path.home()))) / SECONDARY_MUTABLE_APP_ROOT_SUFFIX


def probe_mutable_app_root(root: Path) -> ProbeResult:
    test_dir = root / ".probe"
    test_a = test_dir / "probe-a.tmp"
    test_b = test_dir / "probe-b.tmp"
    try:
        test_dir.mkdir(parents=True, exist_ok=True)
        with test_a.open("w", encoding="utf-8") as handle:
            handle.write("probe")
            handle.flush()
        os.replace(test_a, test_b)
        with test_b.open("a", encoding="utf-8") as handle:
            handle.write("-append")
        test_b.unlink()
        test_dir.rmdir()
        return ProbeResult(root=root, ok=True, detail="create/write/replace/delete/cleanup-ok")
    except Exception as exc:
        for path in (test_a, test_b):
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        try:
            if test_dir.exists():
                test_dir.rmdir()
        except Exception:
            pass
        return ProbeResult(root=root, ok=False, detail=f"probe-failed: {exc}")


def choose_mutable_app_root(*, override_root: Path | None = None) -> tuple[Path, list[ProbeResult]]:
    if override_root is not None:
        result = probe_mutable_app_root(override_root)
        if not result.ok:
            raise RuntimeError(f"Selected mutable app root is not writable: {override_root}; {result.detail}")
        return override_root, [result]

    candidates = [default_mutable_app_root(), secondary_mutable_app_root()]
    results: list[ProbeResult] = []
    for candidate in candidates:
        result = probe_mutable_app_root(candidate)
        results.append(result)
        if result.ok:
            return candidate, results
    raise RuntimeError("Unable to find a writable mutable app root from allowed defaults.")


def installed_app_manifest_path(app_state_root: Path | None = None) -> Path:
    root = app_state_root or (_local_app_data_root() / APP_STATE_DIRNAME)
    return root / INSTALLED_APP_MANIFEST_FILENAME


def read_installed_app_manifest(app_state_root: Path | None = None) -> dict | None:
    path = installed_app_manifest_path(app_state_root)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_installed_app_manifest(
    *,
    app_state_root: Path,
    app_version: str,
    channel: str,
    mutable_app_root: Path,
    payload_filename: str,
    payload_sha256: str | None,
    bootstrap_version: str | None,
) -> Path:
    app_state_root.mkdir(parents=True, exist_ok=True)
    path = installed_app_manifest_path(app_state_root)
    payload = {
        "installed_app_manifest_version": 1,
        "app_version": app_version,
        "channel": channel,
        "mutable_app_root": str(mutable_app_root),
        "payload_filename": payload_filename,
        "payload_sha256": payload_sha256,
        "bootstrap_version": bootstrap_version,
        "installed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path
