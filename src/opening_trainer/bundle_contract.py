from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .sqlite_mounts import MountedSQLiteLease, SQLitePayloadResolutionError, get_mounted_sqlite_manager

SUPPORTED_BUNDLE_POSITION_KEY_FORMAT = "fen_normalized"
SUPPORTED_BUNDLE_MOVE_KEY_FORMAT = "uci"
BUNDLE_MANIFEST_NAME = "manifest.json"
BUNDLE_AGGREGATE_RELATIVE_PATH = Path("data/aggregated_position_move_counts.jsonl")
BUNDLE_SQLITE_RELATIVE_PATH = Path("data/corpus.sqlite")
SUPPORTED_BUNDLE_PAYLOAD_FORMATS = {"jsonl", "sqlite"}
BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH = Path("data/exact_corpus.sqlite")
BUNDLE_BEHAVIORAL_PROFILE_SET_DEFAULT_RELATIVE_PATH = Path("data/behavioral_profile_set.sqlite")


@dataclass(frozen=True)
class BundlePayloadResolution:
    payload_format: str
    payload_path: Path
    payload_version: str | None = None
    requested_path: Path | None = None
    used_plain_sqlite: bool | None = None
    used_compressed_sqlite: bool | None = None
    mounted_sqlite_path: Path | None = None
    mounted_sqlite_lease: MountedSQLiteLease | None = None


def manifest_declared_aggregate_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    declared_path = manifest.get("aggregate_position_file")
    if not isinstance(declared_path, str) or not declared_path.strip():
        return None
    return bundle_dir / Path(declared_path)


def manifest_declared_sqlite_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    for key in ("sqlite_corpus_file", "corpus_sqlite_file", "payload_file"):
        declared_path = manifest.get(key)
        if not isinstance(declared_path, str) or not declared_path.strip():
            continue
        return bundle_dir / Path(declared_path)
    return None


def manifest_declared_exact_sqlite_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    for key in (
        "exact_corpus_file",
        "exact_corpus_payload_file",
        "exact_sqlite_file",
        "exact_payload_file",
        "sqlite_corpus_file",
        "corpus_sqlite_file",
        "payload_file",
    ):
        declared_path = manifest.get(key)
        if not isinstance(declared_path, str) or not declared_path.strip():
            continue
        return bundle_dir / Path(declared_path)
    return None


def manifest_declared_canonical_exact_payload_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    for key in ("canonical_exact_payload_file", "canonical_exact_corpus_file"):
        declared_path = manifest.get(key)
        if not isinstance(declared_path, str) or not declared_path.strip():
            continue
        return bundle_dir / Path(declared_path)
    return None


def manifest_declared_compatibility_exact_payload_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    for key in ("compatibility_exact_payload_file", "compatibility_exact_corpus_file"):
        declared_path = manifest.get(key)
        if not isinstance(declared_path, str) or not declared_path.strip():
            continue
        return bundle_dir / Path(declared_path)
    return None


def manifest_payload_version(manifest: dict[str, object]) -> str | None:
    for key in ("payload_version", "exact_payload_version", "compact_payload_version"):
        value = manifest.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def manifest_declared_behavioral_profile_set_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    for key in ("behavioral_profile_set_file", "behavioral_profile_set_sqlite_file", "timing_profile_set_file"):
        declared_path = manifest.get(key)
        if not isinstance(declared_path, str) or not declared_path.strip():
            continue
        return bundle_dir / Path(declared_path)
    return None


def payload_status_mentions_counts(payload_status: object) -> bool:
    return isinstance(payload_status, str) and "count" in payload_status.lower()


def aggregate_payload_exposes_raw_counts(aggregate_path: Path) -> bool:
    try:
        with aggregate_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                candidate_moves = row.get("candidate_moves")
                if not isinstance(candidate_moves, list):
                    return False
                return all(isinstance(candidate, dict) and "raw_count" in candidate for candidate in candidate_moves)
    except (OSError, json.JSONDecodeError):
        return False
    return False


def resolve_bundle_payload(manifest: dict[str, object], bundle_dir: Path) -> tuple[BundlePayloadResolution | None, str | None]:
    declared_payload_format = manifest.get("payload_format")
    if isinstance(declared_payload_format, str) and declared_payload_format.strip():
        payload_format = declared_payload_format.strip().lower()
        if payload_format not in SUPPORTED_BUNDLE_PAYLOAD_FORMATS:
            return None, f"unsupported payload_format {declared_payload_format!r}"
        if payload_format == "sqlite":
            sqlite_path = manifest_declared_sqlite_path(manifest, bundle_dir) or (bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH)
            return _resolve_sqlite_payload(sqlite_path, manifest)
        aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir) or (bundle_dir / BUNDLE_AGGREGATE_RELATIVE_PATH)
        if not aggregate_path.exists():
            return None, f"aggregate payload is missing at {aggregate_path}"
        if not aggregate_path.is_file():
            return None, f"aggregate payload path {aggregate_path} is not a file"
        return BundlePayloadResolution(payload_format="jsonl", payload_path=aggregate_path, payload_version=manifest_payload_version(manifest)), None

    sqlite_default_path = bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH
    sqlite_default_resolution, sqlite_default_error = _resolve_sqlite_payload(sqlite_default_path, manifest, strict=False)
    if sqlite_default_resolution is not None:
        return sqlite_default_resolution, None

    declared_aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir)
    if declared_aggregate_path is not None:
        if not declared_aggregate_path.exists():
            return None, f"aggregate payload is missing at {declared_aggregate_path}"
        if not declared_aggregate_path.is_file():
            return None, f"aggregate payload path {declared_aggregate_path} is not a file"
        return BundlePayloadResolution(payload_format="jsonl", payload_path=declared_aggregate_path, payload_version=manifest_payload_version(manifest)), None

    aggregate_path = bundle_dir / BUNDLE_AGGREGATE_RELATIVE_PATH
    if aggregate_path.exists() and aggregate_path.is_file():
        return BundlePayloadResolution(payload_format="jsonl", payload_path=aggregate_path, payload_version=manifest_payload_version(manifest)), None

    return None, (
        "bundle did not expose a supported payload; expected manifest payload_format, "
        "data/corpus.sqlite, or data/aggregated_position_move_counts.jsonl"
    )


def is_supported_builder_aggregate_bundle(manifest: dict[str, object], bundle_dir: Path) -> tuple[bool, Path | None, str]:
    build_status = manifest.get("build_status")
    if build_status != "aggregation_complete":
        return False, None, f"build_status {build_status!r} is not supported"

    position_key_format = manifest.get("position_key_format")
    if position_key_format != SUPPORTED_BUNDLE_POSITION_KEY_FORMAT:
        return False, None, f"unsupported position_key_format {position_key_format!r}"

    move_key_format = manifest.get("move_key_format")
    if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
        return False, None, f"unsupported move_key_format {move_key_format!r}"

    payload_resolution, resolution_error = resolve_bundle_payload(manifest, bundle_dir)
    if payload_resolution is None:
        return False, None, resolution_error or "unsupported bundle payload"
    inspection_lease = payload_resolution.mounted_sqlite_lease

    payload_status = manifest.get("payload_status")
    if payload_resolution.payload_format == "jsonl":
        if not (aggregate_payload_exposes_raw_counts(payload_resolution.payload_path) or payload_status_mentions_counts(payload_status)):
            return False, payload_resolution.payload_path, "aggregate payload does not expose raw counts required by the trainer runtime"
    if inspection_lease is not None:
        inspection_lease.release()
    return True, payload_resolution.payload_path, "supported builder aggregate bundle"


def resolve_timing_conditioned_exact_payload(manifest: dict[str, object], bundle_dir: Path) -> tuple[BundlePayloadResolution | None, str | None]:
    declared_canonical = manifest_declared_canonical_exact_payload_path(manifest, bundle_dir)
    declared_compatibility = manifest_declared_compatibility_exact_payload_path(manifest, bundle_dir)
    declared_sqlite = manifest_declared_exact_sqlite_path(manifest, bundle_dir)
    if declared_canonical is not None:
        resolution, error = _resolve_sqlite_payload(declared_canonical, manifest)
        if resolution is None:
            return None, error
        return resolution, None
    candidate_paths: list[Path] = []
    if declared_compatibility is not None:
        candidate_paths.append(declared_compatibility)
    if declared_sqlite is not None:
        candidate_paths.append(declared_sqlite)
    candidate_paths.extend(
        [
            bundle_dir / BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH,
            bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH,
        ]
    )
    for candidate in candidate_paths:
        resolution, error = _resolve_sqlite_payload(candidate, manifest, strict=False)
        if resolution is not None:
            return resolution, None
        if error and "not a file" in error:
            return None, error
    return None, "timing-conditioned bundle did not expose a supported exact SQLite payload"


def _resolve_sqlite_payload(
    requested_path: Path,
    manifest: dict[str, object],
    *,
    strict: bool = True,
) -> tuple[BundlePayloadResolution | None, str | None]:
    manager = get_mounted_sqlite_manager()
    try:
        resolution, lease = manager.resolve(requested_path)
    except SQLitePayloadResolutionError as exc:
        if not strict and exc.code == "sqlite_payload_missing":
            return None, None
        return None, exc.detail
    return (
        BundlePayloadResolution(
            payload_format="sqlite",
            payload_path=resolution.active_path,
            payload_version=manifest_payload_version(manifest),
            requested_path=resolution.requested_path,
            used_plain_sqlite=resolution.used_plain_sqlite,
            used_compressed_sqlite=resolution.used_compressed_sqlite,
            mounted_sqlite_path=resolution.mounted_path,
            mounted_sqlite_lease=lease,
        ),
        None,
    )


def is_supported_timing_conditioned_bundle(manifest: dict[str, object], bundle_dir: Path) -> tuple[bool, Path | None, str]:
    payload_resolution, error = resolve_timing_conditioned_exact_payload(manifest, bundle_dir)
    if payload_resolution is None:
        return False, None, error or "missing exact payload"
    inspection_lease = payload_resolution.mounted_sqlite_lease
    if inspection_lease is not None:
        inspection_lease.release()
    return True, payload_resolution.payload_path, "supported timing-conditioned bundle"


def classify_bundle_contract(manifest: dict[str, object]) -> str:
    timing_keys = (
        "timing_overlay",
        "timing_overlay_file",
        "timing_overlay_payload_file",
        "behavioral_profile_set_file",
        "behavioral_profile_set_sqlite_file",
        "timing_overlay_policy_version",
        "context_contract_version",
        "time_control_scope",
        "rating_scope",
        "exact_corpus_file",
        "exact_corpus_payload_file",
        "canonical_exact_payload_file",
        "compatibility_exact_payload_file",
        "payload_version",
    )
    if any(key in manifest for key in timing_keys):
        return "timing_conditioned"
    if manifest.get("build_status") == "aggregation_complete":
        return "legacy_aggregate"
    return "unknown"
