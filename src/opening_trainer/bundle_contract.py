from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

SUPPORTED_BUNDLE_POSITION_KEY_FORMAT = "fen_normalized"
SUPPORTED_BUNDLE_MOVE_KEY_FORMAT = "uci"
BUNDLE_MANIFEST_NAME = "manifest.json"
BUNDLE_AGGREGATE_RELATIVE_PATH = Path("data/aggregated_position_move_counts.jsonl")
BUNDLE_SQLITE_RELATIVE_PATH = Path("data/corpus.sqlite")
SUPPORTED_BUNDLE_PAYLOAD_FORMATS = {"jsonl", "sqlite"}


@dataclass(frozen=True)
class BundlePayloadResolution:
    payload_format: str
    payload_path: Path


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
            if not sqlite_path.exists():
                return None, f"sqlite payload is missing at {sqlite_path}"
            if not sqlite_path.is_file():
                return None, f"sqlite payload path {sqlite_path} is not a file"
            return BundlePayloadResolution(payload_format="sqlite", payload_path=sqlite_path), None
        aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir) or (bundle_dir / BUNDLE_AGGREGATE_RELATIVE_PATH)
        if not aggregate_path.exists():
            return None, f"aggregate payload is missing at {aggregate_path}"
        if not aggregate_path.is_file():
            return None, f"aggregate payload path {aggregate_path} is not a file"
        return BundlePayloadResolution(payload_format="jsonl", payload_path=aggregate_path), None

    sqlite_default_path = bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH
    if sqlite_default_path.exists() and sqlite_default_path.is_file():
        return BundlePayloadResolution(payload_format="sqlite", payload_path=sqlite_default_path), None

    declared_aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir)
    if declared_aggregate_path is not None:
        if not declared_aggregate_path.exists():
            return None, f"aggregate payload is missing at {declared_aggregate_path}"
        if not declared_aggregate_path.is_file():
            return None, f"aggregate payload path {declared_aggregate_path} is not a file"
        return BundlePayloadResolution(payload_format="jsonl", payload_path=declared_aggregate_path), None

    aggregate_path = bundle_dir / BUNDLE_AGGREGATE_RELATIVE_PATH
    if aggregate_path.exists() and aggregate_path.is_file():
        return BundlePayloadResolution(payload_format="jsonl", payload_path=aggregate_path), None

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

    payload_status = manifest.get("payload_status")
    if payload_resolution.payload_format == "jsonl":
        if not (aggregate_payload_exposes_raw_counts(payload_resolution.payload_path) or payload_status_mentions_counts(payload_status)):
            return False, payload_resolution.payload_path, "aggregate payload does not expose raw counts required by the trainer runtime"
    return True, payload_resolution.payload_path, "supported builder aggregate bundle"
