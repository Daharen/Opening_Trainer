from __future__ import annotations

import json
from pathlib import Path

SUPPORTED_BUNDLE_POSITION_KEY_FORMAT = "fen_normalized"
SUPPORTED_BUNDLE_MOVE_KEY_FORMAT = "uci"
BUNDLE_MANIFEST_NAME = "manifest.json"
BUNDLE_AGGREGATE_RELATIVE_PATH = Path("data/aggregated_position_move_counts.jsonl")


def manifest_declared_aggregate_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    declared_path = manifest.get("aggregate_position_file")
    if not isinstance(declared_path, str) or not declared_path.strip():
        return None
    return bundle_dir / Path(declared_path)


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


def is_supported_builder_aggregate_bundle(manifest: dict[str, object], bundle_dir: Path) -> tuple[bool, Path | None, str]:
    build_status = manifest.get("build_status")
    if build_status != "aggregation_complete":
        return False, None, f"build_status {build_status!r} is not supported"

    aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir)
    if aggregate_path is None:
        return False, None, "aggregate_position_file is missing from manifest"
    if not aggregate_path.exists():
        return False, aggregate_path, f"aggregate payload is missing at {aggregate_path}"
    if not aggregate_path.is_file():
        return False, aggregate_path, f"aggregate payload path {aggregate_path} is not a file"

    position_key_format = manifest.get("position_key_format")
    if position_key_format != SUPPORTED_BUNDLE_POSITION_KEY_FORMAT:
        return False, aggregate_path, f"unsupported position_key_format {position_key_format!r}"

    move_key_format = manifest.get("move_key_format")
    if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
        return False, aggregate_path, f"unsupported move_key_format {move_key_format!r}"

    payload_status = manifest.get("payload_status")
    if not (aggregate_payload_exposes_raw_counts(aggregate_path) or payload_status_mentions_counts(payload_status)):
        return False, aggregate_path, "aggregate payload does not expose raw counts required by the trainer runtime"

    return True, aggregate_path, "supported builder aggregate bundle"
