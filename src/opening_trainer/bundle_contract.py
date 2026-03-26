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
BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH = Path("data/exact_corpus.sqlite")
BUNDLE_EXACT_COMPACT_V2_DEFAULT_RELATIVE_PATH = Path("data/exact_compact_corpus.sqlite")
BUNDLE_BEHAVIORAL_PROFILE_SET_DEFAULT_RELATIVE_PATH = Path("data/behavioral_profile_set.sqlite")


@dataclass(frozen=True)
class BundlePayloadResolution:
    payload_format: str
    payload_path: Path
    payload_version: str | None = None
    payload_role: str | None = None


@dataclass(frozen=True)
class CorpusMetadataContract:
    retained_ply_depth: int | None
    max_supported_player_moves: int | None
    time_control_id: str | None
    initial_time_seconds: float | None
    increment_seconds: float | None
    time_format_label: str | None
    target_rating_band: str | None
    rating_policy: str | None
    payload_version: str | None
    payload_role: str | None
    is_canonical_contract: bool


def manifest_declared_aggregate_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    declared_path = manifest.get("aggregate_position_file")
    if not isinstance(declared_path, str) or not declared_path.strip():
        return None
    return bundle_dir / Path(declared_path)


def manifest_declared_sqlite_path(manifest: dict[str, object], bundle_dir: Path) -> Path | None:
    for key in ("sqlite_corpus_file", "corpus_sqlite_file", "payload_file", "exact_corpus_file", "exact_payload_file"):
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


def _coerce_non_empty_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _coerce_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _coerce_float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _format_rating_band(value: object) -> str | None:
    if isinstance(value, dict):
        minimum = value.get("minimum")
        maximum = value.get("maximum")
        if minimum is not None and maximum is not None:
            return f"{minimum}-{maximum}"
    return _coerce_non_empty_string(value)


def read_corpus_metadata_contract(manifest: dict[str, object]) -> CorpusMetadataContract:
    retained_ply_depth = _coerce_int(
        manifest.get("retained_ply_depth")
        or manifest.get("retained_opening_ply_depth")
        or manifest.get("opening_retained_ply_depth")
        or manifest.get("max_retained_ply_depth")
    )
    max_supported_player_moves = _coerce_int(
        manifest.get("max_supported_player_moves")
        or manifest.get("max_supported_training_depth")
    )
    time_control_id = _coerce_non_empty_string(manifest.get("time_control_id"))
    initial_time_seconds = _coerce_float(manifest.get("initial_time_seconds") or manifest.get("initial_seconds"))
    increment_seconds = _coerce_float(manifest.get("increment_seconds"))
    time_format_label = _coerce_non_empty_string(manifest.get("time_format_label"))
    target_rating_band = _format_rating_band(manifest.get("target_rating_band") or manifest.get("rating_band") or manifest.get("elo_band"))
    rating_policy = _coerce_non_empty_string(manifest.get("rating_policy"))
    payload_version = _coerce_non_empty_string(manifest.get("payload_version") or manifest.get("exact_payload_version"))
    payload_role = _coerce_non_empty_string(manifest.get("payload_role") or manifest.get("exact_payload_role"))
    is_canonical_contract = payload_version == "exact_compact_v2" or payload_role == "canonical"
    return CorpusMetadataContract(
        retained_ply_depth=retained_ply_depth,
        max_supported_player_moves=max_supported_player_moves,
        time_control_id=time_control_id,
        initial_time_seconds=initial_time_seconds,
        increment_seconds=increment_seconds,
        time_format_label=time_format_label,
        target_rating_band=target_rating_band,
        rating_policy=rating_policy,
        payload_version=payload_version,
        payload_role=payload_role,
        is_canonical_contract=is_canonical_contract,
    )


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


def resolve_timing_conditioned_exact_payload(manifest: dict[str, object], bundle_dir: Path) -> tuple[BundlePayloadResolution | None, str | None]:
    exact_payloads = manifest.get("exact_payloads")
    if isinstance(exact_payloads, list):
        ranked: list[tuple[int, BundlePayloadResolution]] = []
        for entry in exact_payloads:
            if not isinstance(entry, dict):
                continue
            payload_file = entry.get("payload_file") or entry.get("path") or entry.get("exact_corpus_file")
            if not isinstance(payload_file, str) or not payload_file.strip():
                continue
            payload_format = str(entry.get("payload_format", "sqlite")).strip().lower()
            if payload_format != "sqlite":
                continue
            payload_path = bundle_dir / Path(payload_file)
            if not payload_path.exists():
                continue
            if not payload_path.is_file():
                return None, f"exact corpus payload path {payload_path} is not a file"
            payload_version = _coerce_non_empty_string(entry.get("payload_version"))
            payload_role = _coerce_non_empty_string(entry.get("payload_role"))
            score = 0
            if payload_role == "canonical":
                score += 10
            if payload_version == "exact_compact_v2":
                score += 5
            ranked.append(
                (
                    score,
                    BundlePayloadResolution(
                        payload_format="sqlite",
                        payload_path=payload_path,
                        payload_version=payload_version,
                        payload_role=payload_role,
                    ),
                )
            )
        if ranked:
            ranked.sort(key=lambda item: item[0], reverse=True)
            return ranked[0][1], None

    declared_sqlite = manifest_declared_exact_sqlite_path(manifest, bundle_dir)
    candidate_paths: list[Path] = []
    if declared_sqlite is not None:
        candidate_paths.append(declared_sqlite)
    candidate_paths.extend(
        [
            bundle_dir / BUNDLE_EXACT_COMPACT_V2_DEFAULT_RELATIVE_PATH,
            bundle_dir / BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH,
            bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH,
        ]
    )
    for candidate in candidate_paths:
        if not candidate.exists():
            continue
        if not candidate.is_file():
            return None, f"exact corpus payload path {candidate} is not a file"
        contract = read_corpus_metadata_contract(manifest)
        return BundlePayloadResolution(
            payload_format="sqlite",
            payload_path=candidate,
            payload_version=contract.payload_version,
            payload_role=contract.payload_role,
        ), None
    return None, "timing-conditioned bundle did not expose a supported exact SQLite payload"


def is_supported_timing_conditioned_bundle(manifest: dict[str, object], bundle_dir: Path) -> tuple[bool, Path | None, str]:
    payload_resolution, error = resolve_timing_conditioned_exact_payload(manifest, bundle_dir)
    if payload_resolution is None:
        return False, None, error or "missing exact payload"
    return True, payload_resolution.payload_path, "supported timing-conditioned bundle"


def classify_bundle_contract(manifest: dict[str, object]) -> str:
    contract = read_corpus_metadata_contract(manifest)
    if contract.payload_version == "exact_compact_v2":
        return "timing_conditioned_compact_v2"
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
    )
    if any(key in manifest for key in timing_keys):
        return "timing_conditioned"
    if manifest.get("build_status") == "aggregation_complete":
        return "legacy_aggregate"
    return "unknown"
