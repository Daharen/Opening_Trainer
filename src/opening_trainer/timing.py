from __future__ import annotations

import json
import math
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .bundle_contract import (
    BUNDLE_BEHAVIORAL_PROFILE_SET_DEFAULT_RELATIVE_PATH,
    BUNDLE_MANIFEST_NAME,
    classify_bundle_contract,
    manifest_declared_behavioral_profile_set_path,
    resolve_bundle_payload,
    resolve_timing_conditioned_exact_payload,
)
from .bundle_corpus import BuilderAggregateCorpusProvider, JsonlAggregateCorpusProvider, SQLiteAggregateCorpusProvider


@dataclass(frozen=True)
class MovePressureProfile:
    profile_id: str
    pressure_sensitivity: float
    decisiveness: float
    move_diversity: float


@dataclass(frozen=True)
class ThinkTimeProfile:
    profile_id: str
    base_time_scale: float
    spread: float
    short_mass: float
    deep_think_tail_mass: float
    timeout_tail_mass: float


@dataclass(frozen=True)
class TimingContext:
    time_control_id: str
    mover_elo_band: str
    clock_pressure_bucket: str
    prev_opp_think_bucket: str
    opening_ply_band: str

    def key(self) -> str:
        return "|".join(
            [
                self.time_control_id,
                self.mover_elo_band,
                self.clock_pressure_bucket,
                self.prev_opp_think_bucket,
                self.opening_ply_band,
            ]
        )


@dataclass(frozen=True)
class OverlayResolution:
    move_pressure_profile: MovePressureProfile
    think_time_profile: ThinkTimeProfile
    matched_key: str
    fallback_used: bool
    attempted_key: str
    fallback_keys: tuple[str, ...]


@dataclass(frozen=True)
class TimingOverlayPayload:
    context_profile_map: dict[str, dict[str, str]]
    move_pressure_profiles: dict[str, MovePressureProfile]
    think_time_profiles: dict[str, ThinkTimeProfile]
    context_contract_version: str | None
    timing_overlay_policy_version: str | None


@dataclass(frozen=True)
class TimingConditionedCorpusBundleHandle:
    bundle_dir: Path
    manifest: dict[str, object]
    exact_corpus: BuilderAggregateCorpusProvider
    overlay: TimingOverlayPayload | None
    bundle_kind: str
    exact_payload_path: Path | None
    overlay_source: str

    @property
    def timing_overlay_available(self) -> bool:
        return self.overlay is not None

    def lookup_position(self, position_key: str):
        return self.exact_corpus.lookup_position(position_key)

    def resolve_overlay(self, context: TimingContext) -> OverlayResolution | None:
        if self.overlay is None:
            return None
        fallback_keys = fallback_keys_for_context(context)
        for index, key in enumerate(fallback_keys):
            profile_ids = self.overlay.context_profile_map.get(key)
            if not isinstance(profile_ids, dict):
                continue
            move_profile_id = profile_ids.get("move_pressure_profile_id")
            think_profile_id = profile_ids.get("think_time_profile_id")
            if not move_profile_id or not think_profile_id:
                continue
            move_profile = self.overlay.move_pressure_profiles.get(move_profile_id)
            think_profile = self.overlay.think_time_profiles.get(think_profile_id)
            if move_profile is None or think_profile is None:
                continue
            return OverlayResolution(
                move_pressure_profile=move_profile,
                think_time_profile=think_profile,
                matched_key=key,
                fallback_used=index > 0,
                attempted_key=context.key(),
                fallback_keys=tuple(fallback_keys),
            )
        return None


class TimingConditionedCorpusBundleLoader:
    def load(self, bundle_dir: str | Path, rng=None) -> TimingConditionedCorpusBundleHandle:
        local_dir = Path(bundle_dir)
        manifest = json.loads((local_dir / BUNDLE_MANIFEST_NAME).read_text(encoding="utf-8"))
        exact, bundle_kind, exact_payload_path = _load_exact_corpus_provider(local_dir, manifest, rng=rng)
        overlay, overlay_source = _load_timing_overlay(local_dir, manifest)
        return TimingConditionedCorpusBundleHandle(
            bundle_dir=local_dir,
            manifest=manifest,
            exact_corpus=exact,
            overlay=overlay,
            bundle_kind=bundle_kind,
            exact_payload_path=exact_payload_path,
            overlay_source=overlay_source,
        )


def _load_exact_corpus_provider(bundle_dir: Path, manifest: dict[str, object], rng=None) -> tuple[BuilderAggregateCorpusProvider, str, Path | None]:
    bundle_kind = classify_bundle_contract(manifest)
    if bundle_kind == "legacy_aggregate":
        provider = BuilderAggregateCorpusProvider(bundle_dir, rng=rng)
        return provider, bundle_kind, provider.metadata.aggregate_path

    payload_resolution, error = resolve_timing_conditioned_exact_payload(manifest, bundle_dir)
    if payload_resolution is None:
        legacy_resolution, legacy_error = resolve_bundle_payload(manifest, bundle_dir)
        if legacy_resolution is None:
            raise ValueError(error or legacy_error or "unsupported exact payload for timing-conditioned bundle")
        payload_resolution = legacy_resolution

    manifest_for_provider = {
        **manifest,
        "position_key_format": manifest.get("position_key_format", "fen_normalized"),
        "move_key_format": manifest.get("move_key_format", "uci"),
        "payload_status": manifest.get("payload_status", "timing_conditioned_exact_payload"),
    }
    if payload_resolution.payload_format == "sqlite":
        return SQLiteAggregateCorpusProvider(bundle_dir, manifest_for_provider, payload_resolution.payload_path), "timing_conditioned", payload_resolution.payload_path
    return JsonlAggregateCorpusProvider(bundle_dir, manifest_for_provider, payload_resolution.payload_path, rng=rng), "timing_conditioned", payload_resolution.payload_path


def _load_timing_overlay(bundle_dir: Path, manifest: dict[str, object]) -> tuple[TimingOverlayPayload | None, str]:
    overlay_payload = manifest.get("timing_overlay")
    if isinstance(overlay_payload, dict):
        return _parse_overlay_payload(overlay_payload), "inline_json"
    overlay_file = manifest.get("timing_overlay_file") or manifest.get("timing_overlay_payload_file")
    if isinstance(overlay_file, str) and overlay_file.strip():
        overlay_path = bundle_dir / Path(overlay_file)
        if overlay_path.exists():
            payload = json.loads(overlay_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return _parse_overlay_payload(payload), "json_file"
    sqlite_overlay = _load_overlay_from_behavioral_profile_set_sqlite(bundle_dir, manifest)
    if sqlite_overlay is not None:
        return sqlite_overlay, "behavioral_profile_set_sqlite"
    return None, "absent"


def _load_overlay_from_behavioral_profile_set_sqlite(bundle_dir: Path, manifest: dict[str, object]) -> TimingOverlayPayload | None:
    declared = manifest_declared_behavioral_profile_set_path(manifest, bundle_dir)
    candidate_paths = [declared] if declared is not None else []
    candidate_paths.append(bundle_dir / BUNDLE_BEHAVIORAL_PROFILE_SET_DEFAULT_RELATIVE_PATH)
    for candidate in candidate_paths:
        if candidate is None or not candidate.exists() or not candidate.is_file():
            continue
        connection = sqlite3.connect(f"file:{candidate}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            return _parse_overlay_sqlite_connection(connection, manifest)
        finally:
            connection.close()
    return None


def _parse_overlay_sqlite_connection(connection: sqlite3.Connection, manifest: dict[str, object]) -> TimingOverlayPayload:
    move_profiles: dict[str, MovePressureProfile] = {}
    think_profiles: dict[str, ThinkTimeProfile] = {}
    context_profile_map: dict[str, dict[str, str]] = {}
    for row in connection.execute(
        "SELECT profile_id, pressure_sensitivity, decisiveness, move_diversity FROM move_pressure_profiles ORDER BY profile_id ASC"
    ).fetchall():
        profile_id = str(row["profile_id"])
        move_profiles[profile_id] = MovePressureProfile(
            profile_id=profile_id,
            pressure_sensitivity=float(row["pressure_sensitivity"] or 0.0),
            decisiveness=float(row["decisiveness"] or 0.0),
            move_diversity=float(row["move_diversity"] or 0.1),
        )
    for row in connection.execute(
        "SELECT profile_id, base_time_scale, spread, short_mass, deep_think_tail_mass, timeout_tail_mass FROM think_time_profiles ORDER BY profile_id ASC"
    ).fetchall():
        profile_id = str(row["profile_id"])
        think_profiles[profile_id] = ThinkTimeProfile(
            profile_id=profile_id,
            base_time_scale=float(row["base_time_scale"] or 1.0),
            spread=float(row["spread"] or 1.0),
            short_mass=float(row["short_mass"] or 0.2),
            deep_think_tail_mass=float(row["deep_think_tail_mass"] or 0.1),
            timeout_tail_mass=float(row["timeout_tail_mass"] or 0.0),
        )
    context_rows = connection.execute("SELECT * FROM context_profile_map ORDER BY context_key ASC").fetchall()
    for row in context_rows:
        context_key = row["context_key"] if "context_key" in row.keys() else None
        if not context_key:
            context_key = "|".join(
                [
                    str(row["time_control_id"]),
                    str(row["mover_elo_band"]),
                    str(row["clock_pressure_bucket"]),
                    str(row["prev_opp_think_bucket"]),
                    str(row["opening_ply_band"]),
                ]
            )
        context_profile_map[str(context_key)] = {
            "move_pressure_profile_id": str(row["move_pressure_profile_id"]),
            "think_time_profile_id": str(row["think_time_profile_id"]),
        }
    return TimingOverlayPayload(
        context_profile_map=context_profile_map,
        move_pressure_profiles=move_profiles,
        think_time_profiles=think_profiles,
        context_contract_version=str(manifest.get("context_contract_version")) if manifest.get("context_contract_version") is not None else None,
        timing_overlay_policy_version=str(manifest.get("timing_overlay_policy_version")) if manifest.get("timing_overlay_policy_version") is not None else None,
    )


def _parse_overlay_payload(payload: dict[str, object]) -> TimingOverlayPayload:
    context_profile_map = payload.get("context_profile_map") or {}
    move_pressure_profiles = payload.get("move_pressure_profiles") or {}
    think_time_profiles = payload.get("think_time_profiles") or {}
    parsed_move_profiles: dict[str, MovePressureProfile] = {}
    parsed_think_profiles: dict[str, ThinkTimeProfile] = {}

    if isinstance(move_pressure_profiles, dict):
        for profile_id, profile in move_pressure_profiles.items():
            if not isinstance(profile, dict):
                continue
            parsed_move_profiles[str(profile_id)] = MovePressureProfile(
                profile_id=str(profile_id),
                pressure_sensitivity=float(profile.get("pressure_sensitivity", 0.0)),
                decisiveness=float(profile.get("decisiveness", 0.0)),
                move_diversity=float(profile.get("move_diversity", 0.1)),
            )
    if isinstance(think_time_profiles, dict):
        for profile_id, profile in think_time_profiles.items():
            if not isinstance(profile, dict):
                continue
            parsed_think_profiles[str(profile_id)] = ThinkTimeProfile(
                profile_id=str(profile_id),
                base_time_scale=float(profile.get("base_time_scale", 1.0)),
                spread=float(profile.get("spread", 1.0)),
                short_mass=float(profile.get("short_mass", 0.2)),
                deep_think_tail_mass=float(profile.get("deep_think_tail_mass", 0.1)),
                timeout_tail_mass=float(profile.get("timeout_tail_mass", 0.0)),
            )
    return TimingOverlayPayload(
        context_profile_map={str(key): value for key, value in context_profile_map.items() if isinstance(value, dict)},
        move_pressure_profiles=parsed_move_profiles,
        think_time_profiles=parsed_think_profiles,
        context_contract_version=str(payload.get("context_contract_version")) if payload.get("context_contract_version") is not None else None,
        timing_overlay_policy_version=str(payload.get("timing_overlay_policy_version")) if payload.get("timing_overlay_policy_version") is not None else None,
    )


def bucket_clock_pressure(remaining_ratio: float) -> str:
    if remaining_ratio < 0.10:
        return "critical"
    if remaining_ratio < 0.25:
        return "low"
    if remaining_ratio < 0.50:
        return "medium"
    return "comfortable"


def bucket_prev_opp_think(seconds: float | None) -> str:
    if seconds is None:
        return "none"
    if seconds < 2.0:
        return "instant"
    if seconds < 10.0:
        return "short"
    if seconds < 30.0:
        return "medium"
    return "long"


def bucket_opening_ply_band(ply_count: int) -> str:
    if ply_count <= 10:
        return "01-10"
    if ply_count <= 20:
        return "11-20"
    if ply_count <= 30:
        return "21-30"
    return "31+"


def apply_move_pressure_modulation(candidate_weights: list[tuple[str, float]], profile: MovePressureProfile, clock_pressure_bucket: str) -> tuple[list[tuple[str, float]], dict[str, float]]:
    bucket_scalar_map = {"comfortable": 0.0, "medium": 0.33, "low": 0.66, "critical": 1.0}
    bucket_scalar = bucket_scalar_map.get(clock_pressure_bucket, 0.0)
    strength = _clamp(profile.pressure_sensitivity * 100.0 * bucket_scalar, 0.0, 1.0)
    rank_sharpness = 0.5 + 3.0 * profile.decisiveness
    tail_floor = _clamp(profile.move_diversity, 0.02, 0.50)

    ranked = sorted(candidate_weights, key=lambda item: item[1], reverse=True)
    adjusted: list[tuple[str, float]] = []
    for rank, (uci, weight) in enumerate(ranked):
        rank_curve = math.exp(-rank_sharpness * rank)
        rank_modifier = tail_floor + (1.0 - tail_floor) * rank_curve
        adjusted_weight = max(0.0, float(weight)) * _lerp(1.0, rank_modifier, strength)
        adjusted.append((uci, adjusted_weight))
    total = sum(weight for _uci, weight in adjusted)
    if total > 0:
        adjusted = [(uci, weight / total) for uci, weight in adjusted]
    return adjusted, {
        "strength": strength,
        "rank_sharpness": rank_sharpness,
        "tail_floor": tail_floor,
        "bucket_scalar": bucket_scalar,
    }


def sample_think_time_seconds(profile: ThinkTimeProfile, remaining_time_seconds: float, rng=None) -> float:
    rng = rng or random
    u = rng.random()
    timeout_edge = profile.timeout_tail_mass
    short_edge = timeout_edge + profile.short_mass
    deep_edge = short_edge + profile.deep_think_tail_mass

    if u < timeout_edge:
        sample = remaining_time_seconds * rng.uniform(0.90, 1.05)
    elif u < short_edge:
        sample = rng.uniform(0.0, max(0.25, min(2.0, profile.base_time_scale)))
    elif u < deep_edge:
        sample = profile.base_time_scale + rng.expovariate(1.0 / max(0.25, profile.spread))
    else:
        stddev = max(0.25, profile.spread * 0.35)
        sample = max(0.0, rng.gauss(profile.base_time_scale, stddev))
    return max(0.0, sample)


def fallback_keys_for_context(context: TimingContext) -> list[str]:
    return [
        context.key(),
        TimingContext(context.time_control_id, context.mover_elo_band, context.clock_pressure_bucket, "none", context.opening_ply_band).key(),
        "|".join([context.time_control_id, context.mover_elo_band, context.clock_pressure_bucket, context.opening_ply_band]),
        "|".join([context.time_control_id, context.mover_elo_band, context.clock_pressure_bucket]),
        "|".join([context.time_control_id, context.mover_elo_band]),
    ]


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t
