from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from pathlib import Path

from .bundle_contract import BUNDLE_MANIFEST_NAME
from .bundle_corpus import BuilderAggregateCorpusProvider


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

    @property
    def timing_overlay_available(self) -> bool:
        return self.overlay is not None

    def lookup_position(self, position_key: str):
        return self.exact_corpus.lookup_position(position_key)

    def resolve_overlay(self, context: TimingContext) -> OverlayResolution | None:
        if self.overlay is None:
            return None
        fallback_keys = _fallback_keys(context)
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
            )
        return None


class TimingConditionedCorpusBundleLoader:
    def load(self, bundle_dir: str | Path, rng=None) -> TimingConditionedCorpusBundleHandle:
        local_dir = Path(bundle_dir)
        manifest = json.loads((local_dir / BUNDLE_MANIFEST_NAME).read_text(encoding="utf-8"))
        exact = BuilderAggregateCorpusProvider(local_dir, rng=rng)
        overlay = _load_timing_overlay(local_dir, manifest)
        return TimingConditionedCorpusBundleHandle(bundle_dir=local_dir, manifest=manifest, exact_corpus=exact, overlay=overlay)


def _load_timing_overlay(bundle_dir: Path, manifest: dict[str, object]) -> TimingOverlayPayload | None:
    overlay_payload = manifest.get("timing_overlay")
    if isinstance(overlay_payload, dict):
        return _parse_overlay_payload(overlay_payload)
    overlay_file = manifest.get("timing_overlay_file") or manifest.get("timing_overlay_payload_file")
    if isinstance(overlay_file, str) and overlay_file.strip():
        overlay_path = bundle_dir / Path(overlay_file)
        if overlay_path.exists():
            payload = json.loads(overlay_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return _parse_overlay_payload(payload)
    return None


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


def _fallback_keys(context: TimingContext) -> list[str]:
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
