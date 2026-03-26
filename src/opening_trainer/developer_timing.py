from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_DEVELOPER_TIMING_FILENAME = "developer_timing_overrides.json"


@dataclass(frozen=True)
class DeveloperTimingOverrideState:
    enabled: bool = False
    force_time_control_id: str = "Auto"
    force_mover_elo_band: str = "Auto"
    force_clock_pressure_bucket: str = "Auto"
    force_prev_opp_think_bucket: str = "Auto"
    force_opening_ply_band: str = "Auto"
    force_ordinary_corpus_play: bool = False
    visible_delay_scale: float = 1.0
    visible_delay_min_seconds: float | None = None
    visible_delay_max_seconds: float | None = None

    def normalized(self) -> "DeveloperTimingOverrideState":
        return DeveloperTimingOverrideState(
            enabled=bool(self.enabled),
            force_time_control_id=_normalize_text_or_auto(self.force_time_control_id),
            force_mover_elo_band=_normalize_text_or_auto(self.force_mover_elo_band),
            force_clock_pressure_bucket=_normalize_text_or_auto(self.force_clock_pressure_bucket),
            force_prev_opp_think_bucket=_normalize_text_or_auto(self.force_prev_opp_think_bucket),
            force_opening_ply_band=_normalize_text_or_auto(self.force_opening_ply_band),
            force_ordinary_corpus_play=bool(self.force_ordinary_corpus_play),
            visible_delay_scale=max(0.0, float(self.visible_delay_scale)),
            visible_delay_min_seconds=_normalize_optional_float(self.visible_delay_min_seconds),
            visible_delay_max_seconds=_normalize_optional_float(self.visible_delay_max_seconds),
        )

    @classmethod
    def disabled_defaults(cls) -> "DeveloperTimingOverrideState":
        return cls().normalized()


@dataclass(frozen=True)
class LiveTimingDebugState:
    bundle_path: str | None = None
    overlay_available: bool = False
    overlay_source: str = "absent"
    raw_runtime_context_components: dict[str, object] | None = None
    effective_context_key: str | None = None
    fallback_keys_attempted: tuple[str, ...] = ()
    matched_context_key: str | None = None
    fallback_used: bool = False
    move_pressure_profile_id: str | None = None
    think_time_profile_id: str | None = None
    sampled_think_time_seconds: float | None = None
    visible_delay_applied_seconds: float | None = None
    visible_delay_reason: str = "none"
    last_opponent_source: str | None = None
    review_predecessor_bypassed: bool = False


class DeveloperTimingOverrideStore:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / DEFAULT_DEVELOPER_TIMING_FILENAME

    def load(self) -> DeveloperTimingOverrideState:
        if not self.path.exists():
            return DeveloperTimingOverrideState.disabled_defaults()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return DeveloperTimingOverrideState.disabled_defaults()
        settings = DeveloperTimingOverrideState(
            enabled=bool(payload.get("enabled", False)),
            force_time_control_id=str(payload.get("force_time_control_id", "Auto")),
            force_mover_elo_band=str(payload.get("force_mover_elo_band", "Auto")),
            force_clock_pressure_bucket=str(payload.get("force_clock_pressure_bucket", "Auto")),
            force_prev_opp_think_bucket=str(payload.get("force_prev_opp_think_bucket", "Auto")),
            force_opening_ply_band=str(payload.get("force_opening_ply_band", "Auto")),
            force_ordinary_corpus_play=bool(payload.get("force_ordinary_corpus_play", False)),
            visible_delay_scale=float(payload.get("visible_delay_scale", 1.0)),
            visible_delay_min_seconds=payload.get("visible_delay_min_seconds"),
            visible_delay_max_seconds=payload.get("visible_delay_max_seconds"),
        )
        normalized = settings.normalized()
        if normalized != settings:
            self.save(normalized)
        return normalized

    def save(self, settings: DeveloperTimingOverrideState) -> DeveloperTimingOverrideState:
        normalized = settings.normalized()
        self.path.write_text(json.dumps(asdict(normalized), indent=2), encoding="utf-8")
        return normalized


def parse_overlay_key_dimensions(context_keys: list[str]) -> dict[str, list[str]]:
    dimensions = {
        "time_control_id": set(),
        "mover_elo_band": set(),
        "clock_pressure_bucket": set(),
        "prev_opp_think_bucket": set(),
        "opening_ply_band": set(),
    }
    for key in context_keys:
        tokens = [part.strip() for part in str(key).split("|")]
        if len(tokens) < 5:
            continue
        dimensions["time_control_id"].add(tokens[0])
        dimensions["mover_elo_band"].add(tokens[1])
        dimensions["clock_pressure_bucket"].add(tokens[2])
        dimensions["prev_opp_think_bucket"].add(tokens[3])
        dimensions["opening_ply_band"].add(tokens[4])
    return {name: sorted(values) for name, values in dimensions.items()}


def _normalize_text_or_auto(value: object) -> str:
    text = str(value or "").strip()
    return text if text else "Auto"


def _normalize_optional_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return max(0.0, float(text))
