from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import inf
from typing import Any

from .review.storage import ReviewStorage

SUPPORTED_TRACK_BY_TIME_CONTROL = {
    "600+0": ("rapid", "600+0"),
    "300+0": ("blitz", "300+0"),
    "120+1": ("bullet", "120+1"),
}

ORDINARY_ONLY_ROUTING_SOURCES = {"ordinary_corpus_play"}


@dataclass(frozen=True)
class OpponentTarget:
    kind: str
    rating_min: int | None = None
    rating_max: int | None = None


@dataclass(frozen=True)
class SmartProfileLevelContract:
    level: int
    opponent_target: OpponentTarget
    turns_to_succeed: int
    good_accepted: bool
    game_successes_to_promote: int | float
    game_failures_to_demote: int

    @property
    def is_stockfish_tier(self) -> bool:
        return self.opponent_target.kind == "stockfish"


@dataclass
class SmartProfileTrackState:
    track_id: str
    time_control_category_id: str
    current_level: int = 1
    wins_toward_promotion: int = 0
    losses_toward_demotion: int = 0
    eligible_games_played: int = 0
    last_eligible_result: str | None = None
    last_eligible_bundle_time_control_id: str | None = None
    last_eligible_bundle_rating_band: str | None = None
    last_updated_at_utc: str | None = None
    placement_seed_complete: bool = False
    placement_games_remaining_hint: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "track_id": self.track_id,
            "time_control_category_id": self.time_control_category_id,
            "current_level": self.current_level,
            "wins_toward_promotion": self.wins_toward_promotion,
            "losses_toward_demotion": self.losses_toward_demotion,
            "eligible_games_played": self.eligible_games_played,
            "last_eligible_result": self.last_eligible_result,
            "last_eligible_bundle_time_control_id": self.last_eligible_bundle_time_control_id,
            "last_eligible_bundle_rating_band": self.last_eligible_bundle_rating_band,
            "last_updated_at_utc": self.last_updated_at_utc,
            "placement_seed_complete": self.placement_seed_complete,
            "placement_games_remaining_hint": self.placement_games_remaining_hint,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SmartProfileTrackState":
        return cls(
            track_id=str(payload.get("track_id", "rapid")),
            time_control_category_id=str(payload.get("time_control_category_id", "600+0")),
            current_level=max(1, int(payload.get("current_level", 1))),
            wins_toward_promotion=max(0, int(payload.get("wins_toward_promotion", 0))),
            losses_toward_demotion=max(0, int(payload.get("losses_toward_demotion", 0))),
            eligible_games_played=max(0, int(payload.get("eligible_games_played", 0))),
            last_eligible_result=payload.get("last_eligible_result"),
            last_eligible_bundle_time_control_id=payload.get("last_eligible_bundle_time_control_id"),
            last_eligible_bundle_rating_band=payload.get("last_eligible_bundle_rating_band"),
            last_updated_at_utc=payload.get("last_updated_at_utc"),
            placement_seed_complete=bool(payload.get("placement_seed_complete", False)),
            placement_games_remaining_hint=payload.get("placement_games_remaining_hint"),
        )


@dataclass
class SmartProfileState:
    mode_enabled: bool = True
    tracks: dict[str, dict[str, SmartProfileTrackState]] = field(default_factory=dict)

    def ensure_defaults(self) -> None:
        for tc, (track, _category) in SUPPORTED_TRACK_BY_TIME_CONTROL.items():
            by_category = self.tracks.setdefault(track, {})
            if tc not in by_category:
                by_category[tc] = SmartProfileTrackState(track_id=track, time_control_category_id=tc)

    def get_track_state(self, track_id: str, category_id: str) -> SmartProfileTrackState:
        self.ensure_defaults()
        return self.tracks[track_id][category_id]

    def to_dict(self) -> dict[str, Any]:
        self.ensure_defaults()
        return {
            "mode_enabled": self.mode_enabled,
            "tracks": {
                track: {category: state.to_dict() for category, state in categories.items()}
                for track, categories in self.tracks.items()
            },
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SmartProfileState":
        tracks_payload = payload.get("tracks") or {}
        tracks: dict[str, dict[str, SmartProfileTrackState]] = {}
        if isinstance(tracks_payload, dict):
            for track_id, by_category in tracks_payload.items():
                if not isinstance(by_category, dict):
                    continue
                tracks[str(track_id)] = {}
                for category_id, row in by_category.items():
                    if isinstance(row, dict):
                        tracks[str(track_id)][str(category_id)] = SmartProfileTrackState.from_dict(row)
        state = cls(mode_enabled=bool(payload.get("mode_enabled", True)), tracks=tracks)
        state.ensure_defaults()
        return state


@dataclass(frozen=True)
class SmartProfileEligibility:
    eligible: bool
    reason: str
    track_id: str | None = None
    time_control_category_id: str | None = None


@dataclass(frozen=True)
class SmartProfileStatus:
    active: bool
    track_id: str | None
    category_id: str | None
    level: int | None
    contract_summary: str
    wins_toward_promotion: int
    losses_toward_demotion: int
    eligible_now: bool
    eligibility_reason: str
    reserved_stockfish_tiers_pending: bool


def _band(minimum: int, maximum: int) -> OpponentTarget:
    return OpponentTarget(kind="rating_band", rating_min=minimum, rating_max=maximum)


def _stockfish() -> OpponentTarget:
    return OpponentTarget(kind="stockfish")


SMART_PROFILE_LEVELS: tuple[SmartProfileLevelContract, ...] = (
    SmartProfileLevelContract(1, _band(400, 600), 3, True, 20, 10),
    SmartProfileLevelContract(2, _band(600, 800), 4, True, 20, 10),
    SmartProfileLevelContract(3, _band(800, 1000), 5, True, 20, 10),
    SmartProfileLevelContract(4, _band(1000, 1200), 5, True, 20, 10),
    SmartProfileLevelContract(5, _band(1000, 1200), 6, True, 50, 10),
    SmartProfileLevelContract(6, _band(1200, 1400), 6, True, 20, 10),
    SmartProfileLevelContract(7, _band(1200, 1400), 7, True, 50, 10),
    SmartProfileLevelContract(8, _band(1400, 1600), 7, True, 20, 10),
    SmartProfileLevelContract(9, _band(1400, 1600), 3, False, 20, 10),
    SmartProfileLevelContract(10, _band(1400, 1600), 4, False, 20, 10),
    SmartProfileLevelContract(11, _band(1400, 1600), 5, False, 20, 10),
    SmartProfileLevelContract(12, _band(1400, 1600), 6, False, 20, 10),
    SmartProfileLevelContract(13, _band(1400, 1600), 7, False, 50, 10),
    SmartProfileLevelContract(14, _band(1600, 1800), 7, False, 50, 10),
    SmartProfileLevelContract(15, _band(1800, 2000), 7, False, 20, 10),
    SmartProfileLevelContract(16, _band(1800, 2000), 8, False, 50, 10),
    SmartProfileLevelContract(17, _band(2000, 2200), 8, False, 20, 10),
    SmartProfileLevelContract(18, _band(2000, 2200), 9, False, 50, 10),
    SmartProfileLevelContract(19, _band(2200, 2400), 9, False, 20, 10),
    SmartProfileLevelContract(20, _band(2200, 2400), 10, False, 50, 10),
    SmartProfileLevelContract(21, _band(2400, 2600), 10, False, 20, 10),
    SmartProfileLevelContract(22, _band(2400, 2600), 11, False, 50, 10),
    SmartProfileLevelContract(23, _band(2600, 2800), 11, False, 20, 10),
    SmartProfileLevelContract(24, _band(2600, 2800), 12, False, 50, 10),
    SmartProfileLevelContract(25, _band(2800, 3000), 12, False, 20, 10),
    SmartProfileLevelContract(26, _band(2800, 3000), 13, False, 50, 10),
    SmartProfileLevelContract(27, _band(3000, 3999), 13, False, 20, 10),
    SmartProfileLevelContract(28, _band(3000, 3999), 14, False, 50, 10),
    SmartProfileLevelContract(29, _stockfish(), 14, False, 20, 10),
    SmartProfileLevelContract(30, _stockfish(), 15, False, inf, 10),
)
LEVEL_BY_INDEX = {row.level: row for row in SMART_PROFILE_LEVELS}
HIGHEST_CORPUS_BACKED_LEVEL = 28


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_time_control_id(raw: str | None) -> str | None:
    if raw is None:
        return None
    token = str(raw).strip()
    if not token:
        return None
    if "+" in token:
        left, right = token.split("+", 1)
        if left.isdigit() and right.isdigit():
            return f"{int(left)}+{int(right)}"
    return token


def resolve_track_category(time_control_id: str | None) -> tuple[str, str] | None:
    normalized = normalize_time_control_id(time_control_id)
    if normalized is None:
        return None
    return SUPPORTED_TRACK_BY_TIME_CONTROL.get(normalized)


def format_expected_band(contract: SmartProfileLevelContract) -> str:
    target = contract.opponent_target
    if target.kind == "rating_band":
        return f"{target.rating_min}-{target.rating_max}"
    return "Stockfish"


class SmartProfileService:
    def __init__(self, storage: ReviewStorage, profile_id: str):
        self.storage = storage
        self.profile_id = profile_id
        self.state = SmartProfileState.from_dict(self.storage.load_smart_profile_state(profile_id))

    def switch_profile(self, profile_id: str) -> None:
        self.profile_id = profile_id
        self.state = SmartProfileState.from_dict(self.storage.load_smart_profile_state(profile_id))

    def save(self) -> None:
        self.storage.save_smart_profile_state(self.profile_id, self.state.to_dict())

    def current_track_state(self, time_control_id: str | None) -> tuple[SmartProfileTrackState | None, SmartProfileLevelContract | None]:
        resolved = resolve_track_category(time_control_id)
        if resolved is None:
            return None, None
        track_id, category_id = resolved
        track_state = self.state.get_track_state(track_id, category_id)
        return track_state, LEVEL_BY_INDEX.get(track_state.current_level)

    def enforce_runtime_contract(self, *, time_control_id: str | None, fallback_turns: int, fallback_good_accepted: bool) -> tuple[int, bool]:
        if not self.state.mode_enabled:
            return fallback_turns, fallback_good_accepted
        track_state, contract = self.current_track_state(time_control_id)
        if track_state is None or contract is None:
            return fallback_turns, fallback_good_accepted
        if contract.is_stockfish_tier:
            clamped = LEVEL_BY_INDEX[HIGHEST_CORPUS_BACKED_LEVEL]
            return clamped.turns_to_succeed, clamped.good_accepted
        return contract.turns_to_succeed, contract.good_accepted

    def evaluate_eligibility(
        self,
        *,
        routing_source: str,
        bundle_available: bool,
        time_control_id: str | None,
        bundle_rating_band: str | None,
        required_turns: int,
        good_accepted: bool,
    ) -> SmartProfileEligibility:
        if not self.state.mode_enabled:
            return SmartProfileEligibility(False, "Smart Profile mode is disabled.")
        if routing_source not in ORDINARY_ONLY_ROUTING_SOURCES:
            return SmartProfileEligibility(False, f"Ineligible routing source: {routing_source}.")
        if not bundle_available:
            return SmartProfileEligibility(False, "No corpus bundle is active for this run.")
        resolved = resolve_track_category(time_control_id)
        if resolved is None:
            return SmartProfileEligibility(False, f"Unsupported time control: {time_control_id or 'unknown'}.")
        track_id, category_id = resolved
        state = self.state.get_track_state(track_id, category_id)
        contract = LEVEL_BY_INDEX[state.current_level]
        if contract.is_stockfish_tier:
            return SmartProfileEligibility(False, "Top Stockfish tiers are reserved until engine-opponent ladder support is implemented.", track_id, category_id)
        expected_band = format_expected_band(contract)
        normalized_band = (bundle_rating_band or "").strip()
        if normalized_band != expected_band:
            return SmartProfileEligibility(False, f"Bundle rating band mismatch: expected {expected_band}, got {normalized_band or 'unknown' }.", track_id, category_id)
        if required_turns != contract.turns_to_succeed:
            return SmartProfileEligibility(False, f"Runtime turns mismatch: expected {contract.turns_to_succeed}, got {required_turns}.", track_id, category_id)
        if bool(good_accepted) != bool(contract.good_accepted):
            return SmartProfileEligibility(False, f"Good-move policy mismatch: expected {'accepted' if contract.good_accepted else 'rejected'}.", track_id, category_id)
        return SmartProfileEligibility(True, "Eligible ordinary corpus ladder game.", track_id, category_id)

    def apply_eligible_result(self, eligibility: SmartProfileEligibility, *, passed: bool, bundle_time_control_id: str | None, bundle_rating_band: str | None) -> str:
        if not eligibility.eligible or not eligibility.track_id or not eligibility.time_control_category_id:
            return "ignored_ineligible"
        track_state = self.state.get_track_state(eligibility.track_id, eligibility.time_control_category_id)
        contract = LEVEL_BY_INDEX[track_state.current_level]
        track_state.eligible_games_played += 1
        track_state.last_eligible_result = "success" if passed else "failure"
        track_state.last_eligible_bundle_time_control_id = normalize_time_control_id(bundle_time_control_id)
        track_state.last_eligible_bundle_rating_band = bundle_rating_band
        track_state.last_updated_at_utc = utc_now_iso()
        if passed:
            track_state.wins_toward_promotion += 1
        else:
            track_state.losses_toward_demotion += 1
        shift = "none"
        if track_state.wins_toward_promotion >= int(contract.game_successes_to_promote if contract.game_successes_to_promote != inf else 10**9):
            next_level = track_state.current_level + 1
            if next_level > HIGHEST_CORPUS_BACKED_LEVEL:
                next_level = HIGHEST_CORPUS_BACKED_LEVEL
                shift = "promotion_clamped"
            else:
                shift = "promotion"
            track_state.current_level = next_level
            track_state.wins_toward_promotion = 0
            track_state.losses_toward_demotion = 0
        elif track_state.losses_toward_demotion >= contract.game_failures_to_demote:
            next_level = max(1, track_state.current_level - 1)
            shift = "demotion" if next_level != track_state.current_level else "none"
            track_state.current_level = next_level
            track_state.wins_toward_promotion = 0
            track_state.losses_toward_demotion = 0
        self.save()
        return shift

    def status(
        self,
        *,
        routing_source: str,
        bundle_available: bool,
        time_control_id: str | None,
        bundle_rating_band: str | None,
        required_turns: int,
        good_accepted: bool,
    ) -> SmartProfileStatus:
        eligibility = self.evaluate_eligibility(
            routing_source=routing_source,
            bundle_available=bundle_available,
            time_control_id=time_control_id,
            bundle_rating_band=bundle_rating_band,
            required_turns=required_turns,
            good_accepted=good_accepted,
        )
        resolved = resolve_track_category(time_control_id)
        if resolved is None:
            return SmartProfileStatus(
                active=self.state.mode_enabled,
                track_id=None,
                category_id=None,
                level=None,
                contract_summary="Contract unavailable for current time control.",
                wins_toward_promotion=0,
                losses_toward_demotion=0,
                eligible_now=eligibility.eligible,
                eligibility_reason=eligibility.reason,
                reserved_stockfish_tiers_pending=True,
            )
        track_id, category_id = resolved
        track_state = self.state.get_track_state(track_id, category_id)
        contract = LEVEL_BY_INDEX[track_state.current_level]
        return SmartProfileStatus(
            active=self.state.mode_enabled,
            track_id=track_id,
            category_id=category_id,
            level=track_state.current_level,
            contract_summary=(
                f"L{track_state.current_level}: target {format_expected_band(contract)}, turns {contract.turns_to_succeed}, "
                f"Good {'accepted' if contract.good_accepted else 'rejected'}, promote {contract.game_successes_to_promote}, demote {contract.game_failures_to_demote}"
            ),
            wins_toward_promotion=track_state.wins_toward_promotion,
            losses_toward_demotion=track_state.losses_toward_demotion,
            eligible_now=eligibility.eligible,
            eligibility_reason=eligibility.reason,
            reserved_stockfish_tiers_pending=True,
        )

    def reset_all(self) -> None:
        self.state = SmartProfileState(mode_enabled=self.state.mode_enabled)
        self.state.ensure_defaults()
        self.save()

    def set_level_for_current_track(self, *, time_control_id: str | None, level: int) -> bool:
        resolved = resolve_track_category(time_control_id)
        if resolved is None:
            return False
        track_id, category_id = resolved
        state = self.state.get_track_state(track_id, category_id)
        state.current_level = max(1, min(HIGHEST_CORPUS_BACKED_LEVEL, int(level)))
        state.wins_toward_promotion = 0
        state.losses_toward_demotion = 0
        state.last_updated_at_utc = utc_now_iso()
        self.save()
        return True
