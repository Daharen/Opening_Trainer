from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class UrgencyTier(str, Enum):
    ORDINARY = 'ordinary_review'
    BOOSTED = 'boosted_review'
    EXTREME = 'extreme_urgency'


class RoutingSource(str, Enum):
    CORPUS = 'ordinary_corpus_play'
    IMMEDIATE_RETRY = 'immediate_retry'
    SRS_DUE_REVIEW = 'srs_due_review'
    SCHEDULED_REVIEW = 'scheduled_review'
    BOOSTED_REVIEW = 'boosted_review'
    EXTREME = 'extreme_urgency_review'
    STUBBORN_EXTREME_REPEAT = 'stubborn_extreme_repeat'
    HIJACK_REENTRY = 'hijack_reentry'
    HIJACK_DECAY_PASS = 'hijack_decay_pass'
    HIJACK_NO_ANCHOR = 'hijack_no_anchor'
    HIJACK_DORMANT_SKIP = 'hijack_dormant_skip'


class HijackStage(str, Enum):
    NONE = 'none'
    H80 = 'h80'
    H60 = 'h60'
    H40 = 'h40'
    H20 = 'h20'
    DORMANT = 'dormant'


@dataclass(frozen=True)
class ReviewPathMove:
    ply_index: int
    side_to_move: str
    move_uci: str
    san: str
    fen_before: str


@dataclass
class ReviewItem:
    review_item_id: str
    position_key: str
    position_fen_normalized: str
    side_to_move: str
    created_at_utc: str
    updated_at_utc: str
    last_seen_at_utc: str
    last_failed_at_utc: str | None
    last_passed_at_utc: str | None
    times_seen: int
    times_failed: int
    times_passed: int
    consecutive_failures: int
    consecutive_successes: int
    success_streak: int
    mastery_score: float
    stability_score: float
    urgency_tier: str
    urgency_multiplier: float
    due_at_utc: str
    last_routing_reason: str
    failure_reason: str
    preferred_move_uci: str | None
    accepted_move_set: list[str] = field(default_factory=list)
    predecessor_path: list[dict[str, Any]] = field(default_factory=list)
    line_preview_san: str = ''
    profile_id: str = 'default'
    frequency_retired_for_current_due_cycle: bool = False
    stubborn_extreme_state: str = 'none'
    stubborn_extra_repeat_consumed_until_success: bool = False
    skipped_review_slots: int = 0
    was_due_previous_check: bool = True
    pending_forced_stubborn_repeat: bool = False
    canonical_predecessor_path_id: str | None = None
    canonical_predecessor_path_metadata: dict[str, Any] = field(default_factory=dict)
    canonical_anchor_positions: list[str] = field(default_factory=list)
    hijack_stage: str = HijackStage.NONE.value
    hijack_pass_ticker: int = 0
    dormant: bool = False
    avoidance_count: int = 0
    last_hijack_routing_source: str = ''
    last_anchor_seen_at: str | None = None
    frequency_state: str = UrgencyTier.ORDINARY.value
    frequency_state_entered_at_utc: str = ''
    srs_stage_index: int = 0
    srs_next_due_at_utc: str = ''
    srs_last_reviewed_at_utc: str | None = None
    srs_last_result: str = 'none'
    srs_lapse_count: int = 0

    @classmethod
    def create(
        cls,
        profile_id: str,
        position_key: str,
        fen: str,
        side_to_move: str,
        failure_reason: str,
        preferred_move_uci: str | None,
        accepted_move_set: list[str] | None,
        predecessor_path: list[ReviewPathMove],
    ) -> 'ReviewItem':
        now = utc_now_iso()
        rid = hashlib.sha256(f'{profile_id}|{position_key}|{side_to_move}'.encode('utf-8')).hexdigest()[:16]
        return cls(
            review_item_id=rid,
            position_key=position_key,
            position_fen_normalized=fen,
            side_to_move=side_to_move,
            created_at_utc=now,
            updated_at_utc=now,
            last_seen_at_utc=now,
            last_failed_at_utc=now,
            last_passed_at_utc=None,
            times_seen=1,
            times_failed=1,
            times_passed=0,
            consecutive_failures=1,
            consecutive_successes=0,
            success_streak=0,
            mastery_score=0.0,
            stability_score=0.0,
            urgency_tier=UrgencyTier.ORDINARY.value,
            urgency_multiplier=1.0,
            due_at_utc=now,
            last_routing_reason='created_from_failure',
            failure_reason=failure_reason,
            preferred_move_uci=preferred_move_uci,
            accepted_move_set=accepted_move_set or [],
            predecessor_path=[asdict(move) for move in predecessor_path],
            line_preview_san=' '.join(move.san for move in predecessor_path[-6:]),
            profile_id=profile_id,
            was_due_previous_check=True,
            canonical_predecessor_path_id='legacy_default',
            canonical_predecessor_path_metadata={'path_count': 1, 'selection_rule': 'legacy_single_path'},
            canonical_anchor_positions=[move.fen_before for move in predecessor_path[-2:]] if predecessor_path else [],
            frequency_state=UrgencyTier.ORDINARY.value,
            frequency_state_entered_at_utc=now,
            srs_stage_index=0,
            srs_next_due_at_utc=(datetime.now(timezone.utc) + timedelta(days=1)).replace(microsecond=0).isoformat(),
            srs_last_reviewed_at_utc=now,
            srs_last_result='failure',
            srs_lapse_count=0,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> 'ReviewItem':
        payload = dict(payload)
        payload.setdefault('success_streak', payload.get('consecutive_successes', 0))
        payload.setdefault('frequency_retired_for_current_due_cycle', False)
        payload.setdefault('stubborn_extreme_state', 'none')
        payload.setdefault('stubborn_extra_repeat_consumed_until_success', False)
        payload.setdefault('skipped_review_slots', 0)
        payload.setdefault('was_due_previous_check', due_state(payload.get('due_at_utc', utc_now_iso())) == 'due')
        payload.setdefault('pending_forced_stubborn_repeat', False)
        payload.setdefault('canonical_predecessor_path_id', 'legacy_default')
        payload.setdefault('canonical_predecessor_path_metadata', {'path_count': 1, 'selection_rule': 'legacy_single_path'})
        payload.setdefault('canonical_anchor_positions', [])
        payload.setdefault('hijack_stage', HijackStage.NONE.value)
        payload.setdefault('hijack_pass_ticker', 0)
        payload.setdefault('dormant', False)
        payload.setdefault('avoidance_count', 0)
        payload.setdefault('last_hijack_routing_source', '')
        payload.setdefault('last_anchor_seen_at', None)
        legacy_tier = payload.get('urgency_tier', UrgencyTier.ORDINARY.value)
        payload.setdefault('frequency_state', legacy_tier)
        payload.setdefault('frequency_state_entered_at_utc', payload.get('updated_at_utc', utc_now_iso()))
        payload.setdefault('srs_stage_index', 0)
        payload.setdefault('srs_next_due_at_utc', payload.get('due_at_utc', utc_now_iso()))
        payload.setdefault('srs_last_reviewed_at_utc', payload.get('last_seen_at_utc'))
        payload.setdefault('srs_last_result', 'none')
        payload.setdefault('srs_lapse_count', 0)
        return cls(**payload)


@dataclass
class ProfileMeta:
    profile_id: str
    display_name: str
    created_at_utc: str
    updated_at_utc: str
    is_default: bool = False
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TrainerStats:
    total_reps: int = 0
    total_failures: int = 0
    total_successes: int = 0
    active_due_count: int = 0
    extreme_urgency_count: int = 0
    boosted_count: int = 0
    recent_routing_sources: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> 'TrainerStats':
        return cls(**payload)


@dataclass(frozen=True)
class ReviewPlan:
    root_fen: str
    target_review_item_id: str
    target_position_key: str
    target_fen: str
    predecessor_path: tuple[dict[str, Any], ...]
    routing_reason: str


@dataclass(frozen=True)
class RoutingDecision:
    routing_source: str
    selected_review_item_id: str | None
    urgency_tier: str | None
    due_state: str
    review_plan_present: bool
    selection_explanation: str
    profile_id: str
    review_plan: ReviewPlan | None = None
    corpus_share: float | None = None
    review_share: float | None = None
    due_count: int = 0
    boosted_due_count: int = 0
    extreme_due_count: int = 0
    deck_size: int = 0
    token_counts: dict[str, int] = field(default_factory=dict)
    selected_token_category: str | None = None
    queue_position_before: int | None = None
    queue_position_after: int | None = None
    rebuild_trigger: str | None = None


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def due_state(due_at_utc: str) -> str:
    return 'due' if parse_iso(due_at_utc) <= datetime.now(timezone.utc) else 'scheduled'
