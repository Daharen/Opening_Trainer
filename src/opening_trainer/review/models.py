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
    SCHEDULED_REVIEW = 'scheduled_review'
    BOOSTED_REVIEW = 'boosted_review'
    EXTREME = 'extreme_urgency_override'


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
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> 'ReviewItem':
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
    boosted_due_count: int = 0
    extreme_due_count: int = 0


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def due_state(due_at_utc: str) -> str:
    return 'due' if parse_iso(due_at_utc) <= datetime.now(timezone.utc) else 'scheduled'
