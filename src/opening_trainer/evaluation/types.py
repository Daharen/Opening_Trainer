from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CanonicalJudgment(str, Enum):
    BOOK = "Book"
    BETTER = "Better"
    FAIL = "Fail"
    AUTHORITY_UNAVAILABLE = "AuthorityUnavailable"


class OverlayLabel(str, Enum):
    BOOK = "Book"
    BEST = "Best"
    EXCELLENT = "Excellent"
    GOOD = "Good"
    INACCURACY = "Inaccuracy"
    MISTAKE = "Mistake"
    BLUNDER = "Blunder"
    MISSED_WIN = "MissedWin"
    AUTHORITY_UNAVAILABLE = "AuthorityUnavailable"


class AuthoritySource(str, Enum):
    BOOK = "book"
    ENGINE = "engine"
    NONE = "none"


class ReasonCode(str, Enum):
    ILLEGAL_MOVE = "illegal_move"
    BOOK_HIT = "book_hit"
    BOOK_UNAVAILABLE = "book_unavailable"
    ENGINE_PASS = "engine_pass"
    ENGINE_FAIL = "engine_fail"
    ENGINE_UNAVAILABLE = "engine_unavailable"
    MISSED_WIN = "missed_win"
    OPENING_EXIT_BEFORE_OPPONENT = "opening_exit_before_opponent"


@dataclass(frozen=True)
class EvaluationResult:
    accepted: bool
    canonical_judgment: CanonicalJudgment
    overlay_label: OverlayLabel
    reason_code: ReasonCode
    reason_text: str
    authority_source: AuthoritySource
    move_uci: str
    legal_move_confirmed: bool
    preferred_move_uci: str | None = None
    preferred_move_san: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BookAuthorityResult:
    accepted: bool
    available: bool
    reason_code: ReasonCode
    reason_text: str
    candidate_move_uci: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EngineAuthorityResult:
    accepted: bool
    available: bool
    reason_code: ReasonCode
    reason_text: str
    best_move_uci: str | None = None
    best_move_san: str | None = None
    played_move_uci: str | None = None
    played_move_san: str | None = None
    cp_loss: int | None = None
    best_score_cp: int | None = None
    played_score_cp: int | None = None
    mate_for_side_to_move: int | None = None
    mate_after_move_for_side_to_move: int | None = None
    principal_variation: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
