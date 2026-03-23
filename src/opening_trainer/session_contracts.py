from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class OutcomeBoardContract:
    title: str
    board_fen: str
    player_color: bool
    arrow_move_uci: str | None
    arrow_color: str
    arrow_label: str
    move_label: str | None = None


@dataclass(frozen=True)
class OutcomeModalContract:
    headline: str
    summary: str
    reason: str
    preferred_move: str | None
    routing_reason: str
    next_routing_reason: str
    impact_summary: str
    requires_acknowledgement: bool = True
    review_boards: tuple[OutcomeBoardContract, ...] = ()


@dataclass(frozen=True)
class SessionEvent:
    event_type: str
    payload: dict = field(default_factory=dict)
