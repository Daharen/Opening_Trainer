from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class OutcomeArrowContract:
    move_uci: str
    color: str
    width_scale: float = 1.0


@dataclass(frozen=True)
class PunishmentSlideContract:
    step_index: int
    total_steps: int
    board_fen: str
    current_move_uci: str
    current_move_san: str
    player_color: bool


@dataclass(frozen=True)
class OutcomeBoardContract:
    title: str
    board_fen: str
    player_color: bool
    arrow_label: str
    move_label: str | None = None
    arrows: tuple[OutcomeArrowContract, ...] = ()


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
    punishment_slides: tuple[PunishmentSlideContract, ...] = ()


@dataclass(frozen=True)
class SessionEvent:
    event_type: str
    payload: dict = field(default_factory=dict)
