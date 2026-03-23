from dataclasses import dataclass
from enum import Enum, auto

import chess

from .evaluation import EvaluationResult


class SessionState(Enum):
    IDLE = auto()
    STARTING_GAME = auto()
    PLAYER_TURN = auto()
    OPPONENT_TURN = auto()
    FAIL_RESOLUTION = auto()
    SUCCESS_RESOLUTION = auto()
    AUTHORITY_UNAVAILABLE_RESOLUTION = auto()
    RESTART_PENDING = auto()


@dataclass(frozen=True)
class SessionOutcome:
    passed: bool
    reason: str
    preferred_move: str | None = None
    evaluation: EvaluationResult | None = None
    terminal_kind: str = "fail"
    routing_reason: str = "ordinary_corpus_play"
    next_routing_reason: str = "ordinary_corpus_play"
    profile_name: str = "Default"
    impact_summary: str = "No review impact recorded."


@dataclass(frozen=True)
class SessionView:
    board_fen: str
    player_color: chess.Color
    state: SessionState
    player_move_count: int
    required_player_moves: int
    last_evaluation: EvaluationResult | None
    last_outcome: SessionOutcome | None
    routing_state: object | None = None

    @property
    def awaiting_user_input(self) -> bool:
        return self.state == SessionState.PLAYER_TURN

    @property
    def processing_opponent(self) -> bool:
        return self.state == SessionState.OPPONENT_TURN

    @property
    def run_failed(self) -> bool:
        return self.last_outcome is not None and self.last_outcome.terminal_kind == "fail"

    @property
    def run_passed(self) -> bool:
        return self.last_outcome is not None and self.last_outcome.terminal_kind == "pass"
