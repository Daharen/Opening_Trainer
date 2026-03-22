from dataclasses import dataclass
from enum import Enum, auto

from .evaluation import EvaluationResult


class SessionState(Enum):
    IDLE = auto()
    STARTING_GAME = auto()
    PLAYER_TURN = auto()
    OPPONENT_TURN = auto()
    FAIL_RESOLUTION = auto()
    SUCCESS_RESOLUTION = auto()
    RESTART_PENDING = auto()


@dataclass(frozen=True)
class SessionOutcome:
    passed: bool
    reason: str
    preferred_move: str | None = None
    evaluation: EvaluationResult | None = None
