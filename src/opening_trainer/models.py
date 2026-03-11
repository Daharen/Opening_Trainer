from dataclasses import dataclass
from enum import Enum, auto


class SessionState(Enum):
    IDLE = auto()
    STARTING_GAME = auto()
    PLAYER_TURN = auto()
    OPPONENT_TURN = auto()
    FAIL_RESOLUTION = auto()
    SUCCESS_RESOLUTION = auto()
    RESTART_PENDING = auto()


class MoveJudgment(Enum):
    BOOK = auto()
    BETTER = auto()
    FAIL = auto()


@dataclass(frozen=True)
class EvaluationResult:
    judgment: MoveJudgment
    accepted: bool
    reason: str
    preferred_move: str | None = None


@dataclass(frozen=True)
class SessionOutcome:
    passed: bool
    reason: str
    preferred_move: str | None = None
