import chess

from .models import EvaluationResult, MoveJudgment


class MoveEvaluator:
    """
    Provisional evaluator contract.

    This lane does not yet implement true opening-book membership or
    engine-tolerance logic. Instead, it upgrades the API so later lanes
    can drop in real judgment without altering session orchestration.
    """

    def evaluate(
        self,
        board: chess.Board,
        played_move: chess.Move,
        player_move_number: int,
    ) -> EvaluationResult:
        """
        Evaluate the move that was just played.

        Current provisional behavior:
        1. Accept every legal move.
        2. Mark all accepted moves as BOOK for placeholder purposes.
        3. Return explicit reasoning text so transparency plumbing exists now.
        """

        return EvaluationResult(
            judgment=MoveJudgment.BOOK,
            accepted=True,
            reason=(
                f"Provisional evaluator accepted move "
                f"on player move {player_move_number}."
            ),
            preferred_move=None,
        )
