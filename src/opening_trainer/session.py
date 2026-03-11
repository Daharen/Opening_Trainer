import random
import chess

from .board import GameBoard
from .evaluator import MoveEvaluator
from .models import EvaluationResult, SessionOutcome, SessionState
from .opponent import OpponentProvider


class TrainingSession:
    REQUIRED_PLAYER_MOVES = 5

    def __init__(self):
        self.board = GameBoard()
        self.opponent = OpponentProvider()
        self.evaluator = MoveEvaluator()

        self.player_color = chess.WHITE
        self.player_move_count = 0
        self.state = SessionState.IDLE

        self.last_evaluation: EvaluationResult | None = None
        self.last_outcome: SessionOutcome | None = None

    def start_new_game(self) -> None:
        self.state = SessionState.STARTING_GAME
        self.board.reset()
        self.player_color = random.choice([chess.WHITE, chess.BLACK])
        self.player_move_count = 0
        self.last_evaluation = None
        self.last_outcome = None

        self._print_new_game_banner()

        if self.board.turn() == self.player_color:
            self.state = SessionState.PLAYER_TURN
        else:
            self.state = SessionState.OPPONENT_TURN

    def run_session(self) -> None:
        while True:
            if self.state == SessionState.PLAYER_TURN:
                self._handle_player_turn()
                continue

            if self.state == SessionState.OPPONENT_TURN:
                self._handle_opponent_turn()
                continue

            if self.state == SessionState.FAIL_RESOLUTION:
                self._resolve_fail()
                return

            if self.state == SessionState.SUCCESS_RESOLUTION:
                self._resolve_success()
                return

            if self.state == SessionState.RESTART_PENDING:
                return

            raise RuntimeError(f"Unexpected session state: {self.state}")

    def _handle_player_turn(self) -> None:
        print()
        print(self.board)
        print()

        move_str = input("Your move: ").strip()

        if not self.board.is_legal(move_str):
            print("Illegal move. Try again.")
            return

        move = self.board.push(move_str)
        self.player_move_count += 1

        evaluation = self.evaluator.evaluate(
            self.board.board,
            move,
            self.player_move_count,
        )
        self.last_evaluation = evaluation

        self._print_evaluation_feedback(evaluation)

        if not evaluation.accepted:
            self.last_outcome = SessionOutcome(
                passed=False,
                reason=evaluation.reason,
                preferred_move=evaluation.preferred_move,
            )
            self.state = SessionState.FAIL_RESOLUTION
            return

        if self.player_move_count >= self.REQUIRED_PLAYER_MOVES:
            self.last_outcome = SessionOutcome(
                passed=True,
                reason=(
                    f"Completed {self.REQUIRED_PLAYER_MOVES} accepted player moves "
                    f"inside the opening window."
                ),
                preferred_move=None,
            )
            self.state = SessionState.SUCCESS_RESOLUTION
            return

        self.state = SessionState.OPPONENT_TURN

    def _handle_opponent_turn(self) -> None:
        move = self.opponent.choose_move(self.board.board)
        san = self.board.board.san(move)
        self.board.board.push(move)

        print(f"Opponent plays: {san}")

        if self.board.turn() == self.player_color:
            self.state = SessionState.PLAYER_TURN
        else:
            self.state = SessionState.OPPONENT_TURN

    def _resolve_fail(self) -> None:
        print()
        print("FAIL")
        if self.last_outcome is not None:
            print(self.last_outcome.reason)
            if self.last_outcome.preferred_move:
                print(f"Preferred move: {self.last_outcome.preferred_move}")
        print("Restarting training game...")
        self.state = SessionState.RESTART_PENDING

    def _resolve_success(self) -> None:
        print()
        print("SUCCESS")
        if self.last_outcome is not None:
            print(self.last_outcome.reason)
        print("Opening window cleared. Restarting training game...")
        self.state = SessionState.RESTART_PENDING

    def _print_new_game_banner(self) -> None:
        print()
        print("=== New Training Game ===")
        if self.player_color == chess.WHITE:
            print("You are WHITE")
        else:
            print("You are BLACK")

    def _print_evaluation_feedback(self, evaluation: EvaluationResult) -> None:
        judgment_name = evaluation.judgment.name
        accepted_text = "ACCEPTED" if evaluation.accepted else "REJECTED"
        print(f"{judgment_name} — {accepted_text}")
        print(evaluation.reason)
        if evaluation.preferred_move:
            print(f"Preferred move: {evaluation.preferred_move}")
