from __future__ import annotations

import random

import chess

from .board import GameBoard
from .evaluation import EvaluatorConfig, format_evaluation_feedback
from .evaluator import MoveEvaluator
from .models import EvaluationResult, SessionOutcome, SessionState, SessionView
from .opponent import OpponentProvider


class TrainingSession:
    restart_delay_ms = 900

    def __init__(self):
        self.board = GameBoard()
        self.opponent = OpponentProvider()
        self.evaluator = MoveEvaluator()
        self.config = EvaluatorConfig()
        self.required_player_moves = self.config.active_envelope_player_moves

        self.player_color = chess.WHITE
        self.player_move_count = 0
        self.state = SessionState.IDLE

        self.last_evaluation: EvaluationResult | None = None
        self.last_outcome: SessionOutcome | None = None

    def start_new_game(self) -> SessionView:
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
            self.advance_until_user_turn()

        return self.get_view()

    def get_view(self) -> SessionView:
        return SessionView(
            board_fen=self.board.board.fen(),
            player_color=self.player_color,
            state=self.state,
            player_move_count=self.player_move_count,
            required_player_moves=self.required_player_moves,
            last_evaluation=self.last_evaluation,
            last_outcome=self.last_outcome,
        )

    def current_board(self) -> chess.Board:
        return self.board.board.copy(stack=True)

    def get_current_player_color(self) -> chess.Color:
        return self.player_color

    def get_current_session_phase(self) -> SessionState:
        return self.state

    def get_last_evaluation_result(self) -> EvaluationResult | None:
        return self.last_evaluation

    def is_awaiting_user_input(self) -> bool:
        return self.state == SessionState.PLAYER_TURN

    def is_processing_opponent_motion(self) -> bool:
        return self.state == SessionState.OPPONENT_TURN

    def has_failed(self) -> bool:
        return self.state == SessionState.FAIL_RESOLUTION or (
            self.last_outcome is not None and not self.last_outcome.passed
        )

    def has_passed(self) -> bool:
        return self.state == SessionState.SUCCESS_RESOLUTION or (
            self.last_outcome is not None and self.last_outcome.passed
        )

    def run_session(self, input_func=None) -> None:
        if input_func is None:
            input_func = input
        while True:
            if self.state == SessionState.PLAYER_TURN:
                self._handle_player_turn(input_func)
                continue

            if self.state == SessionState.OPPONENT_TURN:
                self.advance_until_user_turn()
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

    def submit_user_move_uci(self, move_uci: str) -> SessionView:
        return self._submit_user_move(move_uci.strip())

    def submit_user_move(self, move_text: str) -> SessionView:
        return self._submit_user_move(move_text.strip())

    def advance_until_user_turn(self) -> SessionView:
        while self.state == SessionState.OPPONENT_TURN:
            self._handle_opponent_turn()
        return self.get_view()

    def legal_moves_from(self, square: chess.Square) -> list[chess.Move]:
        return self.board.legal_moves_from(square)

    def _handle_player_turn(self, input_func=None) -> None:
        if input_func is None:
            input_func = input
        print("", flush=True)
        print(self.board, flush=True)
        print("", flush=True)

        print("Your move: ", end="", flush=True)
        move_str = input_func().strip()
        self._submit_user_move(move_str)

    def _submit_user_move(self, move_str: str) -> SessionView:
        if self.state != SessionState.PLAYER_TURN:
            raise RuntimeError("Cannot submit a user move when the session is not awaiting player input.")

        if not self.board.is_legal(move_str):
            print("Illegal move. Try again.", flush=True)
            return self.get_view()

        board_before_move = self.board.board.copy(stack=False)
        move = self.board.push(move_str)
        self.player_move_count += 1

        evaluation = self.evaluator.evaluate(
            board_before_move,
            move,
            self.player_move_count,
        )
        self.last_evaluation = evaluation

        self._print_evaluation_feedback(evaluation)

        if not evaluation.accepted:
            self.last_outcome = SessionOutcome(
                passed=False,
                reason=evaluation.reason_text,
                preferred_move=evaluation.preferred_move_san or evaluation.preferred_move_uci,
                evaluation=evaluation,
            )
            self.state = SessionState.FAIL_RESOLUTION
            self._resolve_fail()
            return self.get_view()

        if self.player_move_count >= self.required_player_moves:
            self.last_outcome = SessionOutcome(
                passed=True,
                reason=(
                    f"Completed {self.required_player_moves} accepted player moves inside the opening window."
                ),
                preferred_move=None,
                evaluation=evaluation,
            )
            self.state = SessionState.SUCCESS_RESOLUTION
            self._resolve_success()
            return self.get_view()

        self.state = SessionState.OPPONENT_TURN
        self.advance_until_user_turn()
        return self.get_view()

    def _handle_opponent_turn(self) -> None:
        move = self.opponent.choose_move(self.board.board)
        san = self.board.board.san(move)
        self.board.board.push(move)

        print(f"Opponent plays: {san}", flush=True)

        if self.board.turn() == self.player_color:
            self.state = SessionState.PLAYER_TURN
        else:
            self.state = SessionState.OPPONENT_TURN

    def _resolve_fail(self) -> None:
        print("", flush=True)
        print("FAIL", flush=True)
        if self.last_outcome is not None:
            print(self.last_outcome.reason, flush=True)
            if self.last_outcome.preferred_move:
                print(f"Preferred move: {self.last_outcome.preferred_move}", flush=True)
        print("Restarting training game...", flush=True)
        self.state = SessionState.RESTART_PENDING

    def _resolve_success(self) -> None:
        print("", flush=True)
        print("SUCCESS", flush=True)
        if self.last_outcome is not None:
            print(self.last_outcome.reason, flush=True)
        print("Opening window cleared. Restarting training game...", flush=True)
        self.state = SessionState.RESTART_PENDING

    def _print_new_game_banner(self) -> None:
        print("", flush=True)
        print("=== New Training Game ===", flush=True)
        if self.player_color == chess.WHITE:
            print("You are WHITE", flush=True)
        else:
            print("You are BLACK", flush=True)

    def _print_evaluation_feedback(self, evaluation: EvaluationResult) -> None:
        for line in format_evaluation_feedback(evaluation):
            print(line, flush=True)
