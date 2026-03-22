import chess

from opening_trainer.evaluation import (
    BookAuthorityResult,
    EngineAuthorityResult,
    ReasonCode,
)
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.models import SessionState
from opening_trainer.session import TrainingSession
from opening_trainer.ui.square_mapping import display_to_square, square_to_display


class StubBookAuthority:
    def __init__(self, result: BookAuthorityResult):
        self.result = result

    def evaluate(self, board_before_move: chess.Board, played_move: chess.Move) -> BookAuthorityResult:
        return self.result


class StubEngineAuthority:
    def __init__(self, result: EngineAuthorityResult):
        self.result = result

    def evaluate(self, board_before_move: chess.Board, played_move: chess.Move) -> EngineAuthorityResult:
        return self.result


BOOK_MISS = BookAuthorityResult(
    accepted=False,
    available=False,
    reason_code=ReasonCode.BOOK_UNAVAILABLE,
    reason_text="Book authority unavailable for this position.",
    metadata={"book_available": False},
)


def test_square_mapping_white_and_black_orientation_round_trip():
    assert square_to_display(chess.A1, chess.WHITE) == (7, 0)
    assert square_to_display(chess.H8, chess.WHITE) == (0, 7)
    assert square_to_display(chess.A1, chess.BLACK) == (0, 7)
    assert square_to_display(chess.H8, chess.BLACK) == (7, 0)

    assert display_to_square(7, 0, chess.WHITE) == chess.A1
    assert display_to_square(0, 7, chess.WHITE) == chess.H8
    assert display_to_square(0, 7, chess.BLACK) == chess.A1
    assert display_to_square(7, 0, chess.BLACK) == chess.H8


def test_submit_user_move_uci_uses_existing_evaluator_pipeline_without_gui_logic_duplication():
    session = TrainingSession()
    session.player_color = chess.WHITE
    session.state = SessionState.PLAYER_TURN
    session.evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=False,
                available=True,
                reason_code=ReasonCode.ENGINE_FAIL,
                reason_text="Rejected by engine.",
                best_move_uci="d2d4",
                best_move_san="d4",
                played_move_uci="e2e4",
                played_move_san="e4",
                cp_loss=170,
                metadata={"engine_available": True},
            )
        ),
    )

    view = session.submit_user_move_uci("e2e4")

    assert view.state == SessionState.RESTART_PENDING
    assert view.run_failed is True
    assert view.last_evaluation is not None
    assert view.last_evaluation.move_uci == "e2e4"
    assert view.last_outcome is not None
    assert view.last_outcome.evaluation is view.last_evaluation


def test_start_new_game_advances_black_sessions_until_user_turn(monkeypatch):
    session = TrainingSession()
    monkeypatch.setattr('random.choice', lambda options: chess.BLACK)
    monkeypatch.setattr(session.opponent, 'choose_move', lambda board: chess.Move.from_uci('e2e4'))

    view = session.start_new_game()

    assert view.player_color == chess.BLACK
    assert view.awaiting_user_input is True
    assert session.current_board().move_stack[-1].uci() == 'e2e4'


def test_run_session_cli_path_accepts_injected_input_function(monkeypatch):
    session = TrainingSession()
    session.required_player_moves = 1
    monkeypatch.setattr('random.choice', lambda options: chess.WHITE)
    session.start_new_game()
    session.evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=True,
                available=True,
                reason_code=ReasonCode.ENGINE_PASS,
                reason_text='Accepted by engine.',
                best_move_uci='e2e4',
                best_move_san='e4',
                played_move_uci='e2e4',
                played_move_san='e4',
                cp_loss=0,
                metadata={'engine_available': True},
            )
        ),
    )

    session.run_session(input_func=lambda: 'e2e4')

    assert session.state == SessionState.RESTART_PENDING
    assert session.has_passed() is True
