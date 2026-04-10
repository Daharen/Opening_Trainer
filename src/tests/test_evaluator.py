import chess

from opening_trainer.evaluation import (
    AuthoritySource,
    BookAuthorityResult,
    CanonicalJudgment,
    EngineAuthorityResult,
    EvaluatorConfig,
    OverlayClassifier,
    OverlayLabel,
    ReasonCode,
)
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.practical_risk_reconciled import ReconciledBandResolution
from opening_trainer.session import TrainingSession
from opening_trainer.models import SessionState


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


class StubReconciledServiceRescue:
    active = True
    activation_error = None

    def resolve_band_id(self, requested_band_id):
        return ReconciledBandResolution(requested_band_id, "1400-1600", "exact")

    def get_move_admission(self, position_key, band_id, move_uci):
        return {
            "reconciled_admitted_if_good_accepted": True,
            "reconciled_admitted_if_good_rejected": True,
            "reconciled_admission_origin_if_good_accepted": "reconciled",
            "reconciled_admission_origin_if_good_rejected": "reconciled",
            "admission_origin": "reconciled",
            "local_admitted_if_good_accepted": False,
            "local_admitted_if_good_rejected": False,
        }

    def get_failure_explanation(self, position_key, band_id, move_uci, mode_id):
        return None


BOOK_MISS = BookAuthorityResult(
    accepted=False,
    available=False,
    reason_code=ReasonCode.BOOK_UNAVAILABLE,
    reason_text="Book authority unavailable for this position.",
    metadata={"book_available": False},
)


def make_move(move_uci: str) -> tuple[chess.Board, chess.Move]:
    board = chess.Board()
    move = chess.Move.from_uci(move_uci)
    assert move in board.legal_moves
    return board, move


def test_book_acceptance_path():
    board, move = make_move("e2e4")
    evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(
            BookAuthorityResult(
                accepted=True,
                available=True,
                reason_code=ReasonCode.BOOK_HIT,
                reason_text="Accepted via book membership.",
                candidate_move_uci="e2e4",
                metadata={"book_available": True},
            )
        ),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.ENGINE_UNAVAILABLE,
                reason_text="Engine unavailable.",
            )
        ),
    )

    result = evaluator.evaluate(board, move, 1)

    assert result.accepted is True
    assert result.canonical_judgment == CanonicalJudgment.BOOK
    assert result.overlay_label == OverlayLabel.BOOK
    assert result.authority_source == AuthoritySource.BOOK


def test_better_acceptance_path_and_overlay_mapping():
    board, move = make_move("e2e4")
    evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=True,
                available=True,
                reason_code=ReasonCode.ENGINE_PASS,
                reason_text="Accepted by engine.",
                best_move_uci="d2d4",
                best_move_san="d4",
                played_move_uci="e2e4",
                played_move_san="e4",
                cp_loss=30,
                best_score_cp=40,
                played_score_cp=10,
                metadata={"engine_available": True},
            )
        ),
    )

    result = evaluator.evaluate(board, move, 1)

    assert result.accepted is True
    assert result.canonical_judgment == CanonicalJudgment.BETTER
    assert result.overlay_label == OverlayLabel.EXCELLENT
    assert result.reason_code == ReasonCode.ENGINE_PASS


def test_fail_path_for_unacceptable_move_and_preferred_move():
    board, move = make_move("e2e4")
    evaluator = MoveEvaluator(
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
                cp_loss=180,
                best_score_cp=120,
                played_score_cp=-60,
                metadata={"engine_available": True},
            )
        ),
    )

    result = evaluator.evaluate(board, move, 1)

    assert result.accepted is False
    assert result.canonical_judgment == CanonicalJudgment.FAIL
    assert result.overlay_label == OverlayLabel.MISTAKE
    assert result.preferred_move_uci == "d2d4"
    assert result.preferred_move_san == "d4"


def test_fail_overlay_mapping_blunder_and_inaccuracy():
    classifier = OverlayClassifier(EvaluatorConfig())

    blunder = classifier.classify(
        CanonicalJudgment.FAIL,
        EngineAuthorityResult(
            accepted=False,
            available=True,
            reason_code=ReasonCode.ENGINE_FAIL,
            reason_text="",
            cp_loss=300,
        ),
    )
    inaccuracy = classifier.classify(
        CanonicalJudgment.FAIL,
        EngineAuthorityResult(
            accepted=False,
            available=True,
            reason_code=ReasonCode.ENGINE_FAIL,
            reason_text="",
            cp_loss=100,
        ),
    )

    assert blunder[0] == OverlayLabel.BLUNDER
    assert inaccuracy[0] == OverlayLabel.INACCURACY


def test_missed_win_horizon_cap_behavior():
    config = EvaluatorConfig(missed_win_mate_ply_cap_by_mode={"default": 4})
    classifier = OverlayClassifier(config)

    missed = classifier.classify(
        CanonicalJudgment.FAIL,
        EngineAuthorityResult(
            accepted=False,
            available=True,
            reason_code=ReasonCode.ENGINE_FAIL,
            reason_text="",
            cp_loss=20,
            mate_for_side_to_move=3,
            mate_after_move_for_side_to_move=None,
        ),
    )
    not_missed = classifier.classify(
        CanonicalJudgment.FAIL,
        EngineAuthorityResult(
            accepted=False,
            available=True,
            reason_code=ReasonCode.ENGINE_FAIL,
            reason_text="",
            cp_loss=20,
            mate_for_side_to_move=6,
            mate_after_move_for_side_to_move=None,
        ),
    )

    assert missed[0] == OverlayLabel.MISSED_WIN
    assert not_missed[0] == OverlayLabel.INACCURACY


def test_structured_result_contains_required_fields():
    board, move = make_move("e2e4")
    evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=True,
                available=True,
                reason_code=ReasonCode.ENGINE_PASS,
                reason_text="Accepted by engine.",
                best_move_uci="e2e4",
                best_move_san="e4",
                played_move_uci="e2e4",
                played_move_san="e4",
                cp_loss=0,
                metadata={"engine_available": True},
            )
        ),
    )

    result = evaluator.evaluate(board, move, 1)

    assert result.move_uci == "e2e4"
    assert result.legal_move_confirmed is True
    assert isinstance(result.metadata["thresholds"], dict)
    assert "engine" in result.metadata
    assert result.reason_text


def test_good_overlay_can_be_rejected_by_acceptance_policy():
    board, move = make_move("e2e4")
    evaluator = MoveEvaluator(
        config=EvaluatorConfig(good_moves_acceptable=False),
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=True,
                available=True,
                reason_code=ReasonCode.ENGINE_PASS,
                reason_text="Accepted by engine.",
                best_move_uci="d2d4",
                best_move_san="d4",
                played_move_uci="e2e4",
                played_move_san="e4",
                cp_loss=70,
                metadata={"engine_available": True},
            )
        ),
    )

    result = evaluator.evaluate(board, move, 1)

    assert result.accepted is False
    assert result.overlay_label == OverlayLabel.GOOD
    assert result.canonical_judgment == CanonicalJudgment.FAIL
    assert "Good moves are configured to count as fails" in result.reason_text


def test_session_consumes_structured_result_without_duplicate_acceptance_logic(monkeypatch):
    session = TrainingSession()
    session.required_player_moves = 5
    session.player_color = chess.WHITE
    session.state = SessionState.PLAYER_TURN

    class StubBoard:
        def __init__(self):
            self.board = chess.Board()

        def __str__(self):
            return str(self.board)

        def is_legal(self, move_str):
            return True

        def push(self, move_str):
            move = self.board.parse_san(move_str)
            self.board.push(move)
            return move

        def turn(self):
            return self.board.turn

    class StubEvaluator:
        def evaluate(self, board_before_move, played_move, player_move_number, mode="default"):
            return MoveEvaluator(
                book_authority=StubBookAuthority(BOOK_MISS),
                engine_authority=StubEngineAuthority(
                    EngineAuthorityResult(
                        accepted=False,
                        available=True,
                        reason_code=ReasonCode.ENGINE_FAIL,
                        reason_text="Rejected by engine.",
                        best_move_uci="d2d4",
                        best_move_san="d4",
                        played_move_uci=played_move.uci(),
                        played_move_san="e4",
                        cp_loss=200,
                        metadata={"engine_available": True},
                    )
                ),
            ).evaluate(board_before_move, played_move, player_move_number, mode)

    session.board = StubBoard()
    session.evaluator = StubEvaluator()
    monkeypatch.setattr("builtins.input", lambda: "e4")

    session._handle_player_turn()

    assert session.state == SessionState.RESTART_PENDING
    assert session.last_outcome is not None
    assert session.last_outcome.passed is False
    assert session.last_outcome.evaluation is session.last_evaluation


def test_practical_risk_rescue_clears_engine_fail_reason_text():
    board, move = make_move("e2e4")
    evaluator = MoveEvaluator(
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
                cp_loss=180,
                metadata={"engine_available": True},
            )
        ),
        reconciled_service=StubReconciledServiceRescue(),
    )
    result = evaluator.evaluate(board, move, 1, requested_band_id="1400-1600")
    assert result.accepted is True
    assert result.canonical_judgment == CanonicalJudgment.BETTER
    assert result.reason_code == ReasonCode.ENGINE_PASS
    assert result.reason_text == "Accepted via practical-risk reconciliation."
    assert "Rejected as an inaccuracy outside engine tolerance." not in result.reason_text
