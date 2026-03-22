from __future__ import annotations

import json
import random
from pathlib import Path

import chess

from opening_trainer.corpus import CorpusIngestor, RatingBandPolicy, load_artifact, normalize_position_key, save_artifact
from opening_trainer.opponent import CorpusBackedOpponentProvider
from opening_trainer.evaluation import (
    BookAuthorityResult,
    EngineAuthorityResult,
    ReasonCode,
)
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.models import SessionState
from opening_trainer.session import TrainingSession


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_corpus.pgn"


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


def test_rating_band_policy_requires_both_players_in_band():
    policy = RatingBandPolicy()
    ingestor = CorpusIngestor(rating_policy=policy)

    artifact = ingestor.build_artifact([str(FIXTURE_PATH)])

    payload = artifact.to_dict()
    assert payload["target_rating_band"] == {"minimum": 475, "maximum": 525}
    assert payload["rating_policy"] == "both_players_in_band"
    position_keys = {position.position_key for position in artifact.positions}
    assert normalize_position_key(chess.Board()) in position_keys
    assert all("d4" not in json.dumps(position.to_dict()) for position in artifact.positions)


def test_position_keys_and_uci_counts_are_deterministic():
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    start_position = next(position for position in artifact.positions if position.position_key == normalize_position_key(chess.Board()))

    assert start_position.side_to_move == "white"
    assert [move.uci for move in start_position.candidate_moves] == ["e2e4"]
    assert start_position.candidate_moves[0].raw_count == 2
    assert start_position.total_observed_count == 2

    board = chess.Board()
    for uci in ["e2e4", "e7e5", "g1f3", "b8c6", "f1c4"]:
        board.push(chess.Move.from_uci(uci))
    exact_position = next(position for position in artifact.positions if position.position_key == normalize_position_key(board))
    assert [move.uci for move in exact_position.candidate_moves] == ["f8c5", "g8f6"]
    assert [move.raw_count for move in exact_position.candidate_moves] == [1, 1]


def test_artifact_serialization_and_reload_preserve_raw_counts(tmp_path):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    output_path = tmp_path / "artifact.json"

    save_artifact(artifact, output_path)
    reloaded = load_artifact(output_path)

    assert reloaded == artifact
    saved_payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved_payload["schema_version"] == 1
    assert saved_payload["positions"][0]["candidate_moves"][0]["raw_count"] >= 1


def test_sparse_position_annotation_and_runtime_backoff(tmp_path):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "artifact.json")
    provider = CorpusBackedOpponentProvider(artifact_path, rng=random.Random(7))

    sparse_position = next(position for position in artifact.positions if position.sparse)
    board = chess.Board(sparse_position.position_key)
    choice = provider.choose_move(board)

    assert choice.position_key == sparse_position.position_key
    assert choice.sparse is True
    assert choice.raw_count == 1

    unseen_board = chess.Board()
    for uci in ["e2e4", "e7e5", "g1f3", "b8c6", "f1c4", "g8f6", "d2d3", "f8c5", "c2c3", "d7d6", "d1b3"]:
        unseen_board.push(chess.Move.from_uci(uci))
    try:
        provider.choose_move(unseen_board)
    except LookupError as exc:
        assert "No corpus-backed move available" in str(exc)
    else:
        raise AssertionError("Expected a clear lookup failure for positions with no acceptable corpus-backed move.")


def test_runtime_move_sampling_from_exact_position_is_deterministic(tmp_path):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "artifact.json")
    seed = 3
    provider_a = CorpusBackedOpponentProvider(artifact_path, rng=random.Random(seed))
    provider_b = CorpusBackedOpponentProvider(artifact_path, rng=random.Random(seed))

    board = chess.Board()
    for uci in ["e2e4", "e7e5", "g1f3", "b8c6", "f1c4"]:
        board.push(chess.Move.from_uci(uci))

    choice_a = provider_a.choose_move(board)
    choice_b = provider_b.choose_move(board)

    assert choice_a.move == choice_b.move
    assert choice_a.candidate_summaries[0]["uci"] == "f8c5"
    assert choice_a.candidate_summaries[1]["uci"] == "g8f6"
    assert choice_a.move in board.legal_moves


def test_side_to_move_correctness_from_artifact_runtime(tmp_path):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "artifact.json")
    provider = CorpusBackedOpponentProvider(artifact_path, rng=random.Random(11))

    board = chess.Board()
    for uci in ["e2e4", "e7e5", "g1f3"]:
        board.push(chess.Move.from_uci(uci))

    choice = provider.choose_move(board)

    assert board.turn == chess.BLACK
    assert chess.Move.from_uci(choice.candidate_summaries[0]["uci"]) in board.legal_moves
    assert choice.move in board.legal_moves


def test_missing_engine_does_not_become_ordinary_fail():
    session = TrainingSession()
    session.player_color = chess.WHITE
    session.state = SessionState.PLAYER_TURN
    session.evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.ENGINE_UNAVAILABLE,
                reason_text="Engine binary missing.",
                played_move_uci="e2e4",
                played_move_san="e4",
                metadata={"engine_available": False},
            )
        ),
    )

    view = session.submit_user_move_uci("e2e4")

    assert view.state == SessionState.RESTART_PENDING
    assert view.last_evaluation is not None
    assert view.last_evaluation.reason_code == ReasonCode.ENGINE_UNAVAILABLE
    assert view.run_failed is False
    assert session.has_failed() is False
    assert session.has_authority_unavailable() is True
