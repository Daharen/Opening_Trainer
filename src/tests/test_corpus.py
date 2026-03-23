from __future__ import annotations

import json
import random
from pathlib import Path

import chess

from opening_trainer.zstd_compat import compress as zstd_compress

from opening_trainer.corpus import CorpusIngestor, RatingBandPolicy, load_artifact, normalize_position_key, save_artifact
from opening_trainer.opponent import CorpusBackedOpponentProvider, OpponentMoveChoice, OpponentProvider
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


def test_plain_pgn_ingestion_still_works():
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])

    assert artifact.source_files == (str(FIXTURE_PATH),)
    assert len(artifact.positions) > 0


def test_pgn_zst_ingestion_works(tmp_path):
    compressed_path = tmp_path / "sample_corpus.pgn.zst"
    compressed_path.write_bytes(zstd_compress(FIXTURE_PATH.read_bytes()))

    artifact = CorpusIngestor().build_artifact([str(compressed_path)])
    plain_artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])

    assert artifact.positions == plain_artifact.positions
    assert artifact.source_files == (str(compressed_path),)


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

from opening_trainer.bundle_corpus import normalize_builder_position_key


def _write_bundle(bundle_dir: Path, manifest: dict[str, object], rows: list[dict[str, object]]) -> Path:
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (data_dir / "aggregated_position_move_counts.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    return bundle_dir


def _sample_bundle_manifest(**overrides: object) -> dict[str, object]:
    manifest = {
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "payload_status": "ready",
    }
    manifest.update(overrides)
    return manifest


def test_builder_bundle_move_sampling_uses_raw_counts(tmp_path):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [
                    {"uci": "e2e4", "raw_count": 5},
                    {"uci": "d2d4", "raw_count": 1},
                ],
                "total_observed_count": 6,
            }
        ],
    )
    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(2))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "corpus_aggregate_bundle"
    assert {summary["uci"] for summary in choice.candidate_summaries} == {"e2e4", "d2d4"}
    assert {summary["effective_weight"] for summary in choice.candidate_summaries} == {5.0, 1.0}
    assert choice.raw_count in {5, 1}
    assert choice.total_observed_count == 6
    assert choice.candidate_summaries[0]["uci"] == "e2e4"


def test_bundle_absent_position_falls_back_to_stockfish_before_random(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(tmp_path / "bundle", _sample_bundle_manifest(), [])
    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(2))
    monkeypatch.setattr(provider.stockfish_provider, "choose_move", lambda current_board: OpponentMoveChoice(
        move=chess.Move.from_uci("e2e4"),
        position_key=normalize_builder_position_key(current_board),
        selected_via="stockfish_fallback",
        raw_count=0,
        effective_weight=1.0,
        total_observed_count=0,
        sparse=False,
        sparse_reason=None,
        fallback_applied=True,
        candidate_summaries=({"uci": "e2e4", "raw_count": 0, "effective_weight": 1.0},),
    ))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "stockfish_fallback"
    assert choice.move.uci() == "e2e4"


def test_bundle_illegal_uci_degrades_cleanly(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "e9e5", "raw_count": 3}], "total_observed_count": 3}],
    )
    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(2))
    monkeypatch.setattr(provider.stockfish_provider, "choose_move", lambda current_board: (_ for _ in ()).throw(FileNotFoundError("no engine")))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "random_legal_move"
    assert choice.fallback_applied is True
    assert "Stockfish fallback failed" in (choice.sparse_reason or "")


def test_directory_artifact_path_is_not_loaded_as_legacy_corpus(tmp_path, monkeypatch):
    board = chess.Board()
    bogus_artifact_dir = tmp_path / "selected_bundle_dir"
    bogus_artifact_dir.mkdir()
    provider = OpponentProvider(
        bundle_dir=None,
        artifact_path=str(bogus_artifact_dir),
        evaluator_config=TrainingSession().config,
        rng=random.Random(4),
    )
    monkeypatch.setattr(provider.stockfish_provider, "choose_move", lambda current_board: (_ for _ in ()).throw(FileNotFoundError("no engine")))

    choice = provider.choose_move_with_context(board)

    assert provider.corpus_provider is None
    assert choice.selected_via == "random_legal_move"
    assert choice.fallback_applied is True


def test_no_corpus_and_engine_unavailable_uses_random_fallback(monkeypatch):
    board = chess.Board()
    provider = OpponentProvider(bundle_dir=None, artifact_path=None, evaluator_config=TrainingSession().config, rng=random.Random(4))
    monkeypatch.setattr(provider.stockfish_provider, "choose_move", lambda current_board: (_ for _ in ()).throw(FileNotFoundError("no engine")))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "random_legal_move"
    assert choice.fallback_applied is True
