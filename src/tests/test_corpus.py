from __future__ import annotations

import json
import random
import sqlite3
import threading
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

from opening_trainer.bundle_corpus import BuilderAggregateCorpusProvider, normalize_builder_position_key


def _write_bundle(bundle_dir: Path, manifest: dict[str, object], rows: list[dict[str, object]]) -> Path:
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (data_dir / "aggregated_position_move_counts.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    return bundle_dir


def _write_sqlite_bundle(bundle_dir: Path, manifest: dict[str, object], rows: list[dict[str, object]]) -> Path:
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    db_path = data_dir / "corpus.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE positions (id INTEGER PRIMARY KEY, position_key TEXT NOT NULL, side_to_move TEXT NOT NULL, total_observed_count INTEGER NOT NULL)")
        conn.execute("CREATE TABLE moves (id INTEGER PRIMARY KEY, position_id INTEGER NOT NULL, uci TEXT NOT NULL, raw_count INTEGER NOT NULL, FOREIGN KEY(position_id) REFERENCES positions(id))")
        for row in rows:
            cursor = conn.execute(
                "INSERT INTO positions(position_key, side_to_move, total_observed_count) VALUES (?, ?, ?)",
                (row["position_key"], row.get("side_to_move", "white"), int(row.get("total_observed_count", row.get("total_observations", 0)))),
            )
            position_id = int(cursor.lastrowid)
            for move in row.get("candidate_moves", []):
                conn.execute(
                    "INSERT INTO moves(position_id, uci, raw_count) VALUES (?, ?, ?)",
                    (position_id, move.get("uci") or move.get("move_key"), int(move["raw_count"])),
                )
        conn.commit()
    finally:
        conn.close()
    return bundle_dir


def _sample_bundle_manifest(**overrides: object) -> dict[str, object]:
    manifest = {
        "build_status": "aggregation_complete",
        "aggregate_position_file": "data/aggregated_position_move_counts.jsonl",
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "payload_status": "ready",
    }
    manifest.update(overrides)
    return manifest


def test_normalize_builder_position_key_omits_move_counters_for_start_position():
    board = chess.Board()

    assert normalize_builder_position_key(board) == "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -"


def test_normalize_builder_position_key_matches_builder_shape_after_e2e4():
    board = chess.Board()
    board.push(chess.Move.from_uci("e2e4"))

    assert normalize_builder_position_key(board) == "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq -"


def test_builder_bundle_provider_parses_real_builder_candidate_fields(tmp_path):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(payload_status="raw_aggregate_counts_present_non_final_trainer_payload"),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [
                    {"move_key": "e2e4", "move_key_format": "uci", "raw_count": 5},
                    {"move_key": "d2d4", "move_key_format": "uci", "raw_count": 1},
                ],
                "total_observations": 6,
            }
        ],
    )

    provider = BuilderAggregateCorpusProvider(bundle_dir)
    position = provider.position_index[normalize_builder_position_key(board)]

    assert [candidate.uci for candidate in position.candidates] == ["e2e4", "d2d4"]
    assert [candidate.raw_count for candidate in position.candidates] == [5, 1]
    assert position.total_observed_count == 6
    assert position.candidate_row_count == 2
    assert position.unsupported_candidate_row_count == 0


def test_builder_bundle_move_sampling_uses_real_builder_rows_from_initial_position(tmp_path):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [
                    {"move_key": "e2e4", "move_key_format": "uci", "raw_count": 5},
                    {"move_key": "d2d4", "move_key_format": "uci", "raw_count": 1},
                ],
                "total_observations": 6,
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
        corpus_lookup_reason_code="stockfish_fallback_used_after_corpus_miss",
        normalized_position_key=normalize_builder_position_key(current_board),
        candidate_row_count=0,
        legal_candidate_count=1,
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


def test_builder_bundle_move_sampling_uses_real_builder_rows_after_player_move(tmp_path):
    board = chess.Board()
    board.push(chess.Move.from_uci("e2e4"))
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [
                    {"move_key": "e7e5", "move_key_format": "uci", "raw_count": 4},
                    {"move_key": "c7c5", "move_key_format": "uci", "raw_count": 2},
                ],
                "total_observations": 6,
            }
        ],
    )
    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(1))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "corpus_aggregate_bundle"
    assert choice.move.uci() in {"e7e5", "c7c5"}
    assert {summary["uci"] for summary in choice.candidate_summaries} == {"e7e5", "c7c5"}


def test_bundle_malformed_candidate_rows_report_supported_move_diagnostic(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [{"move_key": "e2e4", "move_key_format": "san", "raw_count": 3}],
                "total_observations": 3,
            }
        ],
    )
    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(2))
    monkeypatch.setattr(provider.stockfish_provider, "choose_move", lambda current_board: OpponentMoveChoice(
        move=chess.Move.from_uci("d2d4"),
        position_key=normalize_builder_position_key(current_board),
        selected_via="stockfish_fallback",
        corpus_lookup_reason_code="stockfish_fallback_used_after_corpus_miss",
        normalized_position_key=normalize_builder_position_key(current_board),
        candidate_row_count=0,
        legal_candidate_count=1,
        raw_count=0,
        effective_weight=1.0,
        total_observed_count=0,
        sparse=False,
        sparse_reason=None,
        fallback_applied=True,
        candidate_summaries=({"uci": "d2d4", "raw_count": 0, "effective_weight": 1.0},),
    ))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "stockfish_fallback"
    assert provider.bundle_provider is not None
    assert provider.bundle_provider.last_lookup_diagnostic is not None
    assert "reason_code=position_row_found_but_no_supported_candidate_moves" in provider.bundle_provider.last_lookup_diagnostic
    assert "candidate_rows_loaded=1" in provider.bundle_provider.last_lookup_diagnostic


def test_bundle_illegal_uci_degrades_cleanly(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"move_key": "e7e5", "move_key_format": "uci", "raw_count": 3}], "total_observations": 3}],
    )
    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(2))
    monkeypatch.setattr(provider.stockfish_provider, "choose_move", lambda current_board: OpponentMoveChoice(
        move=chess.Move.from_uci("d2d4"),
        position_key=normalize_builder_position_key(current_board),
        selected_via="stockfish_fallback",
        corpus_lookup_reason_code="stockfish_fallback_used_after_corpus_miss",
        normalized_position_key=normalize_builder_position_key(current_board),
        candidate_row_count=0,
        legal_candidate_count=1,
        raw_count=0,
        effective_weight=1.0,
        total_observed_count=0,
        sparse=False,
        sparse_reason=None,
        fallback_applied=True,
        candidate_summaries=({"uci": "d2d4", "raw_count": 0, "effective_weight": 1.0},),
    ))

    choice = provider.choose_move_with_context(board)

    assert choice.selected_via == "stockfish_fallback"
    assert provider.bundle_provider is not None
    assert provider.bundle_provider.last_lookup_diagnostic is not None
    assert "reason_code=position_row_found_but_all_candidate_moves_illegal" in provider.bundle_provider.last_lookup_diagnostic
    assert "candidate_rows_loaded=1" in provider.bundle_provider.last_lookup_diagnostic
    assert "legal_candidates=0" in provider.bundle_provider.last_lookup_diagnostic


def test_bundle_rejects_unsupported_move_key_format(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(move_key_format="san"),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "e2e4", "raw_count": 3}], "total_observed_count": 3}],
    )

    provider = OpponentProvider(bundle_dir=str(bundle_dir), evaluator_config=TrainingSession().config, rng=random.Random(2))

    assert provider.bundle_provider is None
    assert "unsupported move_key_format 'san'" in provider.status_message
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


def test_builder_sqlite_bundle_lookup_reads_position_on_demand(tmp_path):
    board = chess.Board()
    bundle_dir = _write_sqlite_bundle(
        tmp_path / "sqlite_bundle",
        _sample_bundle_manifest(payload_format="sqlite"),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "side_to_move": "white",
                "candidate_moves": [
                    {"uci": "e2e4", "raw_count": 5},
                    {"uci": "d2d4", "raw_count": 2},
                ],
                "total_observed_count": 7,
            }
        ],
    )

    provider = BuilderAggregateCorpusProvider(bundle_dir)
    position = provider.lookup_position(normalize_builder_position_key(board))

    assert provider.metadata.payload_format == "sqlite"
    assert provider.position_index == {}
    assert position is not None
    assert [candidate.uci for candidate in position.candidates] == ["e2e4", "d2d4"]
    assert [candidate.raw_count for candidate in position.candidates] == [5, 2]


def test_builder_sqlite_bundle_lookup_is_thread_safe_across_loader_and_runtime_threads(tmp_path):
    board = chess.Board()
    position_key = normalize_builder_position_key(board)
    bundle_dir = _write_sqlite_bundle(
        tmp_path / "sqlite_bundle_threaded",
        _sample_bundle_manifest(payload_format="sqlite"),
        [
            {
                "position_key": position_key,
                "side_to_move": "white",
                "candidate_moves": [
                    {"uci": "e2e4", "raw_count": 9},
                    {"uci": "d2d4", "raw_count": 3},
                ],
                "total_observed_count": 12,
            }
        ],
    )

    provider = BuilderAggregateCorpusProvider(bundle_dir)
    result: dict[str, object] = {}

    def _lookup_on_runtime_thread() -> None:
        try:
            result["position"] = provider.lookup_position(position_key)
        except Exception as exc:  # pragma: no cover - explicit failure capture for thread join
            result["error"] = exc

    lookup_thread = threading.Thread(target=_lookup_on_runtime_thread)
    lookup_thread.start()
    lookup_thread.join(timeout=5)

    assert not lookup_thread.is_alive()
    assert "error" not in result
    position = result.get("position")
    assert position is not None
    assert position.total_observed_count == 12
    assert [candidate.uci for candidate in position.candidates] == ["e2e4", "d2d4"]
    assert [candidate.raw_count for candidate in position.candidates] == [9, 3]
