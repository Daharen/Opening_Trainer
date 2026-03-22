from __future__ import annotations

import json
from pathlib import Path

import chess

from opening_trainer.corpus import CorpusIngestor, save_artifact
from opening_trainer.evaluation import CanonicalJudgment, ReasonCode
from opening_trainer.evaluation.book import OpeningBookAuthority
from opening_trainer.evaluation.engine import EngineAuthority
from opening_trainer.runtime import RuntimeOverrides, corpus_status_detail, load_runtime_config
from opening_trainer.session import TrainingSession

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_corpus.pgn"


def test_runtime_artifact_auto_discovery_success(tmp_path, monkeypatch):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    data_dir = tmp_path / "data"
    artifact_path = save_artifact(artifact, data_dir / "opening_corpus.json")
    monkeypatch.chdir(tmp_path)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.corpus.available is True
    assert runtime.corpus.path == artifact_path
    detail = corpus_status_detail(runtime.corpus.path)
    assert "schema=1" in detail
    assert "rating_policy=both_players_in_band" in detail
    assert "positions=" in detail


def test_runtime_artifact_missing_is_explicit_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    runtime = load_runtime_config(RuntimeOverrides())
    session = TrainingSession(runtime_context=runtime)

    assert runtime.corpus.available is False
    assert "not found" in runtime.corpus.detail
    assert "provisional fallback" in session.opponent.status_message


def test_engine_path_configured_and_passed_through(tmp_path, monkeypatch):
    config_path = tmp_path / "runtime.json"
    config_path.write_text(json.dumps({"engine_executable_path": "/tmp/custom-stockfish", "engine_depth": 16}), encoding="utf-8")

    runtime = load_runtime_config(RuntimeOverrides(runtime_config_path=str(config_path)))
    session = TrainingSession(runtime_context=runtime)

    assert session.config.engine_path == "/tmp/custom-stockfish"
    assert session.evaluator.engine_authority.config.engine_path == "/tmp/custom-stockfish"
    assert session.config.engine_depth == 16


def test_invalid_engine_path_returns_authority_unavailable_not_fail():
    authority = EngineAuthority(load_runtime_config(RuntimeOverrides(engine_executable_path="/definitely/missing/stockfish")).evaluator_config)
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")

    result = authority.evaluate(board, move)

    assert result.available is False
    assert result.reason_code == ReasonCode.ENGINE_UNAVAILABLE
    session = TrainingSession(runtime_context=load_runtime_config(RuntimeOverrides(engine_executable_path="/definitely/missing/stockfish")))
    session.player_color = chess.WHITE
    session.state = session.state.PLAYER_TURN
    view = session.submit_user_move_uci("e2e4")
    assert view.last_evaluation is not None
    assert view.last_evaluation.canonical_judgment == CanonicalJudgment.AUTHORITY_UNAVAILABLE
    assert view.run_failed is False


def test_book_path_configured_and_passed_through(tmp_path):
    book_path = tmp_path / "book.bin"
    runtime = load_runtime_config(RuntimeOverrides(opening_book_path=str(book_path)))
    session = TrainingSession(runtime_context=runtime)

    assert session.runtime_context.book.path == book_path
    assert session.evaluator.book_authority.book_path is None


def test_missing_book_path_explicit_no_book_state(tmp_path):
    runtime = load_runtime_config(RuntimeOverrides(opening_book_path=str(tmp_path / "missing.bin")))

    assert runtime.book.available is False
    assert "missing" in runtime.book.detail


def test_book_authority_uses_polyglot_membership(monkeypatch, tmp_path):
    board = chess.Board()
    played_move = chess.Move.from_uci("e2e4")
    other_move = chess.Move.from_uci("d2d4")
    book_path = tmp_path / "book.bin"
    book_path.write_bytes(b"fake")

    class Entry:
        def __init__(self, move, weight=10, learn=0):
            self.move = move
            self.weight = weight
            self.learn = learn

    class Reader:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def find_all(self, candidate_board):
            assert candidate_board.board_fen() == board.board_fen()
            return [Entry(played_move), Entry(other_move)]

    monkeypatch.setattr("chess.polyglot.open_reader", lambda path: Reader())

    result = OpeningBookAuthority(book_path).evaluate(board, played_move)

    assert result.accepted is True
    assert result.available is True
    assert result.reason_code == ReasonCode.BOOK_HIT
    assert result.metadata["candidate_moves"] == ["e2e4", "d2d4"]


def test_startup_diagnostics_reflect_active_authorities(tmp_path):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "artifact.json")
    runtime = load_runtime_config(
        RuntimeOverrides(
            corpus_artifact_path=str(artifact_path),
            engine_executable_path="/definitely/missing/stockfish",
            opening_book_path=str(tmp_path / "missing.bin"),
        )
    )
    session = TrainingSession(runtime_context=runtime, mode="gui")
    session.player_color = chess.BLACK

    summary = session.runtime_context.startup_status(mode="GUI", user_color="BLACK")

    assert summary.mode == "GUI"
    assert "loaded" in summary.corpus_status
    assert "missing" in summary.book_status
    assert "missing" in summary.engine_status
    assert "Degraded mode" in summary.doctrine_status
    assert any(line.startswith("Corpus:") for line in summary.lines)


def test_cli_and_gui_share_same_runtime_resolution(tmp_path):
    config_path = tmp_path / "runtime.json"
    config_path.write_text(
        json.dumps(
            {
                "corpus_artifact_path": str(tmp_path / "artifact.json"),
                "engine_executable_path": "/tmp/stockfish",
                "opening_book_path": str(tmp_path / "book.bin"),
            }
        ),
        encoding="utf-8",
    )

    cli_runtime = load_runtime_config(RuntimeOverrides(runtime_config_path=str(config_path)))
    gui_runtime = load_runtime_config(RuntimeOverrides(runtime_config_path=str(config_path)))

    assert cli_runtime.corpus.path == gui_runtime.corpus.path
    assert cli_runtime.engine.path == gui_runtime.engine.path
    assert cli_runtime.book.path == gui_runtime.book.path
