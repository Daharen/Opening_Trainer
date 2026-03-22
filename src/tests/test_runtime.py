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
    assert runtime.engine.path == "/tmp/custom-stockfish"
    assert "configured value=/tmp/custom-stockfish" in runtime.engine.detail
    assert session.config.engine_depth == 16


def test_engine_path_cli_override_remains_literal_when_probed(tmp_path):
    engine_path = tmp_path / "bin" / "stockfish"
    engine_path.parent.mkdir(parents=True)
    engine_path.write_text("", encoding="utf-8")

    runtime = load_runtime_config(RuntimeOverrides(engine_executable_path=str(engine_path)))

    assert runtime.engine.path == str(engine_path)
    assert runtime.engine.available is True
    assert f"configured value={engine_path}" in runtime.engine.detail


def test_engine_path_environment_remains_literal_when_probed(tmp_path, monkeypatch):
    engine_path = tmp_path / "env" / "stockfish"
    engine_path.parent.mkdir(parents=True)
    engine_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("OPENING_TRAINER_ENGINE_PATH", str(engine_path))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.engine_executable_path == str(engine_path)
    assert runtime.engine.path == str(engine_path)
    assert runtime.engine.available is True
    assert "environment winner" in runtime.engine.detail
    assert f"configured value={engine_path}" in runtime.engine.detail


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
    book_path.write_bytes(b"book")
    runtime = load_runtime_config(RuntimeOverrides(opening_book_path=str(book_path)))
    session = TrainingSession(runtime_context=runtime)

    assert session.runtime_context.book.path == str(book_path)
    assert session.runtime_context.book.available is True
    assert session.evaluator.book_authority.book_path == book_path
    assert f"configured value={book_path}" in session.runtime_context.book.detail


def test_corpus_path_environment_remains_literal_when_probed(tmp_path, monkeypatch):
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "artifacts" / "opening_corpus.json")
    monkeypatch.setenv("OPENING_TRAINER_CORPUS_PATH", str(artifact_path))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.corpus_artifact_path == str(artifact_path)
    assert runtime.corpus.path == str(artifact_path)
    assert runtime.corpus.available is True
    assert "environment winner" in runtime.corpus.detail
    assert f"configured value={artifact_path}" in runtime.corpus.detail


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
    assert "configured value=" in summary.corpus_status
    assert "configured value=" in summary.book_status
    assert "configured value=" in summary.engine_status
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


def test_workspace_runtime_local_config_auto_discovery(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    workspace_root = tmp_path
    repo_root.mkdir()
    (workspace_root / "runtime.local.json").write_text(
        json.dumps({"engine_executable_path": "/tmp/workspace-stockfish", "engine_depth": 18}),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.engine_executable_path == "/tmp/workspace-stockfish"
    assert runtime.config_source == f"workspace-root default runtime config: {workspace_root / 'runtime.local.json'}"
    assert runtime.evaluator_config.engine_depth == 18


def test_explicit_runtime_config_overrides_workspace_runtime_local(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (tmp_path / "runtime.local.json").write_text(json.dumps({"engine_executable_path": "/tmp/workspace-stockfish"}), encoding="utf-8")
    explicit_config = repo_root / "explicit-runtime.json"
    explicit_config.write_text(json.dumps({"engine_executable_path": "/tmp/explicit-stockfish"}), encoding="utf-8")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides(runtime_config_path=str(explicit_config)))

    assert runtime.config.engine_executable_path == "/tmp/explicit-stockfish"
    assert runtime.config_source == f"CLI flag --runtime-config: {explicit_config}"


def test_workspace_engine_default_discovery(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    engine_path = tmp_path / "tools" / "stockfish" / "stockfish-windows-x86-64-avx2.exe"
    engine_path.parent.mkdir(parents=True)
    engine_path.write_text("", encoding="utf-8")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.engine.available is True
    assert runtime.engine.path == engine_path.resolve()
    assert runtime.engine.source == "workspace-default"
    assert "workspace-root default path" in runtime.engine.detail


def test_workspace_corpus_default_discovery(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "artifacts" / "opening_corpus.json")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.corpus.available is True
    assert runtime.corpus.path == artifact_path
    assert runtime.corpus.source == "workspace-default"
    assert "workspace-root default path" in runtime.corpus.detail


def test_workspace_book_default_discovery(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    book_path = tmp_path / "runtime" / "opening_book.bin"
    book_path.parent.mkdir(parents=True)
    book_path.write_bytes(b"book")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.book.available is True
    assert runtime.book.path == book_path.resolve()
    assert runtime.book.source == "workspace-default"
    assert "workspace-root default path" in runtime.book.detail


def test_cli_asset_override_beats_workspace_defaults(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_engine = tmp_path / "tools" / "stockfish" / "stockfish-windows-x86-64-avx2.exe"
    workspace_engine.parent.mkdir(parents=True)
    workspace_engine.write_text("", encoding="utf-8")
    cli_engine = repo_root / "bin" / "stockfish-cli"
    cli_engine.parent.mkdir(parents=True)
    cli_engine.write_text("", encoding="utf-8")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides(engine_executable_path=str(cli_engine)))

    assert runtime.engine.path == str(cli_engine)
    assert runtime.engine.source == "cli"
    assert "CLI winner" in runtime.engine.detail
    assert "configured value=" in runtime.engine.detail


def test_runtime_config_engine_override_beats_workspace_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_engine = tmp_path / "tools" / "stockfish" / "stockfish-windows-x86-64-avx2.exe"
    workspace_engine.parent.mkdir(parents=True)
    workspace_engine.write_text("", encoding="utf-8")
    config_path = repo_root / "runtime.json"
    config_path.write_text(json.dumps({"engine_executable_path": "/tmp/config-stockfish"}), encoding="utf-8")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides(runtime_config_path=str(config_path)))

    assert runtime.config.engine_executable_path == "/tmp/config-stockfish"
    assert runtime.engine.path == "/tmp/config-stockfish"
    assert runtime.engine.source == "runtime-config"
    assert "runtime-config winner" in runtime.engine.detail
    assert "configured value=/tmp/config-stockfish" in runtime.engine.detail


def test_environment_engine_override_beats_workspace_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_engine = tmp_path / "tools" / "stockfish" / "stockfish-windows-x86-64-avx2.exe"
    workspace_engine.parent.mkdir(parents=True)
    workspace_engine.write_text("", encoding="utf-8")
    env_engine = repo_root / "bin" / "env-stockfish"
    env_engine.parent.mkdir(parents=True)
    env_engine.write_text("", encoding="utf-8")
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("OPENING_TRAINER_ENGINE_PATH", str(env_engine))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.engine_executable_path == str(env_engine)
    assert runtime.engine.path == str(env_engine)
    assert runtime.engine.source == "environment"
    assert "environment winner" in runtime.engine.detail
    assert "configured value=" in runtime.engine.detail


def test_environment_book_override_beats_workspace_default(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_book = tmp_path / "runtime" / "opening_book.bin"
    workspace_book.parent.mkdir(parents=True)
    workspace_book.write_bytes(b"workspace-book")
    env_book = repo_root / "books" / "env-book.bin"
    env_book.parent.mkdir(parents=True)
    env_book.write_bytes(b"env-book")
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("OPENING_TRAINER_BOOK_PATH", str(env_book))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.opening_book_path == str(env_book)
    assert runtime.book.path == str(env_book)
    assert runtime.book.source == "environment"
    assert "configured value=" in runtime.book.detail


def test_show_runtime_reports_workspace_default_activation(tmp_path, monkeypatch, capsys):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (tmp_path / "runtime.local.json").write_text(json.dumps({"engine_executable_path": "/tmp/workspace-stockfish"}), encoding="utf-8")
    monkeypatch.chdir(repo_root)

    from opening_trainer.main import run

    run(["--show-runtime"])
    output = capsys.readouterr().out

    assert "Runtime config source: workspace-root default runtime config:" in output


def test_degraded_mode_remains_explicit_without_workspace_assets(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())
    summary = runtime.startup_status(mode="CLI", user_color="WHITE")

    assert runtime.corpus.available is False
    assert runtime.book.available is False
    assert runtime.engine.available is False
    assert "workspace-root default(s)" in runtime.corpus.detail
    assert "workspace-root default(s)" in runtime.book.detail
    assert "workspace-root defaults" in runtime.engine.detail
    assert "Degraded mode" in summary.doctrine_status
