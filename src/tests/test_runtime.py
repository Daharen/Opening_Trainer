from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import chess

import opening_trainer.session_logging as session_logging
from opening_trainer.bundle_corpus import normalize_builder_position_key
from opening_trainer.corpus import CorpusIngestor, save_artifact
from opening_trainer.evaluation import AuthoritySource, CanonicalJudgment, EvaluationResult, OverlayLabel, ReasonCode, EngineAuthorityResult
from opening_trainer.opponent import OpponentMoveChoice
from opening_trainer.evaluation.book import OpeningBookAuthority
from opening_trainer.evaluation.engine import EngineAuthority
from opening_trainer.runtime import RuntimeOverrides, corpus_status_detail, load_runtime_config
from opening_trainer.review.storage import ReviewStorage
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.models import SessionState
from opening_trainer.evaluation import BookAuthorityResult
from opening_trainer.session import TrainingSession

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_corpus.pgn"


def _reset_session_logger(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(session_logging.SESSION_LOG_DIR_ENV, str(tmp_path / "sessions"))
    monkeypatch.delenv(session_logging.SESSION_LOG_PATH_ENV, raising=False)
    monkeypatch.delenv(session_logging.SESSION_ID_ENV, raising=False)
    monkeypatch.delenv(session_logging.CONSOLE_MIRROR_ENV, raising=False)
    session_logging.reset_logger_for_tests()


class StubBookAuthority:
    def __init__(self, result):
        self.result = result

    def evaluate(self, board_before_move, played_move):
        return self.result


class StubEngineAuthority:
    def __init__(self, result):
        self.result = result

    def evaluate(self, board_before_move, played_move):
        return self.result


BOOK_MISS = BookAuthorityResult(False, False, ReasonCode.BOOK_UNAVAILABLE, 'Book authority unavailable for this position.', metadata={'book_available': False})


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
        conn.execute(
            "CREATE TABLE positions (position_id INTEGER PRIMARY KEY, position_key TEXT NOT NULL, position_key_format TEXT NOT NULL, side_to_move TEXT NOT NULL, candidate_move_count INTEGER NOT NULL, total_observations INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE moves (move_id INTEGER PRIMARY KEY, position_id INTEGER NOT NULL, move_key TEXT NOT NULL, move_key_format TEXT NOT NULL, raw_count INTEGER NOT NULL, example_san TEXT, FOREIGN KEY(position_id) REFERENCES positions(position_id))"
        )
        for row in rows:
            position_key = row["position_key"]
            side_to_move = row.get("side_to_move", "white")
            total = int(row.get("total_observed_count", row.get("total_observations", 0)))
            candidate_rows = row.get("candidate_moves", [])
            cursor = conn.execute(
                "INSERT INTO positions(position_key, position_key_format, side_to_move, candidate_move_count, total_observations) VALUES (?, ?, ?, ?, ?)",
                (position_key, "fen_normalized", side_to_move, len(candidate_rows), total),
            )
            position_id = int(cursor.lastrowid)
            for move in candidate_rows:
                move_key = move.get("uci") or move.get("move_key")
                conn.execute(
                    "INSERT INTO moves(position_id, move_key, move_key_format, raw_count, example_san) VALUES (?, ?, ?, ?, ?)",
                    (position_id, move_key, "uci", int(move["raw_count"]), move.get("example_san")),
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
        "retained_ply_depth": 10,
    }
    manifest.update(overrides)
    return manifest


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
    assert "Stockfish fallback before random legal fallback" in session.opponent.status_message


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


def test_invalid_engine_path_returns_authority_unavailable_not_fail(tmp_path):
    overrides = RuntimeOverrides(
        engine_executable_path="/definitely/missing/stockfish",
        opening_book_path=str(tmp_path / "definitely-missing-book.bin"),
    )
    runtime = load_runtime_config(overrides)
    authority = EngineAuthority(runtime.evaluator_config)
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")

    result = authority.evaluate(board, move)

    assert result.available is False
    assert result.reason_code == ReasonCode.ENGINE_UNAVAILABLE
    assert runtime.book.available is False
    session = TrainingSession(runtime_context=runtime)
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


def test_training_depth_clamps_to_bundle_cap_and_minimum(tmp_path):
    bundle_dir = _write_bundle(tmp_path / "bundle", _sample_bundle_manifest(retained_ply_depth=14, payload_status="raw_aggregate_counts_present_non_final_trainer_payload"), [])
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert session.max_supported_training_depth() == 7
    assert session.update_settings(session.settings.__class__(True, 1, True)).active_training_ply_depth == 2
    assert session.update_settings(session.settings.__class__(True, 9, True)).active_training_ply_depth == 7
    assert session.update_settings(session.settings.__class__(True, 4, True)).active_training_ply_depth == 4


def test_training_depth_controls_run_completion(tmp_path):
    session = TrainingSession(review_storage=ReviewStorage(tmp_path / 'runtime' / 'profiles'))
    session.current_routing = session.router.select(session.active_profile_id, [])
    session.player_color = chess.WHITE
    session.state = session.state.PLAYER_TURN
    session.update_settings(session.settings.__class__(True, 2, True))
    session.evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(True, True, ReasonCode.ENGINE_PASS, 'Accepted by engine.', best_move_uci='e2e4', best_move_san='e4', played_move_uci='e2e4', played_move_san='e4', cp_loss=0, metadata={'engine_available': True})
        ),
    )

    first = session.submit_user_move_uci('e2e4')
    session.board.reset()
    session.state = session.state.PLAYER_TURN
    second = session.submit_user_move_uci('e2e4')

    assert first.run_passed is False
    assert second.run_passed is True


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


def test_environment_engine_override_beats_workspace_runtime_local_config(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_engine = tmp_path / "workspace-stockfish"
    workspace_engine.write_text("", encoding="utf-8")
    env_engine = repo_root / "bin" / "env-stockfish"
    env_engine.parent.mkdir(parents=True)
    env_engine.write_text("", encoding="utf-8")
    (tmp_path / "runtime.local.json").write_text(
        json.dumps({"engine_executable_path": str(workspace_engine)}),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("OPENING_TRAINER_ENGINE_PATH", str(env_engine))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.engine_executable_path == str(env_engine)
    assert runtime.engine.path == str(env_engine)
    assert runtime.engine.source == "environment"
    assert runtime.config_source == f"workspace-root default runtime config: {tmp_path / 'runtime.local.json'}"
    assert "environment winner" in runtime.engine.detail



def test_explicit_runtime_config_engine_beats_environment_override(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    explicit_engine = repo_root / "bin" / "explicit-stockfish"
    explicit_engine.parent.mkdir(parents=True)
    explicit_engine.write_text("", encoding="utf-8")
    env_engine = repo_root / "bin" / "env-stockfish"
    env_engine.write_text("", encoding="utf-8")
    explicit_config = repo_root / "explicit-runtime.json"
    explicit_config.write_text(json.dumps({"engine_executable_path": str(explicit_engine)}), encoding="utf-8")
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("OPENING_TRAINER_ENGINE_PATH", str(env_engine))

    runtime = load_runtime_config(RuntimeOverrides(runtime_config_path=str(explicit_config)))

    assert runtime.config.engine_executable_path == str(explicit_engine)
    assert runtime.engine.path == str(explicit_engine)
    assert runtime.engine.source == "runtime-config"
    assert "runtime-config winner" in runtime.engine.detail



def test_workspace_runtime_local_config_beats_default_discovery_without_env_override(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_default_engine = tmp_path / "tools" / "stockfish" / "stockfish-windows-x86-64-avx2.exe"
    workspace_default_engine.parent.mkdir(parents=True)
    workspace_default_engine.write_text("", encoding="utf-8")
    configured_engine = tmp_path / "configured-stockfish"
    configured_engine.write_text("", encoding="utf-8")
    (tmp_path / "runtime.local.json").write_text(
        json.dumps({"engine_executable_path": str(configured_engine)}),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.engine_executable_path == str(configured_engine)
    assert runtime.engine.path == str(configured_engine)
    assert runtime.engine.source == "workspace-runtime-config"
    assert "workspace runtime.local.json winner" in runtime.engine.detail



def test_environment_book_override_beats_workspace_runtime_local_config(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    workspace_book = tmp_path / "workspace-book.bin"
    workspace_book.write_bytes(b"workspace-book")
    env_book = repo_root / "books" / "env-book.bin"
    env_book.parent.mkdir(parents=True)
    env_book.write_bytes(b"env-book")
    (tmp_path / "runtime.local.json").write_text(
        json.dumps({"opening_book_path": str(workspace_book)}),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("OPENING_TRAINER_BOOK_PATH", str(env_book))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.opening_book_path == str(env_book)
    assert runtime.book.path == str(env_book)
    assert runtime.book.source == "environment"
    assert "environment winner" in runtime.book.detail


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


def test_show_runtime_reports_workspace_default_activation(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (tmp_path / "runtime.local.json").write_text(json.dumps({"engine_executable_path": "/tmp/workspace-stockfish"}), encoding="utf-8")
    monkeypatch.chdir(repo_root)
    _reset_session_logger(tmp_path, monkeypatch)

    from opening_trainer.main import run

    run(["--show-runtime"])
    output = "\n".join(session_logging.get_session_logger().visible_lines())

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


def test_corpus_bundle_cli_override_beats_legacy_corpus(tmp_path):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "selected_bundle",
        _sample_bundle_manifest(),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [{"uci": "e2e4", "raw_count": 3}],
                "total_observed_count": 3,
            }
        ],
    )
    legacy_artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    legacy_path = save_artifact(legacy_artifact, tmp_path / "opening_corpus.json")

    runtime = load_runtime_config(
        RuntimeOverrides(corpus_bundle_dir=str(bundle_dir), corpus_artifact_path=str(legacy_path))
    )

    assert runtime.config.corpus_bundle_dir == str(bundle_dir)
    assert runtime.corpus.available is True
    assert runtime.corpus.path == str(bundle_dir.resolve())
    assert "position_key_format=fen_normalized" in runtime.corpus.detail


def test_corpus_bundle_environment_override_is_resolved(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "env_bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "d2d4", "raw_count": 1}], "total_observed_count": 1}],
    )
    monkeypatch.setenv("OPENING_TRAINER_CORPUS_BUNDLE_DIR", str(bundle_dir))

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.corpus_bundle_dir == str(bundle_dir)
    assert runtime.corpus.available is True
    assert runtime.corpus.source == "environment"
    assert "environment winner" in runtime.corpus.detail


def test_training_session_keeps_bundle_dir_out_of_legacy_artifact_loader(tmp_path):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "selected_bundle",
        _sample_bundle_manifest(),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [{"uci": "e2e4", "raw_count": 3}],
                "total_observed_count": 3,
            }
        ],
    )

    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert session.opponent.bundle_dir == bundle_dir
    assert session.opponent.artifact_path is None
    assert session.opponent.bundle_provider is not None
    assert session.opponent.corpus_provider is None
    assert "loaded corpus bundle" in session.opponent.status_message

    choice = session.opponent.choose_move_with_context(board)

    assert choice.selected_via == "corpus_aggregate_bundle"
    assert choice.move.uci() == "e2e4"


def test_sqlite_bundle_selected_and_loaded_without_legacy_artifact(tmp_path):
    board = chess.Board()
    bundle_dir = _write_sqlite_bundle(
        tmp_path / "sqlite_bundle",
        _sample_bundle_manifest(payload_format="sqlite"),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "side_to_move": "white",
                "candidate_moves": [{"uci": "e2e4", "raw_count": 7}, {"uci": "d2d4", "raw_count": 2}],
                "total_observed_count": 9,
            }
        ],
    )

    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert runtime.corpus.available is True
    assert "payload_format='sqlite'" in runtime.corpus.detail
    assert session.opponent.bundle_provider is not None
    assert session.opponent.corpus_provider is None

    choice = session.opponent.choose_move_with_context(board)

    assert choice.selected_via == "corpus_aggregate_bundle_sqlite"
    assert {summary["uci"] for summary in choice.candidate_summaries} == {"e2e4", "d2d4"}


def test_bundle_without_payload_format_prefers_sqlite_payload_when_present(tmp_path):
    board = chess.Board()
    bundle_dir = _write_sqlite_bundle(
        tmp_path / "sqlite_without_manifest_flag",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "e2e4", "raw_count": 3}], "total_observed_count": 3}],
    )

    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    choice = session.opponent.choose_move_with_context(board)

    assert runtime.corpus.available is True
    assert "payload_format='sqlite'" in runtime.corpus.detail
    assert choice.selected_via == "corpus_aggregate_bundle_sqlite"


def test_real_builder_bundle_payload_status_is_accepted(tmp_path):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "selected_bundle",
        _sample_bundle_manifest(payload_status="raw_aggregate_counts_present_non_final_trainer_payload"),
        [
            {
                "position_key": normalize_builder_position_key(board),
                "candidate_moves": [{"uci": "e2e4", "raw_count": 3}],
                "total_observed_count": 3,
            }
        ],
    )

    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert runtime.corpus.available is True
    assert "builder_payload_status='raw_aggregate_counts_present_non_final_trainer_payload'" in runtime.corpus.detail
    assert session.opponent.bundle_provider is not None
    assert session.opponent.corpus_provider is None

    choice = session.opponent.choose_move_with_context(board)

    assert choice.selected_via == "corpus_aggregate_bundle"
    assert choice.move.uci() == "e2e4"


def test_unsupported_bundle_manifest_degrades_to_legacy_corpus(tmp_path):
    bundle_dir = _write_bundle(
        tmp_path / "bad_bundle",
        _sample_bundle_manifest(position_key_format="unsupported"),
        [],
    )
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "opening_corpus.json")

    runtime = load_runtime_config(
        RuntimeOverrides(corpus_bundle_dir=str(bundle_dir), corpus_artifact_path=str(artifact_path))
    )

    assert runtime.corpus.available is True
    assert runtime.corpus.path == str(artifact_path)
    assert "unsupported position_key_format 'unsupported'" in runtime.corpus.detail
    assert "falling back to legacy corpus artifact" in runtime.corpus.detail


def test_bundle_missing_declared_aggregate_payload_degrades_cleanly(tmp_path):
    bundle_dir = tmp_path / "bad_bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(_sample_bundle_manifest(aggregate_position_file="data/missing_counts.jsonl")),
        encoding="utf-8",
    )
    artifact = CorpusIngestor().build_artifact([str(FIXTURE_PATH)])
    artifact_path = save_artifact(artifact, tmp_path / "opening_corpus.json")

    runtime = load_runtime_config(
        RuntimeOverrides(corpus_bundle_dir=str(bundle_dir), corpus_artifact_path=str(artifact_path))
    )

    assert runtime.corpus.available is True
    assert runtime.corpus.path == str(artifact_path)
    assert "aggregate payload is missing" in runtime.corpus.detail
    assert "falling back to legacy corpus artifact" in runtime.corpus.detail


def test_runtime_config_bundle_path_from_workspace_runtime_local(tmp_path, monkeypatch):
    board = chess.Board()
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    bundle_dir = _write_bundle(
        tmp_path / "workspace_bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "e2e4", "raw_count": 1}], "total_observed_count": 1}],
    )
    (tmp_path / "runtime.local.json").write_text(json.dumps({"corpus_bundle_dir": str(bundle_dir)}), encoding="utf-8")
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.config.corpus_bundle_dir == str(bundle_dir)
    assert runtime.corpus.available is True
    assert runtime.corpus.source == "workspace-runtime-config"


def test_startup_summary_mentions_opponent_source_order(tmp_path):
    runtime = load_runtime_config(RuntimeOverrides(engine_executable_path="/missing/stockfish"))
    summary = runtime.startup_status(mode="CLI", user_color="WHITE")

    assert any("Opponent source order:" in line for line in summary.lines)
    assert any("Random fallback remains enabled only as the last-ditch opponent source." == line for line in summary.lines)


def _accepted_evaluation(move_uci: str) -> EvaluationResult:
    return EvaluationResult(
        accepted=True,
        canonical_judgment=CanonicalJudgment.BETTER,
        overlay_label=OverlayLabel.GOOD,
        reason_code=ReasonCode.ENGINE_PASS,
        reason_text="accepted for session-flow regression test",
        authority_source=AuthoritySource.ENGINE,
        move_uci=move_uci,
        legal_move_confirmed=True,
    )


def test_session_opponent_turn_uses_bundle_move_for_black_start_position(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "e2e4", "raw_count": 3}], "total_observed_count": 3}],
    )
    _reset_session_logger(tmp_path, monkeypatch)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)
    session.board.reset()
    session.player_color = chess.BLACK
    session.state = session.state.OPPONENT_TURN
    monkeypatch.setattr(session.opponent.stockfish_provider, "choose_move", lambda board: (_ for _ in ()).throw(AssertionError("stockfish should not be used on corpus hit")))

    session.advance_until_user_turn()
    output = "\n".join(session_logging.get_session_logger().visible_lines())

    assert session.board.board.move_stack[0].uci() == "e2e4"
    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "corpus_aggregate_bundle"
    assert session.last_opponent_choice.corpus_lookup_reason_code == "corpus_hit"
    assert "reason=corpus_hit" in output
    assert "candidate_rows=1" in output
    assert "legal_candidates=1" in output


def test_session_player_move_reply_uses_bundle_move_for_white(tmp_path, monkeypatch):
    reply_board = chess.Board("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1")
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(reply_board), "candidate_moves": [{"uci": "e7e5", "raw_count": 4}], "total_observed_count": 4}],
    )
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)
    session.board.reset()
    session.player_color = chess.WHITE
    session.state = session.state.PLAYER_TURN
    monkeypatch.setattr(session.evaluator, "evaluate", lambda board, move, count: _accepted_evaluation(move.uci()))
    monkeypatch.setattr(session.opponent.stockfish_provider, "choose_move", lambda board: (_ for _ in ()).throw(AssertionError("stockfish should not be used on corpus hit")))

    session.submit_user_move_uci("e2e4")

    assert len(session.board.board.move_stack) == 2
    assert session.board.board.move_stack[1].uci() == "e7e5"
    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "corpus_aggregate_bundle"
    assert session.last_opponent_choice.corpus_lookup_reason_code == "corpus_hit"




def test_session_bundle_diagnostic_reports_actual_normalized_lookup_key(tmp_path, monkeypatch):
    board = chess.Board()
    bundle_dir = _write_bundle(
        tmp_path / "bundle",
        _sample_bundle_manifest(),
        [{"position_key": normalize_builder_position_key(board), "candidate_moves": [{"uci": "e2e4", "raw_count": 3}], "total_observed_count": 3}],
    )
    _reset_session_logger(tmp_path, monkeypatch)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)
    session.board.reset()
    session.player_color = chess.BLACK
    session.state = session.state.OPPONENT_TURN
    monkeypatch.setattr(session.opponent.stockfish_provider, "choose_move", lambda board: (_ for _ in ()).throw(AssertionError("stockfish should not be used on corpus hit")))

    session.advance_until_user_turn()
    output = "\n".join(session_logging.get_session_logger().visible_lines())

    assert f"position={normalize_builder_position_key(board)}" in output
    assert " 0 1" not in output

def test_session_opponent_turn_falls_back_to_stockfish_after_bundle_miss(tmp_path, monkeypatch):
    bundle_dir = _write_bundle(tmp_path / "bundle", _sample_bundle_manifest(), [])
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)
    session.board.reset()
    session.player_color = chess.BLACK
    session.state = session.state.OPPONENT_TURN
    monkeypatch.setattr(
        session.opponent.stockfish_provider,
        "choose_move",
        lambda board: OpponentMoveChoice(
            move=chess.Move.from_uci("d2d4"),
            position_key=board.fen(),
            selected_via="stockfish_fallback",
            corpus_lookup_reason_code="stockfish_fallback_used_after_corpus_miss",
            normalized_position_key=board.fen(),
            candidate_row_count=0,
            legal_candidate_count=1,
            raw_count=0,
            effective_weight=1.0,
            total_observed_count=0,
            sparse=False,
            sparse_reason=None,
            fallback_applied=True,
            candidate_summaries=({"uci": "d2d4", "raw_count": 0, "effective_weight": 1.0},),
        ),
    )

    session.advance_until_user_turn()

    assert session.board.board.move_stack[0].uci() == "d2d4"
    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "stockfish_fallback"
    assert session.last_opponent_choice.corpus_lookup_reason_code == "stockfish_fallback_used_after_corpus_miss"


def test_session_opponent_turn_uses_random_after_bundle_miss_and_stockfish_failure(tmp_path, monkeypatch):
    bundle_dir = _write_bundle(tmp_path / "bundle", _sample_bundle_manifest(), [])
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)
    session.board.reset()
    session.player_color = chess.BLACK
    session.state = session.state.OPPONENT_TURN
    monkeypatch.setattr(
        session.opponent.stockfish_provider,
        "choose_move",
        lambda board: (_ for _ in ()).throw(FileNotFoundError("no engine")),
    )
    monkeypatch.setattr(session.opponent.random_provider.rng, "choice", lambda legal_moves: chess.Move.from_uci("g1f3"))

    session.advance_until_user_turn()

    assert session.board.board.move_stack[0].uci() == "g1f3"
    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "random_legal_move"
    assert session.last_opponent_choice.corpus_lookup_reason_code == "random_fallback_used_after_all_failures"
    assert "Stockfish fallback failed" in (session.last_opponent_choice.sparse_reason or "")



def test_settings_store_persists_shell_defaults_and_last_bundle(tmp_path):
    from opening_trainer.settings import TrainerSettings, TrainerSettingsStore

    store = TrainerSettingsStore(tmp_path)
    saved = store.save(
        TrainerSettings(
            good_moves_acceptable=False,
            active_training_ply_depth=3,
            side_panel_visible=False,
            move_list_visible=True,
            last_bundle_path='  /tmp/example_bundle  ',
            last_corpus_catalog_root='  /tmp/corpus_root  ',
        )
    )

    assert saved.side_panel_visible is False
    assert saved.move_list_visible is True
    assert saved.last_bundle_path == '/tmp/example_bundle'
    assert saved.last_corpus_catalog_root == '/tmp/corpus_root'

    loaded = store.load(maximum_depth=5)
    assert loaded == saved



def test_main_run_defaults_to_gui_without_interactive_bundle_prompt(monkeypatch):
    from opening_trainer import main as trainer_main

    calls = {}

    monkeypatch.setattr(trainer_main, 'load_runtime_config', lambda overrides: type('Runtime', (), {'config': type('Config', (), {'strict_assets': False})()})())

    def fake_launch_gui(runtime_context=None):
        calls['runtime_context'] = runtime_context

    import opening_trainer.ui.gui_app as gui_app
    monkeypatch.setattr(gui_app, 'launch_gui', fake_launch_gui)

    trainer_main.run([])

    assert hasattr(calls['runtime_context'], 'config')


def test_training_depth_uses_manifest_identity_fallback_for_ply30_bundle(tmp_path):
    bundle_dir = _write_bundle(
        tmp_path / 'otb_ply30_bundle',
        _sample_bundle_manifest(retained_ply_depth='not-a-number', bundle_id='trainer_ply30'),
        [],
    )
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert session.bundle_retained_ply_depth() == 30
    assert session.max_supported_training_depth() == 15


def test_training_depth_missing_metadata_falls_back_conservatively(tmp_path):
    bundle_dir = _write_bundle(
        tmp_path / 'bundle_without_depth',
        _sample_bundle_manifest(retained_ply_depth=None, bundle_id='trainer_bundle'),
        [],
    )
    manifest = json.loads((bundle_dir / 'manifest.json').read_text(encoding='utf-8'))
    manifest.pop('retained_ply_depth', None)
    (bundle_dir / 'manifest.json').write_text(json.dumps(manifest), encoding='utf-8')

    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert session.bundle_retained_ply_depth() is None
    assert session.max_supported_training_depth() == 5
    assert 'fall back to Stockfish' in runtime.corpus.detail


def test_training_depth_prefers_manifest_max_supported_player_moves(tmp_path):
    bundle_dir = _write_bundle(
        tmp_path / 'bundle_with_canonical_depth',
        _sample_bundle_manifest(retained_ply_depth=12, max_supported_player_moves=9),
        [],
    )
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)

    assert session.bundle_retained_ply_depth() == 18
    assert session.max_supported_training_depth() == 9


def test_terminal_player_checkmate_inside_envelope_is_success(tmp_path):
    session = TrainingSession(review_storage=ReviewStorage(tmp_path / 'runtime' / 'profiles'))
    session.current_routing = session.router.select(session.active_profile_id, [])
    session.player_color = chess.WHITE
    session.state = session.state.PLAYER_TURN
    session.update_settings(session.settings.__class__(True, 4, True))
    session.board.board = chess.Board('6k1/5Q2/6K1/8/8/8/8/8 w - - 0 1')
    session.evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(True, True, ReasonCode.ENGINE_PASS, 'Accepted by engine.', best_move_uci='f7g7', best_move_san='Qg7#', played_move_uci='f7g7', played_move_san='Qg7#', cp_loss=0, metadata={'engine_available': True})
        ),
    )

    view = session.submit_user_move_uci('f7g7')

    assert view.run_passed is True
    assert view.awaiting_user_input is False
    assert view.state == SessionState.RESTART_PENDING
    assert 'terminal win' in view.last_outcome.reason.lower()


def test_terminal_opponent_mate_does_not_loop_waiting_for_more_moves(tmp_path):
    session = TrainingSession(review_storage=ReviewStorage(tmp_path / 'runtime' / 'profiles'))
    session.current_routing = session.router.select(session.active_profile_id, [])
    session.player_color = chess.WHITE
    session.state = session.state.OPPONENT_TURN
    session.board.board = chess.Board('7k/8/8/8/8/7k/6q1/7K b - - 0 1')
    session.opponent.choose_move_with_context = lambda board: OpponentMoveChoice(
        chess.Move.from_uci('g2f1'),
        board.fen(),
        'scripted_mate',
        'scripted_terminal_test',
        normalize_builder_position_key(board),
        1,
        1,
        1,
        1.0,
        1,
        False,
        None,
        False,
        ({'uci': 'g2f1', 'raw_count': 1, 'effective_weight': 1.0},),
    )

    view = session.advance_until_user_turn()

    assert view.run_failed is True
    assert view.awaiting_user_input is False
    assert view.state == SessionState.RESTART_PENDING
    assert 'defeated' in view.last_outcome.reason.lower()
