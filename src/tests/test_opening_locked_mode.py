from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import chess

from opening_trainer.evaluation import AuthoritySource, CanonicalJudgment, EvaluationResult, OverlayLabel, ReasonCode
from opening_trainer.opening_locked_mode import (
    OpeningLockedProvider,
    OpeningTransitionClassification,
    discover_opening_locked_artifact,
)
from opening_trainer.opponent import OpponentMoveChoice
from opening_trainer.runtime import RuntimeOverrides, load_runtime_config
from opening_trainer.session import TrainingSession
from opening_trainer.settings import TrainerSettings


def _write_opening_locked_artifact(content_root: Path) -> Path:
    root = content_root / "opening_locked_mode"
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(json.dumps({"opening_count": 2}), encoding="utf-8")
    sqlite_path = root / "opening_locked_openings.sqlite"
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("CREATE TABLE opening_membership(position_key TEXT, opening_name TEXT, is_exact INTEGER)")
        conn.execute("CREATE TABLE canonical_continuation(opening_name TEXT, position_key TEXT, move_uci TEXT, ply_index INTEGER)")
    return sqlite_path


def test_opening_locked_artifact_discovery_present_and_missing(tmp_path):
    missing = discover_opening_locked_artifact(tmp_path)
    assert missing.loaded is False

    _write_opening_locked_artifact(tmp_path)
    found = discover_opening_locked_artifact(tmp_path)
    assert found.loaded is True
    assert found.opening_count == 2


def test_dev_runtime_reports_opening_locked_unavailable_when_no_discovery_winner(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))

    assert runtime.opening_locked_artifact.loaded is False
    assert "dev discovery candidates checked" in runtime.opening_locked_artifact.detail


def test_opening_transition_classification_states(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_membership(position_key, opening_name, is_exact) VALUES (?, ?, 1)", ("k1", "Ruy Lopez"))
        conn.execute("INSERT INTO opening_membership(position_key, opening_name, is_exact) VALUES (?, ?, 1)", ("k2", "Sicilian Defense"))
        conn.commit()

    assert provider.classify_transition("k1", "Ruy Lopez").classification == OpeningTransitionClassification.SELECTED_OPENING_PRESERVED
    assert provider.classify_transition("k2", "Ruy Lopez").classification == OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING
    assert provider.classify_transition("k3", "Ruy Lopez").classification == OpeningTransitionClassification.LEFT_TO_UNNAMED


def test_opponent_filter_rejects_other_named_and_allows_unnamed(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    board = chess.Board()
    board.push(chess.Move.from_uci("e2e4"))
    key_e5_board = board.copy(stack=True)
    key_e5_board.push(chess.Move.from_uci("e7e5"))
    key_c5_board = board.copy(stack=True)
    key_c5_board.push(chess.Move.from_uci("c7c5"))
    with sqlite3.connect(sqlite_path) as conn:
        from opening_trainer.bundle_corpus import normalize_builder_position_key

        conn.execute("INSERT INTO opening_membership(position_key, opening_name, is_exact) VALUES (?, ?, 1)", (normalize_builder_position_key(key_e5_board), "Ruy Lopez"))
        conn.execute("INSERT INTO opening_membership(position_key, opening_name, is_exact) VALUES (?, ?, 1)", (normalize_builder_position_key(key_c5_board), "Sicilian Defense"))
        conn.commit()

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))
    session = TrainingSession(runtime_context=runtime)
    session.opening_locked_provider = provider
    session.opening_locked_state.enabled = True
    session.opening_locked_state.selected_opening_name = "Ruy Lopez"

    choice = OpponentMoveChoice(
        move=chess.Move.from_uci("c7c5"),
        position_key="",
        selected_via="corpus",
        corpus_lookup_reason_code="corpus_hit",
        normalized_position_key="",
        candidate_row_count=3,
        legal_candidate_count=3,
        raw_count=1,
        effective_weight=1.0,
        total_observed_count=3,
        sparse=False,
        sparse_reason=None,
        fallback_applied=False,
        candidate_summaries=(
            {"uci": "c7c5", "raw_count": 10, "effective_weight": 10.0},
            {"uci": "e7e5", "raw_count": 5, "effective_weight": 5.0},
            {"uci": "a7a6", "raw_count": 3, "effective_weight": 3.0},
        ),
    )

    filtered = session._opening_locked_filter_opponent_choice(board, choice)
    assert filtered.move.uci() in {"e7e5", "a7a6"}
    assert filtered.move.uci() != "c7c5"


def test_user_move_opening_exit_reason_and_release_behavior(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO canonical_continuation(opening_name, position_key, move_uci, ply_index) VALUES (?, ?, ?, ?)", ("Queen's Gambit", chess.STARTING_FEN, "d2d4", 1))
        conn.commit()
    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))
    session = TrainingSession(runtime_context=runtime)
    session.opening_locked_provider = OpeningLockedProvider(sqlite_path)
    session.update_settings(
        TrainerSettings(
            training_mode="manual",
            smart_profile_enabled=False,
            opening_locked_mode_enabled=True,
            selected_opening_name="Queen's Gambit",
        )
    )
    session.opening_locked_state.enabled = True
    session.opening_locked_state.selected_opening_name = "Queen's Gambit"
    session.state = session.state.PLAYER_TURN
    session.player_color = chess.WHITE
    session.evaluator.evaluate = lambda *args, **kwargs: EvaluationResult(
        accepted=True,
        canonical_judgment=CanonicalJudgment.BETTER,
        overlay_label=OverlayLabel.GOOD,
        reason_code=ReasonCode.ENGINE_PASS,
        reason_text="Accepted by ordinary policy.",
        authority_source=AuthoritySource.ENGINE,
        move_uci="e2e4",
        legal_move_confirmed=True,
        preferred_move_uci="d2d4",
        preferred_move_san="d4",
        metadata={},
    )

    view = session.submit_user_move_uci("e2e4")
    assert view.last_evaluation is not None
    assert view.last_evaluation.reason_code == ReasonCode.OPENING_EXIT_BEFORE_OPPONENT

    session.start_new_game()
    session.state = session.state.PLAYER_TURN
    session.board.board = chess.Board()
    session.opening_locked_state.lock_released_by_opponent = True
    session.submit_user_move_uci("e2e4")
    assert session.last_evaluation is not None
    assert session.last_evaluation.reason_code != ReasonCode.OPENING_EXIT_BEFORE_OPPONENT


def test_opening_exit_correction_uses_canonical_line(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    board = chess.Board()
    from opening_trainer.bundle_corpus import normalize_builder_position_key

    start_key = normalize_builder_position_key(board)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO canonical_continuation(opening_name, position_key, move_uci, ply_index) VALUES (?, ?, ?, ?)", ("Queen's Gambit", start_key, "d2d4", 1))
        conn.commit()

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))
    session = TrainingSession(runtime_context=runtime)
    session.opening_locked_provider = OpeningLockedProvider(sqlite_path)
    session.opening_locked_state.enabled = True
    session.opening_locked_state.selected_opening_name = "Queen's Gambit"
    session.last_evaluation = EvaluationResult(
        accepted=False,
        canonical_judgment=CanonicalJudgment.FAIL,
        overlay_label=OverlayLabel.INACCURACY,
        reason_code=ReasonCode.OPENING_EXIT_BEFORE_OPPONENT,
        reason_text="opening exit",
        authority_source=AuthoritySource.ENGINE,
        move_uci="e2e4",
        legal_move_confirmed=True,
        metadata={},
    )

    line = session.get_corrective_continuation(board, "d2d4", max_plies=4)
    assert line
    assert line[0][0] == "d2d4"


def test_opening_locked_requested_without_selected_opening_stays_ordinary_mode(tmp_path):
    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))
    session = TrainingSession(runtime_context=runtime)

    session.update_settings(
        TrainerSettings(
            training_mode="manual",
            smart_profile_enabled=False,
            opening_locked_mode_enabled=True,
            selected_opening_name=None,
        )
    )

    assert session.settings.opening_locked_mode_enabled is True
    assert session.opening_locked_state.enabled is False


def test_selected_opening_persists_while_opening_locked_toggle_off(tmp_path):
    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))
    session = TrainingSession(runtime_context=runtime)

    session.update_settings(
        TrainerSettings(
            training_mode="manual",
            smart_profile_enabled=False,
            opening_locked_mode_enabled=False,
            selected_opening_name="Italian Game",
        )
    )

    assert session.settings.selected_opening_name == "Italian Game"
    assert session.opening_locked_state.enabled is False
