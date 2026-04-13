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
from opening_trainer.runtime import (
    HARDCODED_DEV_OPENING_LOCKED_CONTENT_ROOT,
    RuntimeOverrides,
    load_runtime_config,
)
from opening_trainer.session import TrainingSession
from opening_trainer.settings import TrainerSettings


def _write_opening_locked_artifact(content_root: Path) -> Path:
    root = content_root / "opening_locked_mode"
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(json.dumps({"opening_count": 2}), encoding="utf-8")
    sqlite_path = root / "opening_locked_openings.sqlite"
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("CREATE TABLE source_files(source_file_id INTEGER PRIMARY KEY, file_path TEXT NOT NULL)")
        conn.execute("CREATE TABLE opening_nodes(node_id INTEGER PRIMARY KEY, node_name TEXT NOT NULL, node_kind TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE node_closure(ancestor_node_id INTEGER NOT NULL, descendant_node_id INTEGER NOT NULL, depth INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE positions(position_id INTEGER PRIMARY KEY, position_key TEXT NOT NULL UNIQUE, side_to_move TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE exact_lines(exact_line_id INTEGER PRIMARY KEY, opening_node_id INTEGER NOT NULL, terminal_position_id INTEGER)"
        )
        conn.execute(
            "CREATE TABLE path_memberships(position_id INTEGER NOT NULL, node_id INTEGER NOT NULL, remaining_plies INTEGER)"
        )
        conn.execute(
            "CREATE TABLE node_moves(node_id INTEGER NOT NULL, from_position_id INTEGER NOT NULL, move_uci TEXT NOT NULL, to_position_id INTEGER, is_canonical INTEGER NOT NULL, support_count INTEGER NOT NULL)"
        )
    return sqlite_path


def _add_family_aware_tables(sqlite_path: Path) -> None:
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("CREATE TABLE ui_tree(parent_node_name TEXT NOT NULL, child_node_name TEXT NOT NULL)")
        conn.execute("CREATE TABLE family_edges(parent_node_name TEXT NOT NULL, child_node_name TEXT NOT NULL)")
        conn.execute("CREATE TABLE family_memberships(family_node_name TEXT NOT NULL, member_node_name TEXT NOT NULL)")
        conn.execute("CREATE TABLE transposition_edges(from_node_name TEXT NOT NULL, to_node_name TEXT NOT NULL)")
        conn.commit()


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
    assert "checked content_root=" in runtime.opening_locked_artifact.detail
    assert "checked dev hardcoded fallback content_root=" in runtime.opening_locked_artifact.detail


def test_consumer_runtime_uses_content_root_without_hardcoded_dev_fallback(tmp_path, monkeypatch):
    content_root = tmp_path / "localappdata" / "OpeningTrainerContent"
    _write_opening_locked_artifact(content_root)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    monkeypatch.setenv("OPENING_TRAINER_RUNTIME_MODE", "consumer")

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.opening_locked_artifact.loaded is True
    assert str(content_root / "opening_locked_mode") in runtime.opening_locked_artifact.detail
    assert "dev hardcoded fallback" not in runtime.opening_locked_artifact.detail


def test_dev_runtime_uses_hardcoded_fallback_when_primary_content_root_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.chdir(repo_root)
    fallback_content_root = tmp_path / "seed_content"
    _write_opening_locked_artifact(fallback_content_root)
    monkeypatch.setattr("opening_trainer.runtime.HARDCODED_DEV_OPENING_LOCKED_CONTENT_ROOT", fallback_content_root)

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))

    assert runtime.opening_locked_artifact.loaded is True
    assert runtime.opening_locked_artifact.manifest_path == fallback_content_root / "opening_locked_mode" / "manifest.json"
    assert runtime.opening_locked_artifact.sqlite_path == fallback_content_root / "opening_locked_mode" / "opening_locked_openings.sqlite"
    assert "dev hardcoded fallback" in runtime.opening_locked_artifact.detail
    assert str(fallback_content_root / "opening_locked_mode") in runtime.opening_locked_artifact.detail


def test_dev_runtime_hardcoded_fallback_failure_is_explicit(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.chdir(repo_root)
    missing_fallback_root = tmp_path / "missing_seed_content"
    assert HARDCODED_DEV_OPENING_LOCKED_CONTENT_ROOT != missing_fallback_root
    monkeypatch.setattr("opening_trainer.runtime.HARDCODED_DEV_OPENING_LOCKED_CONTENT_ROOT", missing_fallback_root)

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))

    assert runtime.opening_locked_artifact.loaded is False
    detail = runtime.opening_locked_artifact.detail
    assert "checked content_root=" in detail
    assert f"checked dev hardcoded fallback content_root={missing_fallback_root}" in detail
    assert "opening-locked artifact unavailable" in detail


def test_opening_transition_classification_states(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Ruy Lopez', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (2, 'Sicilian Defense', 'exact_opening')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, 'k1', 'black')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (2, 'k2', 'black')")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (1, 1, 10)")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (2, 2, 10)")
        conn.commit()

    assert provider.classify_transition("k1", "Ruy Lopez").classification == OpeningTransitionClassification.SELECTED_OPENING_PRESERVED
    assert provider.classify_transition("k2", "Ruy Lopez").classification == OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING
    assert provider.classify_transition("k3", "Ruy Lopez").classification == OpeningTransitionClassification.LEFT_TO_UNNAMED


def test_opening_names_for_position_uses_positions_and_memberships(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Italian Game', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (2, 'Open Games', 'synthetic_family')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, 'position-a', 'white')")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (1, 2, 5)")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (1, 1, 3)")
        conn.commit()

    assert provider.opening_names_for_position("position-a") == ("Italian Game", "Open Games")


def test_family_aware_detection_and_root_descendant_listing(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    _add_family_aware_tables(sqlite_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO ui_tree(parent_node_name, child_node_name) VALUES ('French Defense', 'French Defense: Exchange Variation')")
        conn.execute("INSERT INTO ui_tree(parent_node_name, child_node_name) VALUES ('London System', 'London System: with Bd3')")
        conn.execute("INSERT INTO ui_tree(parent_node_name, child_node_name) VALUES ('London System', 'London System: with Be2')")
        conn.commit()

    assert provider.supports_family_aware() is True
    assert provider.supports_family_ui() is True
    assert provider.list_root_openings() == ["French Defense", "London System"]
    assert provider.list_family_root_names() == ["French Defense", "London System"]
    assert provider.list_descendant_openings("London System") == ["London System: with Bd3", "London System: with Be2"]
    assert provider.list_variation_names_for_family("London System") == ["London System: with Bd3", "London System: with Be2"]


def test_family_ui_detection_requires_ui_tree_family_memberships_and_transposition_edges(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("CREATE TABLE ui_tree(parent_node_name TEXT NOT NULL, child_node_name TEXT NOT NULL)")
        conn.execute("CREATE TABLE family_memberships(family_node_name TEXT NOT NULL, member_node_name TEXT NOT NULL)")
        conn.execute("CREATE TABLE transposition_edges(from_node_name TEXT NOT NULL, to_node_name TEXT NOT NULL)")
        conn.commit()

    assert provider.supports_family_ui() is True
    assert provider.supports_family_aware() is True


def test_resolve_effective_selected_opening_blank_variation_means_family_level(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)

    assert provider.resolve_effective_selected_opening("London System", "") == "London System"
    assert provider.resolve_effective_selected_opening("London System", "London System: with Bd3") == "London System: with Bd3"


def test_family_aware_allowed_space_and_transition_preservation(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    _add_family_aware_tables(sqlite_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO ui_tree(parent_node_name, child_node_name) VALUES ('English Opening', 'English Opening: Agincourt')")
        conn.execute("INSERT INTO family_memberships(family_node_name, member_node_name) VALUES ('English Opening', 'English Opening')")
        conn.execute("INSERT INTO family_memberships(family_node_name, member_node_name) VALUES ('English Opening', 'English Opening: Agincourt')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, 'k-english', 'white')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (2, 'k-other', 'white')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'English Opening', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (2, 'English Opening: Agincourt', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (3, 'Sicilian Defense', 'exact_opening')")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (1, 2, 6)")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (2, 3, 6)")
        conn.commit()

    assert "English Opening" in provider.list_root_openings()
    family_allowed = set(provider.resolve_allowed_opening_space("English Opening"))
    variation_allowed = set(provider.resolve_allowed_opening_space("English Opening: Agincourt"))
    assert family_allowed == {"English Opening", "English Opening: Agincourt"}
    assert variation_allowed == {"English Opening: Agincourt"}
    assert provider.classify_transition("k-english", "English Opening", allowed_opening_space=family_allowed).classification == OpeningTransitionClassification.SELECTED_OPENING_PRESERVED
    assert provider.classify_transition("k-other", "English Opening", allowed_opening_space=family_allowed).classification == OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING


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

        key_e5 = normalize_builder_position_key(key_e5_board)
        key_c5 = normalize_builder_position_key(key_c5_board)
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Ruy Lopez', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (2, 'Sicilian Defense', 'exact_opening')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, ?, 'white')", (key_e5,))
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (2, ?, 'white')", (key_c5,))
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (1, 1, 10)")
        conn.execute("INSERT INTO path_memberships(position_id, node_id, remaining_plies) VALUES (2, 2, 10)")
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
        from opening_trainer.bundle_corpus import normalize_builder_position_key

        start_key = normalize_builder_position_key(chess.Board())
        after_d4 = chess.Board()
        after_d4.push(chess.Move.from_uci("d2d4"))
        after_d4_key = normalize_builder_position_key(after_d4)
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, \"Queen's Gambit\", 'exact_opening')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, ?, 'white')", (start_key,))
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (2, ?, 'black')", (after_d4_key,))
        conn.execute(
            "INSERT INTO node_moves(node_id, from_position_id, move_uci, to_position_id, is_canonical, support_count) VALUES (1, 1, 'd2d4', 2, 1, 10)"
        )
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
        after_d4 = board.copy(stack=True)
        after_d4.push(chess.Move.from_uci("d2d4"))
        after_d4_key = normalize_builder_position_key(after_d4)
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, \"Queen's Gambit\", 'exact_opening')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, ?, 'white')", (start_key,))
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (2, ?, 'black')", (after_d4_key,))
        conn.execute(
            "INSERT INTO node_moves(node_id, from_position_id, move_uci, to_position_id, is_canonical, support_count) VALUES (1, 1, 'd2d4', 2, 1, 10)"
        )
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


def test_list_exact_openings_excludes_synthetic_family_nodes(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Italian Game', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (2, 'Open Games', 'synthetic_family')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (3, 'Ruy Lopez', 'exact_opening')")
        conn.commit()

    assert provider.list_exact_opening_names() == ["Italian Game", "Ruy Lopez"]


def test_legacy_flat_artifact_roots_fallback_to_exact_openings(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Italian Game', 'exact_opening')")
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (2, 'Ruy Lopez', 'exact_opening')")
        conn.commit()

    assert provider.supports_family_ui() is False
    assert provider.list_family_root_names() == ["Italian Game", "Ruy Lopez"]
    assert provider.list_variation_names_for_family("Italian Game") == []


def test_canonical_continuation_follows_builder_node_moves(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Ruy Lopez', 'exact_opening')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (1, 'p1', 'white')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (2, 'p2', 'black')")
        conn.execute("INSERT INTO positions(position_id, position_key, side_to_move) VALUES (3, 'p3', 'white')")
        conn.execute(
            "INSERT INTO node_moves(node_id, from_position_id, move_uci, to_position_id, is_canonical, support_count) VALUES (1, 1, 'e2e4', 2, 1, 20)"
        )
        conn.execute(
            "INSERT INTO node_moves(node_id, from_position_id, move_uci, to_position_id, is_canonical, support_count) VALUES (1, 2, 'e7e5', 3, 1, 15)"
        )
        conn.execute(
            "INSERT INTO node_moves(node_id, from_position_id, move_uci, to_position_id, is_canonical, support_count) VALUES (1, 2, 'c7c5', 3, 0, 30)"
        )
        conn.commit()

    canonical = provider.canonical_continuation(position_key="p1", selected_opening_name="Ruy Lopez", max_plies=4)
    assert canonical.next_move_uci == "e2e4"
    assert canonical.line == ("e2e4", "e7e5")


def test_canonical_continuation_returns_empty_when_position_not_in_positions(tmp_path):
    sqlite_path = _write_opening_locked_artifact(tmp_path)
    provider = OpeningLockedProvider(sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("INSERT INTO opening_nodes(node_id, node_name, node_kind) VALUES (1, 'Ruy Lopez', 'exact_opening')")
        conn.commit()
    canonical = provider.canonical_continuation(position_key="missing", selected_opening_name="Ruy Lopez", max_plies=4)
    assert canonical.line == ()
