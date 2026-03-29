from __future__ import annotations

import json
import random
import sqlite3
from pathlib import Path

import chess

from opening_trainer.opponent import BuilderAggregateOpponentProvider
from opening_trainer.runtime import inspect_corpus_bundle
from opening_trainer.runtime import RuntimeOverrides, load_runtime_config
from opening_trainer.session import TrainingSession
from opening_trainer.models import SessionState
from opening_trainer.developer_timing import DeveloperTimingOverrideState, DeveloperTimingOverrideStore, parse_overlay_key_dimensions
from opening_trainer.developer_timing import LiveTimingDebugState
from opening_trainer.review.storage import ReviewStorage
from opening_trainer.timing import (
    TimingConditionedCorpusBundleLoader,
    TimingContext,
    apply_move_pressure_modulation,
    bucket_clock_pressure,
    bucket_opening_ply_band,
    bucket_prev_opp_think,
    sample_think_time_seconds,
)


def _write_exact_sqlite(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE positions (position_id INTEGER PRIMARY KEY, position_key TEXT NOT NULL, position_key_format TEXT NOT NULL, side_to_move TEXT NOT NULL, candidate_move_count INTEGER NOT NULL, total_observations INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE moves (move_id INTEGER PRIMARY KEY, position_id INTEGER NOT NULL, move_key TEXT NOT NULL, move_key_format TEXT NOT NULL, raw_count INTEGER NOT NULL, example_san TEXT, FOREIGN KEY(position_id) REFERENCES positions(position_id))"
        )
        cursor = conn.execute(
            "INSERT INTO positions(position_key, position_key_format, side_to_move, candidate_move_count, total_observations) VALUES (?, ?, ?, ?, ?)",
            ("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -", "fen_normalized", "white", 3, 100),
        )
        pid = int(cursor.lastrowid)
        conn.execute("INSERT INTO moves(position_id, move_key, move_key_format, raw_count, example_san) VALUES (?, ?, ?, ?, ?)", (pid, "e2e4", "uci", 70, "e4"))
        conn.execute("INSERT INTO moves(position_id, move_key, move_key_format, raw_count, example_san) VALUES (?, ?, ?, ?, ?)", (pid, "d2d4", "uci", 20, "d4"))
        conn.execute("INSERT INTO moves(position_id, move_key, move_key_format, raw_count, example_san) VALUES (?, ?, ?, ?, ?)", (pid, "g1f3", "uci", 10, "Nf3"))
        conn.commit()
    finally:
        conn.close()


def _write_compact_exact_sqlite_v2(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE positions (id INTEGER PRIMARY KEY, fen_normalized TEXT NOT NULL, total_observed_count INTEGER NOT NULL, candidate_count INTEGER NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE moves (id INTEGER PRIMARY KEY, parent_position_id INTEGER NOT NULL, uci TEXT NOT NULL, raw_count INTEGER NOT NULL, FOREIGN KEY(parent_position_id) REFERENCES positions(id))"
        )
        cursor = conn.execute(
            "INSERT INTO positions(fen_normalized, total_observed_count, candidate_count) VALUES (?, ?, ?)",
            ("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -", 100, 3),
        )
        pid = int(cursor.lastrowid)
        conn.execute("INSERT INTO moves(parent_position_id, uci, raw_count) VALUES (?, ?, ?)", (pid, "e2e4", 70))
        conn.execute("INSERT INTO moves(parent_position_id, uci, raw_count) VALUES (?, ?, ?)", (pid, "d2d4", 20))
        conn.execute("INSERT INTO moves(parent_position_id, uci, raw_count) VALUES (?, ?, ?)", (pid, "g1f3", 10))
        conn.commit()
    finally:
        conn.close()


def _write_compact_exact_sqlite_v2_normalized(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE positions ("
            "position_id INTEGER PRIMARY KEY, "
            "position_key_inspect TEXT NOT NULL, "
            "position_key_compact TEXT NOT NULL, "
            "total_observed_count INTEGER NOT NULL, "
            "candidate_count INTEGER NOT NULL)"
        )
        conn.execute("CREATE TABLE moves (move_id INTEGER PRIMARY KEY, uci_text TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE position_moves (position_id INTEGER NOT NULL, move_id INTEGER NOT NULL, raw_count INTEGER NOT NULL, "
            "FOREIGN KEY(position_id) REFERENCES positions(position_id), FOREIGN KEY(move_id) REFERENCES moves(move_id))"
        )
        cursor = conn.execute(
            "INSERT INTO positions(position_key_inspect, position_key_compact, total_observed_count, candidate_count) VALUES (?, ?, ?, ?)",
            (
                "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
                "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
                100,
                3,
            ),
        )
        pid = int(cursor.lastrowid)
        conn.execute("INSERT INTO moves(uci_text) VALUES (?)", ("e2e4",))
        move_e2e4 = int(conn.execute("SELECT move_id FROM moves WHERE uci_text = ?", ("e2e4",)).fetchone()[0])
        conn.execute("INSERT INTO moves(uci_text) VALUES (?)", ("d2d4",))
        move_d2d4 = int(conn.execute("SELECT move_id FROM moves WHERE uci_text = ?", ("d2d4",)).fetchone()[0])
        conn.execute("INSERT INTO moves(uci_text) VALUES (?)", ("g1f3",))
        move_g1f3 = int(conn.execute("SELECT move_id FROM moves WHERE uci_text = ?", ("g1f3",)).fetchone()[0])
        conn.execute("INSERT INTO position_moves(position_id, move_id, raw_count) VALUES (?, ?, ?)", (pid, move_e2e4, 70))
        conn.execute("INSERT INTO position_moves(position_id, move_id, raw_count) VALUES (?, ?, ?)", (pid, move_d2d4, 20))
        conn.execute("INSERT INTO position_moves(position_id, move_id, raw_count) VALUES (?, ?, ?)", (pid, move_g1f3, 10))
        conn.commit()
    finally:
        conn.close()


def _write_behavioral_profile_set(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE move_pressure_profiles (profile_id TEXT PRIMARY KEY, pressure_sensitivity REAL, decisiveness REAL, move_diversity REAL)")
        conn.execute("CREATE TABLE think_time_profiles (profile_id TEXT PRIMARY KEY, base_time_scale REAL, spread REAL, short_mass REAL, deep_think_tail_mass REAL, timeout_tail_mass REAL)")
        conn.execute("CREATE TABLE context_profile_map (context_key TEXT PRIMARY KEY, move_pressure_profile_id TEXT, think_time_profile_id TEXT)")
        conn.execute("INSERT INTO move_pressure_profiles VALUES ('mp_fast', 0.04, 0.7, 0.08)")
        conn.execute("INSERT INTO think_time_profiles VALUES ('tt_fast', 2.0, 1.0, 0.3, 0.2, 0.1)")
        conn.execute("INSERT INTO context_profile_map VALUES ('rapid_300_0|1200-1399|medium|short|01-10', 'mp_fast', 'tt_fast')")
        conn.execute("INSERT INTO context_profile_map VALUES ('rapid_300_0|1200-1399|medium|none|01-10', 'mp_fast', 'tt_fast')")
        conn.commit()
    finally:
        conn.close()


def _write_timing_bundle(
    bundle_dir: Path,
    *,
    native: bool,
    use_json_overlay: bool,
    exact_name: str = "exact_corpus.sqlite",
    context_keys: list[str] | None = None,
    include_time_control_id: bool = True,
    include_target_rating_band: bool = True,
    timing_overlay_scope: str | None = None,
    payload_format: str | None = None,
    payload_version: object | None = None,
    canonical_exact_payload_file: str | None = None,
    compatibility_exact_payload_file: str | None = None,
    compact_v2_exact_payload: bool = False,
    compact_v2_normalized_schema: bool = False,
) -> Path:
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "retained_ply_depth": 20,
        "initial_time_seconds": 300,
        "context_contract_version": "v1",
        "timing_overlay_policy_version": "v1",
    }
    if include_time_control_id:
        manifest["time_control_id"] = "rapid_300_0"
    if include_target_rating_band:
        manifest["target_rating_band"] = "1200-1399"
    if timing_overlay_scope is not None:
        manifest["timing_overlay_scope"] = timing_overlay_scope
    if native:
        manifest.update(
            {
                "build_status": "timing_conditioned_ready",
                "exact_corpus_file": f"data/{exact_name}",
                "behavioral_profile_set_file": "data/behavioral_profile_set.sqlite",
            }
        )
    else:
        manifest.update(
            {
                "build_status": "aggregation_complete",
                "position_key_format": "fen_normalized",
                "move_key_format": "uci",
                "payload_status": "counts_preserved",
                "sqlite_corpus_file": f"data/{exact_name}",
            }
        )
    if use_json_overlay:
        manifest["timing_overlay_file"] = "data/timing_overlay.json"
    if payload_format is not None:
        manifest["payload_format"] = payload_format
    if payload_version is not None:
        manifest["payload_version"] = payload_version
    if canonical_exact_payload_file is not None:
        manifest["canonical_exact_payload_file"] = canonical_exact_payload_file
    if compatibility_exact_payload_file is not None:
        manifest["compatibility_exact_payload_file"] = compatibility_exact_payload_file

    keys = context_keys or [
        "rapid_300_0|1200-1399|medium|short|01-10",
        "rapid_300_0|1200-1399|medium|none|01-10",
    ]
    context_profile_map = {
        key: {
            "move_pressure_profile_id": "mp_fast",
            "think_time_profile_id": "tt_fast",
        }
        for key in keys
    }
    overlay = {
        "context_contract_version": "v1",
        "timing_overlay_policy_version": "v1",
        "move_pressure_profiles": {
            "mp_fast": {"pressure_sensitivity": 0.04, "decisiveness": 0.7, "move_diversity": 0.08}
        },
        "think_time_profiles": {
            "tt_fast": {
                "base_time_scale": 2.0,
                "spread": 1.0,
                "short_mass": 0.3,
                "deep_think_tail_mass": 0.2,
                "timeout_tail_mass": 0.1,
            }
        },
        "context_profile_map": context_profile_map,
    }

    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    if compact_v2_exact_payload and compact_v2_normalized_schema:
        _write_compact_exact_sqlite_v2_normalized(data_dir / exact_name)
    elif compact_v2_exact_payload:
        _write_compact_exact_sqlite_v2(data_dir / exact_name)
    else:
        _write_exact_sqlite(data_dir / exact_name)
    if not native:
        row = {
            "position_key": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
            "total_observations": 100,
            "candidate_moves": [{"uci": "e2e4", "raw_count": 70}, {"uci": "d2d4", "raw_count": 20}, {"uci": "g1f3", "raw_count": 10}],
        }
        (data_dir / "aggregated_position_move_counts.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    _write_behavioral_profile_set(data_dir / "behavioral_profile_set.sqlite")
    if use_json_overlay:
        (data_dir / "timing_overlay.json").write_text(json.dumps(overlay), encoding="utf-8")
    return bundle_dir


def test_displayed_clock_seconds_applies_live_elapsed_to_active_player(monkeypatch):
    session = TrainingSession()
    session.player_color = chess.WHITE
    session.state = SessionState.PLAYER_TURN
    session.timed_state = type(
        "TimedState",
        (),
        {"white_remaining_ms": 10_000, "black_remaining_ms": 8_000},
    )()
    session._player_turn_started_at = 100.0
    monkeypatch.setattr("opening_trainer.session.time.monotonic", lambda: 100.6)

    white, black = session.displayed_clock_seconds()

    assert white == 9.401
    assert black == 8.0


def test_submit_user_move_uci_premove_execution_deducts_fixed_time_cost(monkeypatch):
    session = TrainingSession()
    session.player_color = chess.WHITE
    session.mode = "gui"
    session.state = SessionState.PLAYER_TURN
    session.timed_state = type(
        "TimedState",
        (),
        {
            "white_remaining_ms": 10_000,
            "black_remaining_ms": 10_000,
            "increment_seconds": 0.0,
            "previous_opponent_think_seconds": None,
        },
    )()
    session._player_turn_started_at = 50.0
    monkeypatch.setattr("opening_trainer.session.time.monotonic", lambda: 50.0)
    session.board = type(
        "BoardStub",
        (),
        {
            "is_legal": lambda self, move: move == "e2e4",
            "board": chess.Board(),
            "push": lambda self, move: chess.Move.from_uci(move),
            "turn": lambda self: chess.BLACK,
        },
    )()
    session._record_path_move = lambda *args, **kwargs: None
    session.evaluator = type(
        "EvaluatorStub",
        (),
        {
            "evaluate": lambda self, board_before, move, player_move_count: type(
                "Eval",
                (),
                {"canonical_judgment": None, "accepted": True, "reason_text": "ok"},
            )()
        },
    )()
    session._print_evaluation_feedback = lambda *args, **kwargs: None
    session._resolve_terminal_board_state = lambda: False
    session.required_player_moves = 99
    session.current_routing = None

    session.submit_user_move_uci("e2e4", premove_executed=True)

    assert session.timed_state.white_remaining_ms == 9_900


def test_timing_bundle_loader_accepts_native_manifest_and_exact_path(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True, exact_name="exact_corpus.sqlite")
    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)

    assert handle.bundle_kind == "timing_conditioned"
    assert handle.timing_overlay_available is True
    assert handle.overlay_source == "json_file"
    assert handle.exact_payload_path is not None
    assert handle.exact_payload_path.name == "exact_corpus.sqlite"



def test_timing_bundle_loader_supports_corpus_sqlite_name(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True, exact_name="corpus.sqlite")
    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)

    assert handle.exact_payload_path is not None
    assert handle.exact_payload_path.name == "corpus.sqlite"


def test_timing_bundle_loader_prefers_canonical_exact_payload_path_over_compatibility_alias(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
    )
    _write_exact_sqlite(bundle_dir / "data" / "corpus.sqlite")

    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)
    assert handle.exact_payload_path is not None
    assert handle.exact_payload_path.name == "exact_corpus.sqlite"


def test_timing_bundle_loader_supports_compact_exact_payload_v2_sqlite(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
        compact_v2_exact_payload=True,
    )
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(5))
    choice = provider.choose_move(chess.Board())

    assert choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert choice.move.uci() in {"e2e4", "d2d4", "g1f3"}



def test_live_session_path_uses_compact_v2_bundle_for_black_opponent_first_turn(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
        compact_v2_exact_payload=True,
    )
    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["increment_seconds"] = 0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_black_start"))
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()

    pending = session.prepare_pending_opponent_action()
    assert pending is not None
    assert pending.choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert pending.choice.bundle_kind == "timing_conditioned"
    assert pending.choice.exact_payload_path is not None
    assert "exact_corpus.sqlite" in pending.choice.exact_payload_path

    committed = session.commit_pending_opponent_action()
    assert committed is True
    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert session.board.board.move_stack


def test_live_session_path_uses_normalized_compact_v2_bundle_for_black_opponent_first_turn(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
        compact_v2_exact_payload=True,
        compact_v2_normalized_schema=True,
    )
    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["increment_seconds"] = 0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_black_start_normalized"))
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()

    pending = session.prepare_pending_opponent_action()
    assert pending is not None
    assert pending.choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert pending.choice.bundle_kind == "timing_conditioned"
    assert pending.choice.timing_overlay_source in {"json_file", "behavioral_profile_set_sqlite"}
    assert pending.choice.exact_payload_path is not None

    committed = session.commit_pending_opponent_action()
    assert committed is True
    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert session.board.board.move_stack


def test_training_session_binds_real_name_normalized_compact_v2_canonical_bundle(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
        compact_v2_exact_payload=True,
        compact_v2_normalized_schema=True,
    )
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_bind_real_names"))
    provider = session.opponent.bundle_provider

    assert provider is not None
    assert provider.bundle.exact_corpus is not None
    assert provider.bundle.exact_corpus.metadata.provider_label == "corpus_exact_bundle_sqlite_compact_v2"


def test_normalized_compact_v2_bundle_supports_ordinary_initial_position_selection_without_stockfish(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
        compact_v2_exact_payload=True,
        compact_v2_normalized_schema=True,
    )
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(5))
    choice = provider.choose_move(chess.Board())

    assert choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert choice.corpus_lookup_reason_code == "corpus_hit"
    assert choice.bundle_kind == "timing_conditioned"
    assert choice.timing_overlay_source in {"json_file", "behavioral_profile_set_sqlite"}
    assert choice.exact_payload_path is not None
    assert choice.move.uci() in {"e2e4", "d2d4", "g1f3"}
    assert choice.selected_via != "stockfish_fallback"


def test_normalized_compact_v2_live_path_keeps_bundle_aware_timing_diagnostics(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        compatibility_exact_payload_file="data/corpus.sqlite",
        compact_v2_exact_payload=True,
        compact_v2_normalized_schema=True,
    )
    manifest_path = bundle_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["increment_seconds"] = 0
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_diag_normalized"))
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()

    pending = session.prepare_pending_opponent_action()
    assert pending is not None
    committed = session.commit_pending_opponent_action()
    assert committed is True

    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via == "corpus_exact_bundle_sqlite_compact_v2"
    assert session.last_opponent_choice.bundle_kind == "timing_conditioned"
    assert session.timing_diagnostics.overlay_source in {"json_file", "behavioral_profile_set_sqlite"}
    assert session.last_opponent_choice.exact_payload_path is not None


def test_final_canonical_compact_v2_bundle_loader_failure_raises_loudly(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        exact_name="exact_corpus.sqlite",
        payload_format="sqlite_compact_v2",
        payload_version="2",
        canonical_exact_payload_file="data/missing_exact.sqlite",
        compatibility_exact_payload_file="data/exact_corpus.sqlite",
        compact_v2_exact_payload=True,
    )
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    try:
        TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_loud_fail"))
    except RuntimeError as exc:
        text = str(exc)
        assert "Failed to bind final canonical exact corpus bundle" in text
        assert "canonical_exact_payload" in text
        assert "missing_exact.sqlite" in text
    else:
        assert False, "Expected TrainingSession to fail loudly when final canonical compact-v2 bundle cannot be bound."


def test_timing_bundle_loader_reads_overlay_from_behavioral_profile_set_sqlite(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=False)
    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)

    assert handle.timing_overlay_available is True
    assert handle.overlay_source == "behavioral_profile_set_sqlite"
    context = TimingContext("rapid_300_0", "1200-1399", "medium", "short", "01-10")
    direct = handle.resolve_overlay(context)
    assert direct is not None
    assert direct.fallback_used is False



def test_timing_bundle_loader_and_fallback_resolution(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=False, use_json_overlay=True)
    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)

    assert handle.bundle_kind == "timing_conditioned"
    assert handle.timing_lookup_mode == "reduced_dynamic"
    assert handle.bundle_invariant_time_control_id == "rapid_300_0"
    assert handle.bundle_invariant_rating_band == "1200-1399"
    assert handle.timing_overlay_available is True
    context = TimingContext("rapid_300_0", "1200-1399", "medium", "short", "01-10")
    direct = handle.resolve_overlay(context)
    assert direct is not None
    assert direct.fallback_used is False

    fallback_context = TimingContext("rapid_300_0", "1200-1399", "medium", "instant", "01-10")
    fallback = handle.resolve_overlay(fallback_context)
    assert fallback is not None
    assert fallback.fallback_used is True


def test_legacy_aggregate_bundle_still_classifies_without_timing_markers(tmp_path):
    bundle_dir = tmp_path / "bundle"
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "build_status": "aggregation_complete",
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "payload_status": "counts_preserved",
        "sqlite_corpus_file": "data/exact_corpus.sqlite",
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    _write_exact_sqlite(data_dir / "exact_corpus.sqlite")
    row = {
        "position_key": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
        "total_observations": 100,
        "candidate_moves": [{"uci": "e2e4", "raw_count": 70}],
    }
    (data_dir / "aggregated_position_move_counts.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")

    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)
    assert handle.bundle_kind == "legacy_aggregate"
    assert handle.timing_lookup_mode == "full_key"



def test_single_scope_bundle_uses_reduced_dynamic_lookup_with_full_keys(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        context_keys=["rapid_300_0|400-599|medium|none|01-10"],
    )
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(3))
    board = chess.Board()

    choice = provider.choose_move(
        board,
        timing_context={
            "time_control_id": "mismatched_time_control",
            "mover_elo_band": "400-600",
            "remaining_ratio": 0.40,
            "remaining_seconds": 120.0,
            "prev_opp_think_seconds": None,
            "opening_ply": 1,
        },
    )

    assert choice.timing_overlay_active is True
    assert choice.timing_lookup_mode == "reduced_dynamic"
    assert choice.timing_invariants_ignored_for_match is True
    assert choice.timing_attempted_context_key == "medium|none|01-10"
    assert choice.timing_context_key == "medium|none|01-10"



def test_missing_rating_band_metadata_does_not_disable_overlay_matching(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        include_target_rating_band=False,
        context_keys=["rapid_300_0|1200-1399|medium|none|01-10"],
    )
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(3))

    choice = provider.choose_move(
        chess.Board(),
        timing_context={
            "time_control_id": "rapid_300_0",
            "mover_elo_band": "unknown",
            "remaining_ratio": 0.40,
            "remaining_seconds": 120.0,
            "prev_opp_think_seconds": None,
            "opening_ply": 1,
        },
    )
    assert choice.timing_overlay_active is True



def test_missing_time_control_metadata_does_not_disable_overlay_matching(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        include_time_control_id=False,
        context_keys=["rapid_300_0|1200-1399|medium|none|01-10"],
    )
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(3))

    choice = provider.choose_move(
        chess.Board(),
        timing_context={
            "time_control_id": "unknown",
            "mover_elo_band": "unknown",
            "remaining_ratio": 0.40,
            "remaining_seconds": 120.0,
            "prev_opp_think_seconds": None,
            "opening_ply": 1,
        },
    )
    assert choice.timing_overlay_active is True


def test_final_canonical_bundle_missing_time_contract_fails_loudly(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        include_time_control_id=False,
        canonical_exact_payload_file="data/exact_corpus.sqlite",
        payload_version="2",
    )
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_missing_contract"))

    try:
        session._build_timed_state_from_bundle()
    except ValueError as exc:
        assert "missing required timing contract fields" in str(exc)
    else:
        assert False, "Expected _build_timed_state_from_bundle() to fail for missing canonical timing fields."


def test_full_key_lookup_mode_still_supported_when_multi_scope_is_explicit(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        timing_overlay_scope="multi_scope",
        context_keys=["rapid_300_0|1200-1399|medium|none|01-10"],
    )
    handle = TimingConditionedCorpusBundleLoader().load(bundle_dir)
    assert handle.timing_lookup_mode == "full_key"

    unmatched = handle.resolve_overlay(TimingContext("blitz_180_0", "1600-1799", "medium", "none", "01-10"))
    assert unmatched is None



def test_unmatched_reduced_dynamic_lookup_reports_reduced_attempted_keys(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(3))

    choice = provider.choose_move(
        chess.Board(),
        timing_context={
            "time_control_id": "rapid_300_0",
            "mover_elo_band": "1200-1399",
            "remaining_ratio": 0.40,
            "remaining_seconds": 120.0,
            "prev_opp_think_bucket_override": "long",
            "opening_ply_band_override": "31+",
        },
    )

    assert choice.timing_lookup_mode == "reduced_dynamic"
    assert choice.timing_overlay_active is False
    assert choice.timing_attempted_context_key == "medium|long|31+"
    assert choice.timing_fallback_keys_attempted == ("medium|long|31+", "medium|none|31+", "medium|31+", "medium")



def test_unmatched_full_key_lookup_reports_full_attempted_keys(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path / "bundle",
        native=True,
        use_json_overlay=True,
        timing_overlay_scope="multi_scope",
    )
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(3))

    choice = provider.choose_move(
        chess.Board(),
        timing_context={
            "time_control_id": "rapid_300_0",
            "mover_elo_band": "1200-1399",
            "remaining_ratio": 0.40,
            "remaining_seconds": 120.0,
            "prev_opp_think_bucket_override": "long",
            "opening_ply_band_override": "31+",
        },
    )

    assert choice.timing_lookup_mode == "full_key"
    assert choice.timing_overlay_active is False
    assert choice.timing_attempted_context_key == "rapid_300_0|1200-1399|medium|long|31+"
    assert choice.timing_fallback_keys_attempted == (
        "rapid_300_0|1200-1399|medium|long|31+",
        "rapid_300_0|1200-1399|medium|none|31+",
        "rapid_300_0|1200-1399|medium|31+",
        "rapid_300_0|1200-1399|medium",
        "rapid_300_0|1200-1399",
    )



def test_bucket_helpers_and_modulation_sampler_are_deterministic():
    assert bucket_clock_pressure(0.05) == "critical"
    assert bucket_clock_pressure(0.24) == "low"
    assert bucket_prev_opp_think(None) == "none"
    assert bucket_prev_opp_think(1.5) == "instant"
    assert bucket_opening_ply_band(25) == "21-30"

    adjusted, summary = apply_move_pressure_modulation(
        [("e2e4", 100.0), ("d2d4", 40.0), ("g1f3", 10.0)],
        profile=type("P", (), {"pressure_sensitivity": 0.03, "decisiveness": 0.8, "move_diversity": 0.1})(),
        clock_pressure_bucket="critical",
    )
    assert abs(sum(weight for _, weight in adjusted) - 1.0) < 1e-9
    assert summary["strength"] > 0.0

    profile = type("T", (), {"base_time_scale": 2.0, "spread": 1.0, "short_mass": 0.3, "deep_think_tail_mass": 0.2, "timeout_tail_mass": 0.1})()
    rng = random.Random(7)
    sample_a = sample_think_time_seconds(profile, 50.0, rng=rng)
    rng = random.Random(7)
    sample_b = sample_think_time_seconds(profile, 50.0, rng=rng)
    assert sample_a == sample_b



def test_builder_aggregate_opponent_uses_overlay_profiles(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    provider = BuilderAggregateOpponentProvider(bundle_dir, rng=random.Random(3))
    board = chess.Board()

    choice = provider.choose_move(
        board,
        timing_context={
            "time_control_id": "rapid_300_0",
            "mover_elo_band": "1200-1399",
            "remaining_ratio": 0.40,
            "remaining_seconds": 120.0,
            "prev_opp_think_seconds": 6.0,
            "opening_ply": 1,
        },
    )

    assert choice.timing_overlay_available is True
    assert choice.timing_overlay_active is True
    assert choice.move_pressure_profile_id == "mp_fast"
    assert choice.think_time_profile_id == "tt_fast"
    assert choice.sampled_think_time_seconds is not None



def test_visible_delay_clamp_and_summary_is_explicit(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime)
    session.timed_state = session._build_timed_state_from_bundle()
    session.live_timing_debug_state = LiveTimingDebugState(
        bundle_path=str(bundle_dir),
        overlay_available=True,
        overlay_source="json_file",
        effective_context_key="rapid_300_0|1200-1399|medium|none|01-10",
        matched_context_key="rapid_300_0|1200-1399|medium|none|01-10",
        sampled_think_time_seconds=1.25,
        visible_delay_applied_seconds=0.2,
    )
    session.opponent_visible_delay_min_seconds = 0.01
    session.opponent_visible_delay_max_seconds = 0.02

    assert session._visible_opponent_delay_seconds(0.5) == 0.02
    assert session._visible_opponent_delay_seconds(0.001) == 0.01
    summary = session._timing_summary_text()
    assert "Opponent timing: active" in summary
    assert "Clocks W/B:" in summary



def test_runtime_bundle_inspection_accepts_native_timing_bundle(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=False)
    compatibility = inspect_corpus_bundle(bundle_dir)

    assert compatibility.available is True
    assert compatibility.bundle_kind == "timing_conditioned"
    assert "bundle_kind=timing_conditioned" in compatibility.detail


def test_developer_timing_override_state_persists_and_reloads(tmp_path):
    store = DeveloperTimingOverrideStore(tmp_path)
    saved = store.save(
        DeveloperTimingOverrideState(
            enabled=True,
            force_time_control_id="rapid_300_0",
            force_mover_elo_band="1200-1399",
            force_clock_pressure_bucket="low",
            force_prev_opp_think_bucket="short",
            force_opening_ply_band="11-20",
            force_ordinary_corpus_play=True,
            visible_delay_scale=0.5,
            visible_delay_min_seconds=0.1,
            visible_delay_max_seconds=0.9,
        )
    )
    loaded = store.load()

    assert saved == loaded
    assert loaded.enabled is True
    assert loaded.force_ordinary_corpus_play is True


def test_forced_context_values_override_native_runtime_context(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_forced"))
    session.timed_state = session._build_timed_state_from_bundle()
    session.update_developer_timing_overrides(
        DeveloperTimingOverrideState(
            enabled=True,
            force_time_control_id="blitz_180_0",
            force_mover_elo_band="1600-1799",
            force_clock_pressure_bucket="critical",
            force_prev_opp_think_bucket="long",
            force_opening_ply_band="31+",
        )
    )
    context, _native, _adjusted = session._build_opponent_timing_context()

    assert context is not None
    assert context["time_control_id"] == "blitz_180_0"
    assert context["mover_elo_band"] == "1600-1799"
    assert context["clock_pressure_bucket_override"] == "critical"
    assert context["prev_opp_think_bucket_override"] == "long"
    assert context["opening_ply_band_override"] == "31+"


def test_auto_mode_preserves_native_runtime_behavior(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_auto"))
    session.timed_state = session._build_timed_state_from_bundle()
    native, _native_raw, _native_adjusted = session._build_opponent_timing_context()
    session.update_developer_timing_overrides(DeveloperTimingOverrideState(enabled=True))
    overridden, _overridden_raw, _overridden_adjusted = session._build_opponent_timing_context()

    assert overridden == native


def test_force_ordinary_corpus_play_bypasses_review_predecessor_path(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_bypass"))
    session.update_developer_timing_overrides(DeveloperTimingOverrideState(enabled=True, force_ordinary_corpus_play=True))
    session.active_review_plan = type("Plan", (), {"predecessor_path": [{"side_to_move": "white", "move_uci": "e2e4", "fen_before": session.board.board.fen()}]})()
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()
    session.last_opponent_choice = None

    session._handle_opponent_turn()

    assert session.last_opponent_choice is not None
    assert session.last_opponent_choice.selected_via != "review_predecessor_path"
    assert session.timing_diagnostics.review_predecessor_bypassed is True


def test_discovered_overlay_key_dropdown_values_populate_from_context_profile_map():
    dimensions = parse_overlay_key_dimensions(
        [
            "rapid_300_0|1200-1399|medium|short|01-10",
            "rapid_600_5|1400-1599|low|instant|11-20",
        ]
    )

    assert dimensions["time_control_id"] == ["rapid_300_0", "rapid_600_5"]
    assert dimensions["mover_elo_band"] == ["1200-1399", "1400-1599"]
    assert "low" in dimensions["clock_pressure_bucket"]


def test_debug_diagnostics_update_after_move_selection(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_diag"))
    session.start_new_game()
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()

    session._handle_opponent_turn()

    assert session.timing_diagnostics.overlay_source in {"json_file", "absent", "behavioral_profile_set_sqlite"}
    assert session.timing_diagnostics.last_opponent_source is not None
    assert session.timing_diagnostics.bundle_path == str(bundle_dir)


def test_visible_delay_diagnostics_update_correctly_when_overlay_unmatched(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_unmatched"))
    session.start_new_game()
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()
    session.update_developer_timing_overrides(
        DeveloperTimingOverrideState(enabled=True, force_prev_opp_think_bucket="long", force_opening_ply_band="31+")
    )

    session._handle_opponent_turn()

    assert session.timing_diagnostics.effective_context_key is not None
    assert session.timing_diagnostics.lookup_mode == "reduced_dynamic"
    assert session.timing_diagnostics.fallback_keys_attempted
    assert session.timing_diagnostics.matched_context_key is None
    assert session.timing_diagnostics.visible_delay_reason in {"no_overlay_match", "sampled_think_time_missing"}


def test_live_timing_debug_state_initializes_from_loaded_bundle(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_initial_state"))

    assert session.timing_diagnostics.bundle_path == str(bundle_dir)
    assert session.timing_diagnostics.overlay_available is True
    assert session.timing_diagnostics.overlay_source == "json_file"


def test_timing_summary_and_diagnostics_use_same_context_key(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles_consistent_summary"))
    session.start_new_game()
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()

    session._handle_opponent_turn()

    summary = session._timing_summary_text()
    assert "Opponent timing:" in summary
    assert "Overlay source:" not in summary
    assert "Context:" not in summary
    assert session.timing_diagnostics.overlay_source in {"json_file", "inline manifest", "behavioral_profile_set_sqlite", "absent"}
    assert session.timing_diagnostics.effective_context_key is not None


def test_gui_mode_prepares_pending_opponent_action_without_blocking_sleep(tmp_path, monkeypatch):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, mode="gui", review_storage=ReviewStorage(tmp_path / "profiles_gui_nonblocking"))
    session.start_new_game()
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()
    previous_choice = session.last_opponent_choice
    sleep_calls: list[float] = []
    monkeypatch.setattr("opening_trainer.session.time.sleep", lambda seconds: sleep_calls.append(seconds))

    pending = session.prepare_pending_opponent_action()

    assert pending is not None
    assert session.pending_opponent_action is not None
    assert session.last_opponent_choice is previous_choice
    assert sleep_calls == []


def test_pending_opponent_action_commits_move_and_diagnostics(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=True)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, mode="gui", review_storage=ReviewStorage(tmp_path / "profiles_gui_commit"))
    session.start_new_game()
    session.player_color = chess.BLACK
    session.state = SessionState.OPPONENT_TURN
    session.timed_state = session._build_timed_state_from_bundle()
    stack_before = len(session.board.board.move_stack)

    pending = session.prepare_pending_opponent_action()
    committed = session.commit_pending_opponent_action()

    assert pending is not None
    assert committed is True
    assert len(session.board.board.move_stack) == stack_before + 1
    assert session.last_opponent_choice is not None
    assert session.timing_diagnostics.visible_delay_reason in {"applied", "no_overlay_match", "sampled_think_time_missing", "review_predecessor_path"}


def test_start_new_game_gui_player_start_does_not_start_clock(monkeypatch):
    session = TrainingSession(mode='gui')
    session.board.turn = lambda: chess.WHITE
    session.player_color = chess.WHITE
    monkeypatch.setattr('opening_trainer.session.random.choice', lambda choices: chess.WHITE)

    session.start_new_game()

    assert session.state == SessionState.PLAYER_TURN
    assert session._player_turn_started_at is None


def test_start_new_game_gui_opponent_start_does_not_auto_advance(monkeypatch):
    session = TrainingSession(mode='gui')
    monkeypatch.setattr('opening_trainer.session.random.choice', lambda choices: chess.BLACK)

    session.start_new_game()

    assert session.state == SessionState.OPPONENT_TURN
    assert session.pending_opponent_action is None
