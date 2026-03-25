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


def _write_timing_bundle(bundle_dir: Path, *, native: bool, use_json_overlay: bool, exact_name: str = "exact_corpus.sqlite") -> Path:
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "retained_ply_depth": 20,
        "time_control_id": "rapid_300_0",
        "initial_time_seconds": 300,
        "target_rating_band": "1200-1399",
        "context_contract_version": "v1",
        "timing_overlay_policy_version": "v1",
    }
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
        "context_profile_map": {
            "rapid_300_0|1200-1399|medium|short|01-10": {
                "move_pressure_profile_id": "mp_fast",
                "think_time_profile_id": "tt_fast",
            },
            "rapid_300_0|1200-1399|medium|none|01-10": {
                "move_pressure_profile_id": "mp_fast",
                "think_time_profile_id": "tt_fast",
            },
        },
    }

    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
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

    assert handle.timing_overlay_available is True
    context = TimingContext("rapid_300_0", "1200-1399", "medium", "short", "01-10")
    direct = handle.resolve_overlay(context)
    assert direct is not None
    assert direct.fallback_used is False

    fallback_context = TimingContext("rapid_300_0", "1200-1399", "medium", "instant", "01-10")
    fallback = handle.resolve_overlay(fallback_context)
    assert fallback is not None
    assert fallback.fallback_used is True



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
    session.last_opponent_choice = type(
        "Choice",
        (),
        {
            "timing_overlay_available": True,
            "timing_overlay_active": True,
            "timing_fallback_used": False,
            "visible_delay_applied": True,
            "timing_overlay_source": "json_file",
            "timing_context_key": "rapid_300_0|1200-1399|medium|none|01-10",
            "sampled_think_time_seconds": 1.25,
        },
    )()
    session.opponent_visible_delay_min_seconds = 0.01
    session.opponent_visible_delay_max_seconds = 0.02

    assert session._visible_opponent_delay_seconds(0.5) == 0.02
    assert session._visible_opponent_delay_seconds(0.001) == 0.01
    summary = session._timing_summary_text()
    assert "active_direct_visible_delay" in summary
    assert "Overlay source:" in summary



def test_runtime_bundle_inspection_accepts_native_timing_bundle(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle", native=True, use_json_overlay=False)
    compatibility = inspect_corpus_bundle(bundle_dir)

    assert compatibility.available is True
    assert compatibility.bundle_kind == "timing_conditioned"
    assert "bundle_kind=timing_conditioned" in compatibility.detail
