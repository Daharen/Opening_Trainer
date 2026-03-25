from __future__ import annotations

import json
import random
from pathlib import Path

import chess

from opening_trainer.opponent import BuilderAggregateOpponentProvider
from opening_trainer.timing import (
    TimingConditionedCorpusBundleLoader,
    TimingContext,
    apply_move_pressure_modulation,
    bucket_clock_pressure,
    bucket_opening_ply_band,
    bucket_prev_opp_think,
    sample_think_time_seconds,
)


def _write_timing_bundle(bundle_dir: Path) -> Path:
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "build_status": "aggregation_complete",
        "aggregate_position_file": "data/aggregated_position_move_counts.jsonl",
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "payload_status": "counts_preserved",
        "retained_ply_depth": 20,
        "time_control_id": "rapid_300_0",
        "initial_time_seconds": 300,
        "timing_overlay_file": "data/timing_overlay.json",
        "target_rating_band": "1200-1399",
    }
    row = {
        "position_key": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -",
        "total_observations": 100,
        "candidate_moves": [
            {"uci": "e2e4", "raw_count": 70},
            {"uci": "d2d4", "raw_count": 20},
            {"uci": "g1f3", "raw_count": 10},
        ],
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
    (data_dir / "aggregated_position_move_counts.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    (data_dir / "timing_overlay.json").write_text(json.dumps(overlay), encoding="utf-8")
    return bundle_dir


def test_timing_bundle_loader_and_fallback_resolution(tmp_path):
    bundle_dir = _write_timing_bundle(tmp_path / "bundle")
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
    bundle_dir = _write_timing_bundle(tmp_path / "bundle")
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

    assert choice.timing_overlay_active is True
    assert choice.move_pressure_profile_id == "mp_fast"
    assert choice.think_time_profile_id == "tt_fast"
    assert choice.sampled_think_time_seconds is not None
