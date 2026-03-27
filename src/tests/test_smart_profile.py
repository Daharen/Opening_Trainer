from __future__ import annotations

import json
from pathlib import Path

from opening_trainer.review.storage import ReviewStorage
from opening_trainer.runtime import RuntimeOverrides, load_runtime_config
from opening_trainer.session import TrainingSession
from opening_trainer.smart_profile import (
    HIGHEST_CORPUS_BACKED_LEVEL,
    LEVEL_BY_INDEX,
    SMART_PROFILE_LEVELS,
    SmartProfileService,
    resolve_track_category,
)


def _service(tmp_path: Path) -> SmartProfileService:
    storage = ReviewStorage(tmp_path / "runtime" / "profiles")
    return SmartProfileService(storage, "default")


def _bundle(tmp_path: Path, *, time_control: str = "600+0", minimum: int = 1000, maximum: int = 1200) -> Path:
    bundle_dir = tmp_path / f"bundle_{time_control.replace('+', '_')}"
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "build_status": "aggregation_complete",
                "aggregate_position_file": "data/aggregated_position_move_counts.jsonl",
                "payload_status": "raw_aggregate_counts_present_non_final_trainer_payload",
                "position_key_format": "fen_normalized",
                "move_key_format": "uci",
                "retained_ply_depth": 12,
                "time_control_id": time_control,
                "initial_time_seconds": int(time_control.split("+")[0]),
                "increment_seconds": int(time_control.split("+")[1]),
                "target_rating_band": {"minimum": minimum, "maximum": maximum},
            }
        ),
        encoding="utf-8",
    )
    (data_dir / "aggregated_position_move_counts.jsonl").write_text("", encoding="utf-8")
    return bundle_dir


def test_time_control_mapping_exact_controls_only():
    assert resolve_track_category("600+0") == ("rapid", "600+0")
    assert resolve_track_category("300+0") == ("blitz", "300+0")
    assert resolve_track_category("120+1") == ("bullet", "120+1")
    assert resolve_track_category(" 600+0 ") == ("rapid", "600+0")
    assert resolve_track_category("180+0") is None


def test_track_state_is_independent_and_persistent(tmp_path):
    service = _service(tmp_path)
    rapid = service.state.get_track_state("rapid", "600+0")
    blitz = service.state.get_track_state("blitz", "300+0")
    rapid.current_level = 6
    rapid.wins_toward_promotion = 7
    service.save()

    reloaded = _service(tmp_path)
    reloaded_rapid = reloaded.state.get_track_state("rapid", "600+0")
    reloaded_blitz = reloaded.state.get_track_state("blitz", "300+0")
    assert reloaded_rapid.current_level == 6
    assert reloaded_rapid.wins_toward_promotion == 7
    assert reloaded_blitz.current_level == blitz.current_level
    assert reloaded_blitz.wins_toward_promotion == 0


def test_only_ordinary_corpus_counts_and_review_channels_do_not_count(tmp_path):
    service = _service(tmp_path)
    channels = ["scheduled_review", "boosted_review", "extreme_urgency_review", "immediate_retry", "srs_due_review", "stubborn_extreme_repeat"]
    for channel in channels:
        eligibility = service.evaluate_eligibility(
            routing_source=channel,
            bundle_available=True,
            time_control_id="600+0",
            bundle_rating_band="400-600",
            required_turns=3,
            good_accepted=True,
        )
        assert eligibility.eligible is False
        service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="400-600")
    state = service.state.get_track_state("rapid", "600+0")
    assert state.eligible_games_played == 0
    assert state.wins_toward_promotion == 0


def test_bundle_band_mismatch_marks_ineligible_without_counter_change(tmp_path):
    service = _service(tmp_path)
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="600+0",
        bundle_rating_band="1200-1400",
        required_turns=3,
        good_accepted=True,
    )
    assert eligibility.eligible is False
    assert "mismatch" in eligibility.reason
    service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="1200-1400")
    state = service.state.get_track_state("rapid", "600+0")
    assert state.eligible_games_played == 0


def test_promotion_demotion_thresholds_and_counter_reset(tmp_path):
    service = _service(tmp_path)
    state = service.state.get_track_state("rapid", "600+0")
    state.current_level = 4
    service.save()

    for _ in range(20):
        eligibility = service.evaluate_eligibility(
            routing_source="ordinary_corpus_play",
            bundle_available=True,
            time_control_id="600+0",
            bundle_rating_band="1000-1200",
            required_turns=5,
            good_accepted=True,
        )
        service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="1000-1200")

    promoted = service.state.get_track_state("rapid", "600+0")
    assert promoted.current_level == 5
    assert promoted.wins_toward_promotion == 0
    assert promoted.losses_toward_demotion == 0

    promoted.current_level = 5
    promoted.losses_toward_demotion = 9
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="600+0",
        bundle_rating_band="1000-1200",
        required_turns=6,
        good_accepted=True,
    )
    service.apply_eligible_result(eligibility, passed=False, bundle_time_control_id="600+0", bundle_rating_band="1000-1200")
    demoted = service.state.get_track_state("rapid", "600+0")
    assert demoted.current_level == 4
    assert demoted.wins_toward_promotion == 0
    assert demoted.losses_toward_demotion == 0


def test_unsupported_time_control_is_ineligible(tmp_path):
    service = _service(tmp_path)
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="180+0",
        bundle_rating_band="1000-1200",
        required_turns=5,
        good_accepted=True,
    )
    assert eligibility.eligible is False
    assert "Unsupported time control" in eligibility.reason


def test_level_table_contains_stockfish_tiers_and_promotion_clamps_at_28(tmp_path):
    assert len(SMART_PROFILE_LEVELS) == 30
    assert LEVEL_BY_INDEX[29].is_stockfish_tier is True
    assert LEVEL_BY_INDEX[30].is_stockfish_tier is True

    service = _service(tmp_path)
    state = service.state.get_track_state("bullet", "120+1")
    state.current_level = 28
    state.wins_toward_promotion = 49
    service.save()
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="120+1",
        bundle_rating_band="3000-3999",
        required_turns=14,
        good_accepted=False,
    )
    shift = service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="120+1", bundle_rating_band="3000-3999")
    state = service.state.get_track_state("bullet", "120+1")
    assert shift == "promotion_clamped"
    assert state.current_level == HIGHEST_CORPUS_BACKED_LEVEL


def test_runtime_contract_applies_to_session_from_active_level(tmp_path):
    bundle_dir = _bundle(tmp_path, time_control="300+0", minimum=1400, maximum=1600)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles"))
    state = session.smart_profile.state.get_track_state("blitz", "300+0")
    state.current_level = 9
    session.smart_profile.save()

    session._apply_settings(session.settings)

    assert session.required_player_moves == 3
    assert session.config.good_moves_acceptable is False


def test_smart_profile_state_file_created_for_profile(tmp_path):
    storage = ReviewStorage(tmp_path / "runtime" / "profiles")
    assert (tmp_path / "runtime" / "profiles" / "default" / "smart_profile_state.json").exists()
    payload = storage.load_smart_profile_state("default")
    assert payload["mode_enabled"] is True
