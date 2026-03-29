from __future__ import annotations

import json
from pathlib import Path

from opening_trainer.review.storage import ReviewStorage
from opening_trainer.runtime import RuntimeOverrides, load_runtime_config
from opening_trainer.settings import TrainerSettings
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
    assert resolve_track_category("180+0") == ("blitz", "180+0")


def test_track_state_is_independent_and_persistent(tmp_path):
    service = _service(tmp_path)
    rapid = service.state.get_track_state("rapid", "600+0")
    blitz = service.state.get_track_state("blitz", "300+0")
    rapid.current_level = 6
    rapid.consecutive_eligible_successes = 7
    service.save()

    reloaded = _service(tmp_path)
    reloaded_rapid = reloaded.state.get_track_state("rapid", "600+0")
    reloaded_blitz = reloaded.state.get_track_state("blitz", "300+0")
    assert reloaded_rapid.current_level == 6
    assert reloaded_rapid.consecutive_eligible_successes == 7
    assert reloaded_blitz.current_level == blitz.current_level
    assert reloaded_blitz.consecutive_eligible_successes == 0


def test_only_ordinary_corpus_counts_and_review_channels_do_not_count(tmp_path):
    service = _service(tmp_path)
    _bundle(tmp_path, time_control="600+0", minimum=400, maximum=600)
    channels = ["scheduled_review", "boosted_review", "extreme_urgency_review", "immediate_retry", "srs_due_review", "stubborn_extreme_repeat"]
    for channel in channels:
        eligibility = service.evaluate_eligibility(
            routing_source=channel,
            bundle_available=True,
            time_control_id="600+0",
            bundle_rating_band="400-600",
            required_turns=3,
            good_accepted=True,
            catalog_root=str(tmp_path),
        )
        assert eligibility.eligible is False
        service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="400-600")
    state = service.state.get_track_state("rapid", "600+0")
    assert state.eligible_games_played == 0
    assert state.consecutive_eligible_successes == 0


def test_bundle_band_mismatch_marks_ineligible_without_counter_change(tmp_path):
    service = _service(tmp_path)
    _bundle(tmp_path, time_control="600+0", minimum=400, maximum=600)
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="600+0",
        bundle_rating_band="1200-1400",
        required_turns=3,
        good_accepted=True,
        catalog_root=str(tmp_path),
    )
    assert eligibility.eligible is False
    assert "mismatch" in eligibility.reason
    service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="1200-1400")
    state = service.state.get_track_state("rapid", "600+0")
    assert state.eligible_games_played == 0


def test_promotion_demotion_thresholds_and_counter_reset(tmp_path):
    service = _service(tmp_path)
    _bundle(tmp_path, time_control="600+0", minimum=1000, maximum=1200)
    state = service.state.get_track_state("rapid", "600+0")
    state.current_level = 4
    service.save()

    for _ in range(10):
        eligibility = service.evaluate_eligibility(
            routing_source="ordinary_corpus_play",
            bundle_available=True,
            time_control_id="600+0",
            bundle_rating_band="1000-1200",
            required_turns=5,
            good_accepted=True,
            catalog_root=str(tmp_path),
        )
        service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="1000-1200")

    promoted = service.state.get_track_state("rapid", "600+0")
    assert promoted.current_level == 5
    assert promoted.consecutive_eligible_successes == 0
    assert promoted.consecutive_eligible_failures == 0

    promoted.current_level = 5
    promoted.consecutive_eligible_failures = 4
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="600+0",
        bundle_rating_band="1000-1200",
        required_turns=6,
        good_accepted=True,
        catalog_root=str(tmp_path),
    )
    service.apply_eligible_result(eligibility, passed=False, bundle_time_control_id="600+0", bundle_rating_band="1000-1200")
    demoted = service.state.get_track_state("rapid", "600+0")
    assert demoted.current_level == 4
    assert demoted.consecutive_eligible_successes == 0
    assert demoted.consecutive_eligible_failures == 0


def test_unsupported_time_control_is_ineligible(tmp_path):
    service = _service(tmp_path)
    _bundle(tmp_path, time_control="600+0", minimum=400, maximum=600)
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="180+0",
        bundle_rating_band="400-600",
        required_turns=3,
        good_accepted=True,
        catalog_root=str(tmp_path),
    )
    assert eligibility.eligible is False
    assert "time control mismatch" in eligibility.reason




def test_level_table_threshold_remap_and_unaffected_values():
    assert LEVEL_BY_INDEX[4].game_successes_to_promote == 10
    assert LEVEL_BY_INDEX[5].game_successes_to_promote == 20
    assert LEVEL_BY_INDEX[1].game_failures_to_demote == 5
    assert LEVEL_BY_INDEX[30].game_successes_to_promote == float("inf")

def test_level_table_contains_stockfish_tiers_and_promotion_clamps_at_28(tmp_path):
    assert len(SMART_PROFILE_LEVELS) == 30
    assert LEVEL_BY_INDEX[29].is_stockfish_tier is True
    assert LEVEL_BY_INDEX[30].is_stockfish_tier is True

    service = _service(tmp_path)
    _bundle(tmp_path, time_control="120+1", minimum=3000, maximum=3999)
    service.set_selected_track("bullet")
    state = service.state.get_track_state("bullet", "120+1")
    state.current_level = 28
    state.consecutive_eligible_successes = 19
    service.save()
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="120+1",
        bundle_rating_band="3000-3999",
        required_turns=14,
        good_accepted=False,
        catalog_root=str(tmp_path),
    )
    shift = service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="120+1", bundle_rating_band="3000-3999")
    state = service.state.get_track_state("bullet", "120+1")
    assert shift == "promotion_clamped"
    assert state.current_level == HIGHEST_CORPUS_BACKED_LEVEL


def test_runtime_contract_applies_to_session_from_active_level(tmp_path):
    bundle_dir = _bundle(tmp_path, time_control="300+0", minimum=1400, maximum=1600)
    runtime = load_runtime_config(RuntimeOverrides(corpus_bundle_dir=str(bundle_dir)))
    session = TrainingSession(runtime_context=runtime, review_storage=ReviewStorage(tmp_path / "profiles"))
    session.update_settings(session.settings.__class__(training_mode="smart_profile", selected_smart_track="blitz", selected_time_control_id="300+0"))
    session.smart_profile.set_selected_track("blitz")
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
    assert payload["mode"] == "smart_profile"
    assert payload["selected_track_id"] == "rapid"


def test_manual_mode_never_counts_even_with_ladder_matching_contract(tmp_path):
    service = _service(tmp_path)
    _bundle(tmp_path, time_control="600+0", minimum=400, maximum=600)
    service.set_mode("manual")
    eligibility = service.evaluate_eligibility(
        routing_source="ordinary_corpus_play",
        bundle_available=True,
        time_control_id="600+0",
        bundle_rating_band="400-600",
        required_turns=3,
        good_accepted=True,
        catalog_root=str(tmp_path),
    )
    assert eligibility.eligible is False
    assert "Manual mode" in eligibility.reason
    service.apply_eligible_result(eligibility, passed=True, bundle_time_control_id="600+0", bundle_rating_band="400-600")
    state = service.state.get_track_state("rapid", "600+0")
    assert state.eligible_games_played == 0


def test_selected_time_control_drives_expected_bundle_resolution(tmp_path):
    service = _service(tmp_path)
    first = _bundle(tmp_path, time_control="600+0", minimum=400, maximum=600)
    second = _bundle(tmp_path, time_control="600+1", minimum=400, maximum=600)
    assert first != second

    assert service.set_selected_time_control("600+1") is True
    resolution = service.resolve_expected_bundle(str(tmp_path))

    assert resolution.category_id == "600+1"
    assert resolution.resolved_entry is not None
    assert str(resolution.resolved_entry.bundle_dir).endswith("bundle_600_1")


def test_reset_all_preserves_selected_exact_control_and_resets_active_track_state(tmp_path):
    service = _service(tmp_path)
    assert service.set_selected_time_control("600+1") is True
    active = service.state.active_track_state()
    active.current_level = 7
    active.consecutive_eligible_successes = 3
    active.consecutive_eligible_failures = 2
    service.save()

    service.reset_all()

    assert service.state.selected_track_id == "rapid"
    assert service.state.selected_time_control_id == "600+1"
    reset_active = service.state.active_track_state()
    assert reset_active.time_control_category_id == "600+1"
    assert reset_active.current_level == 1
    assert reset_active.consecutive_eligible_successes == 0
    assert reset_active.consecutive_eligible_failures == 0


def test_apply_settings_propagates_selected_exact_time_control_before_contract_enforcement():
    session = TrainingSession.__new__(TrainingSession)
    session.max_supported_training_depth = lambda: 10
    session.config = type(
        "Config",
        (),
        {
            "__init__": lambda self, **kwargs: self.__dict__.update(kwargs),
            "snapshot": lambda self: {"active_envelope_player_moves": 5, "good_moves_acceptable": True},
        },
    )(active_envelope_player_moves=5, good_moves_acceptable=True)
    session.evaluator = type(
        "Evaluator",
        (),
        {
            "config": None,
            "overlay_classifier": type("Overlay", (), {"config": None})(),
            "engine_authority": type("Authority", (), {"config": None})(),
        },
    )()
    calls: list[str] = []

    class SmartProfileSpy:
        def __init__(self):
            self.selected_time_control = None

        def set_mode(self, mode):
            calls.append(f"mode:{mode}")

        def set_selected_track(self, track):
            calls.append(f"track:{track}")

        def set_selected_time_control(self, time_control):
            calls.append(f"time:{time_control}")
            self.selected_time_control = time_control
            return True

        def enforce_runtime_contract(self, *, fallback_turns, fallback_good_accepted):
            calls.append(f"enforce:{self.selected_time_control}")
            assert self.selected_time_control == "300+0"
            return fallback_turns, fallback_good_accepted

    session.smart_profile = SmartProfileSpy()
    settings = TrainerSettings(training_mode="smart_profile", selected_smart_track="blitz", selected_time_control_id="300+0")

    TrainingSession._apply_settings(session, settings)

    assert calls[:4] == ["mode:smart_profile", "track:blitz", "time:300+0", "enforce:300+0"]


def test_record_outcome_promotion_reapplies_runtime_contract_and_exposes_pending_level_change():
    session = TrainingSession.__new__(TrainingSession)
    session.settings = TrainerSettings(training_mode="smart_profile")
    session.runtime_context = type("Runtime", (), {"config": type("Config", (), {"corpus_bundle_dir": "/tmp/bundle"})()})()
    session.required_player_moves = 3
    session.config = type("Config", (), {"good_moves_acceptable": True})()
    session.current_routing = type("Route", (), {"routing_source": "ordinary_corpus_play"})()
    session._pending_smart_level_change = None
    reapplied: list[str] = []
    session._apply_settings = lambda settings: reapplied.append(settings.training_mode)
    session._timing_contract_metadata = lambda: ("600+0", "400-600")

    class SmartProfileSpy:
        def __init__(self):
            self.level = 1

        def current_track_state(self):
            return type("Track", (), {"current_level": self.level})(), object()

        def evaluate_eligibility(self, **_kwargs):
            return type("Eligibility", (), {"eligible": True, "reason": "Eligible ordinary corpus ladder game."})()

        def apply_eligible_result(self, _eligibility, **_kwargs):
            self.level = 2
            return "promotion"

    session.smart_profile = SmartProfileSpy()

    TrainingSession._record_smart_profile_outcome(session, True)

    assert reapplied == ["smart_profile"]
    assert session.consume_pending_smart_level_change() == (1, 2)
    assert session.consume_pending_smart_level_change() is None


def test_record_outcome_streak_only_still_reapplies_runtime_contract_without_pending_level_change():
    session = TrainingSession.__new__(TrainingSession)
    session.settings = TrainerSettings(training_mode="smart_profile")
    session.runtime_context = type("Runtime", (), {"config": type("Config", (), {"corpus_bundle_dir": "/tmp/bundle"})()})()
    session.required_player_moves = 3
    session.config = type("Config", (), {"good_moves_acceptable": True})()
    session.current_routing = type("Route", (), {"routing_source": "ordinary_corpus_play"})()
    session._pending_smart_level_change = None
    reapplied: list[str] = []
    session._apply_settings = lambda settings: reapplied.append(settings.training_mode)
    session._timing_contract_metadata = lambda: ("600+0", "400-600")

    class SmartProfileSpy:
        def __init__(self):
            self.level = 4

        def current_track_state(self):
            return type("Track", (), {"current_level": self.level})(), object()

        def evaluate_eligibility(self, **_kwargs):
            return type("Eligibility", (), {"eligible": True, "reason": "Eligible ordinary corpus ladder game."})()

        def apply_eligible_result(self, _eligibility, **_kwargs):
            return "none"

    session.smart_profile = SmartProfileSpy()

    TrainingSession._record_smart_profile_outcome(session, True)

    assert reapplied == ["smart_profile"]
    assert session.consume_pending_smart_level_change() is None
