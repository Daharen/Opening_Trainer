from __future__ import annotations

from pathlib import Path

import chess
import pytest

from opening_trainer.evaluation import BookAuthorityResult, EngineAuthorityResult, ReasonCode
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.review.models import ReviewItem, ReviewPathMove
from opening_trainer.review.profile_service import ProfileService
from opening_trainer.review.router import ReviewRouter
from opening_trainer.review.scheduler import SRS_INTERVAL_DAYS, apply_failure, apply_success
from opening_trainer.review.scheduler import sync_due_cycle_transition
from opening_trainer.review.storage import ReviewStorage
from opening_trainer.settings import TrainerSettingsStore
from opening_trainer.session import TrainingSession
from opening_trainer.session_contracts import OutcomeModalContract


class StubBookAuthority:
    def __init__(self, result):
        self.result = result

    def evaluate(self, board_before_move, played_move):
        return self.result


class StubEngineAuthority:
    def __init__(self, result, best_reply=(None, None), best_continuation_result=None, ranked_moves_result=None):
        self.result = result
        self.best_reply_result = best_reply
        self.best_continuation_result = list(best_continuation_result or [])
        self.ranked_moves_result = list(ranked_moves_result or [])

    def evaluate(self, board_before_move, played_move):
        return self.result

    def best_reply(self, board):
        return self.best_reply_result

    def best_continuation(self, board, plies=5):
        return self.best_continuation_result[:plies]

    def ranked_candidate_moves(self, board, max_moves=6):
        return self.ranked_moves_result[:max_moves]


BOOK_MISS = BookAuthorityResult(False, False, ReasonCode.BOOK_UNAVAILABLE, 'Book authority unavailable for this position.', metadata={'book_available': False})


def _session(tmp_path: Path) -> TrainingSession:
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    session = TrainingSession(review_storage=storage)
    session.player_color = chess.WHITE
    session.state = session.state.PLAYER_TURN
    return session


def test_review_item_creation_on_failure(tmp_path):
    session = _session(tmp_path)
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(False, True, ReasonCode.ENGINE_FAIL, 'Rejected by engine.', best_move_uci='d2d4', best_move_san='d4', played_move_uci='e2e4', played_move_san='e4', cp_loss=170, metadata={'engine_available': True}), best_reply=('g8f6', 'Nf6')))
    session.submit_user_move_uci('e2e4')
    items = session.review_storage.load_items(session.active_profile_id)
    assert len(items) == 1
    assert 'Rejected' in items[0].failure_reason
    assert items[0].preferred_move_uci == 'd2d4'
    assert session.last_outcome.pre_fail_fen == chess.STARTING_FEN
    assert session.last_outcome.post_fail_fen == 'rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1'
    assert session.last_outcome.preferred_move_uci == 'd2d4'
    assert session.last_outcome.punishing_reply_uci == 'g8f6'


def test_profile_creation_and_deletion(tmp_path):
    service = ProfileService(ReviewStorage(tmp_path / 'runtime' / 'profiles'))
    created = service.create_profile('Experiment A')
    assert created.profile_id == 'experiment_a'
    service.delete_profile(created.profile_id)
    assert all(profile.profile_id != created.profile_id for profile in service.list_profiles())


def test_urgency_promotion_and_hysteresis_exit_behavior():
    item = ReviewItem.create('default', 'k', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    for _ in range(3):
        apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'ordinary_corpus_play')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier != 'extreme_urgency'


def test_due_scheduling_updates_on_fail_and_pass():
    item = ReviewItem.create('default', 'k', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    first_srs_due = item.srs_next_due_at_utc
    apply_success(item, 'ordinary_corpus_play')
    assert item.srs_next_due_at_utc == first_srs_due
    apply_success(item, 'srs_due_review')
    assert item.srs_stage_index == 1
    assert item.srs_next_due_at_utc > first_srs_due
    apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'srs_due_review')
    assert item.srs_stage_index == 0
    assert item.srs_lapse_count == 1


def test_reentry_path_capture_and_deterministic_replay_plan_reconstruction():
    item = ReviewItem.create('default', 'k', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen'), ReviewPathMove(1, 'black', 'e7e5', 'e5', 'fen2')])
    router = ReviewRouter()
    decision = router.immediate_retry('default', item)
    assert decision.review_plan_present is True
    assert decision.review_plan.predecessor_path[1]['move_uci'] == 'e7e5'


def test_review_items_rotate_within_tier_queue():
    router = ReviewRouter()
    items = [_due_item('a', 'ordinary_review'), _due_item('b', 'ordinary_review')]

    seen = [router.select('default', items).selected_review_item_id for _ in range(6)]

    assert set(filter(None, seen)) == {items[0].review_item_id, items[1].review_item_id}


def _due_item(position_key: str, urgency_tier: str) -> ReviewItem:
    item = ReviewItem.create('default', position_key, 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    item.urgency_tier = urgency_tier
    item.due_at_utc = '2000-01-01T00:00:00+00:00'
    item.updated_at_utc = '2026-01-01T00:00:00+00:00'
    item.last_seen_at_utc = '2025-01-01T00:00:00+00:00'
    item.srs_next_due_at_utc = '2099-01-01T00:00:00+00:00'
    return item


def test_due_only_ordinary_keeps_corpus_at_twenty_percent():
    router = ReviewRouter()
    decision = router.select('default', [_due_item('a', 'ordinary_review')])
    assert decision.corpus_share == pytest.approx(0.8)
    assert decision.review_share == pytest.approx(0.2)
    assert decision.routing_source in {'scheduled_review', 'ordinary_corpus_play'}


def test_boosted_and_extreme_reduce_corpus_share_with_exact_penalties():
    router = ReviewRouter()
    items = [_due_item('a', 'ordinary_review'), _due_item('b', 'boosted_review'), _due_item('c', 'boosted_review'), _due_item('d', 'extreme_urgency')]
    decision = router.select('default', items)
    assert decision.corpus_share == pytest.approx(0.65)
    assert decision.review_share == pytest.approx(0.35)
    assert decision.boosted_due_count == 2
    assert decision.extreme_due_count == 1


def test_review_selection_prioritizes_srs_due_before_pressure_deck():
    router = ReviewRouter()
    extreme = _due_item('e', 'extreme_urgency')
    boosted = _due_item('b', 'boosted_review')
    ordinary = _due_item('a', 'ordinary_review')
    ordinary.srs_next_due_at_utc = '2000-01-01T00:00:00+00:00'
    decision = router.select('default', [extreme] + [_due_item(str(i), 'extreme_urgency') for i in range(10)] + [boosted, ordinary])
    assert decision.routing_source == 'srs_due_review'


def test_boosted_review_routing_reason_is_explicit():
    router = ReviewRouter()
    boosted_1 = _due_item('b', 'boosted_review')
    boosted_2 = _due_item('c', 'boosted_review')
    boosted_1.srs_next_due_at_utc = '2099-01-01T00:00:00+00:00'
    boosted_2.srs_next_due_at_utc = '2099-01-01T00:00:00+00:00'
    decision = router.select('default', [boosted_1, boosted_2])
    for _ in range(20):
        if decision.routing_source == 'boosted_review':
            break
        decision = router.select('default', [boosted_1, boosted_2])
    assert decision.routing_source == 'boosted_review'
    assert decision.urgency_tier == 'boosted_review'


def test_outcome_modal_contract_requires_acknowledgement():
    contract = OutcomeModalContract('FAIL', 'summary', 'reason', 'e4', 'route', 'next', 'impact')
    assert contract.requires_acknowledgement is True
    assert contract.reason == 'reason'


def test_profile_reset_clears_review_state_without_touching_runtime_config(tmp_path):
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    service = ProfileService(storage)
    session = TrainingSession(review_storage=storage)
    storage.save_items(session.active_profile_id, [ReviewItem.create('default', 'a', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])])
    service.reset_profile(session.active_profile_id)
    assert storage.load_items(session.active_profile_id) == []
    assert session.runtime_context.config is not None


def test_session_reset_profile_resets_persisted_smart_state_for_non_active_profile(tmp_path):
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    session = TrainingSession(review_storage=storage)
    created = session.profile_service.create_profile('Experiment A')
    storage.save_smart_profile_state(
        created.profile_id,
        {
            'mode': 'smart_profile',
            'mode_enabled': True,
            'selected_track_id': 'blitz',
            'selected_time_control_id': '300+0',
            'tracks': {'blitz': {'300+0': {'current_level': 8, 'eligible_games_played': 5}}},
        },
    )

    was_active = session.reset_profile(created.profile_id)

    payload = storage.load_smart_profile_state(created.profile_id)
    assert was_active is False
    assert payload['selected_track_id'] == 'rapid'
    assert payload['tracks'] == {}


def test_session_reset_profile_resets_active_smart_runtime_state(tmp_path):
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    session = TrainingSession(review_storage=storage)
    session.smart_profile.state.selected_track_id = 'blitz'
    session.smart_profile.state.selected_time_control_id = '300+0'
    active = session.smart_profile.state.active_track_state()
    active.current_level = 9
    active.consecutive_eligible_successes = 3

    was_active = session.reset_profile(session.active_profile_id)

    assert was_active is True
    assert session.smart_profile.state.selected_track_id == 'blitz'
    assert session.smart_profile.state.active_track_state().current_level == 1


def test_integration_fail_then_retry_then_success_updates_mastery(tmp_path):
    session = _session(tmp_path)
    session.current_routing = session.router.select(session.active_profile_id, [])
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(False, True, ReasonCode.ENGINE_FAIL, 'Rejected by engine.', best_move_uci='d2d4', best_move_san='d4', played_move_uci='e2e4', played_move_san='e4', cp_loss=170, metadata={'engine_available': True})))
    session.submit_user_move_uci('e2e4')
    item = session.review_storage.load_items(session.active_profile_id)[0]
    assert session.last_outcome.next_routing_reason == 'immediate_retry'
    session.board.reset()
    session.state = session.state.PLAYER_TURN
    session.player_move_count = 0
    session.current_review_item_id = item.review_item_id
    session.current_routing = session.router.immediate_retry(session.active_profile_id, item)
    session.required_player_moves = 1
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(True, True, ReasonCode.ENGINE_PASS, 'Accepted by engine.', best_move_uci='e2e4', best_move_san='e4', played_move_uci='e2e4', played_move_san='e4', cp_loss=0, metadata={'engine_available': True})))
    session.submit_user_move_uci('e2e4')
    updated = session.review_storage.load_items(session.active_profile_id)[0]
    assert updated.times_passed == 1
    assert updated.mastery_score > 0.0


def test_failure_outcome_leaves_punishing_reply_null_when_engine_reply_unavailable(tmp_path):
    session = _session(tmp_path)
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(False, True, ReasonCode.ENGINE_FAIL, 'Rejected by engine.', best_move_uci='d2d4', best_move_san='d4', played_move_uci='e2e4', played_move_san='e4', cp_loss=170, metadata={'engine_available': True}), best_reply=(None, None)))

    session.submit_user_move_uci('e2e4')

    assert session.last_outcome.pre_fail_fen == chess.STARTING_FEN
    assert session.last_outcome.post_fail_fen is not None
    assert session.last_outcome.preferred_move_uci == 'd2d4'
    assert session.last_outcome.punishing_reply_uci is None


def test_success_outcome_remains_unaffected_by_fail_review_fields(tmp_path):
    session = _session(tmp_path)
    session.current_routing = session.router.select(session.active_profile_id, [])
    session.required_player_moves = 1
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(True, True, ReasonCode.ENGINE_PASS, 'Accepted by engine.', best_move_uci='e2e4', best_move_san='e4', played_move_uci='e2e4', played_move_san='e4', cp_loss=0, metadata={'engine_available': True})))

    session.submit_user_move_uci('e2e4')

    assert session.last_outcome.terminal_kind == 'pass'
    assert session.last_outcome.pre_fail_fen is None
    assert session.last_outcome.punishing_reply_uci is None


def test_settings_persist_good_toggle_and_training_depth(tmp_path):
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    session = TrainingSession(review_storage=storage)

    saved = session.update_settings(session.settings.__class__(good_moves_acceptable=False, active_training_ply_depth=3, side_panel_visible=False))

    reloaded = TrainerSettingsStore(storage.root).load(maximum_depth=session.max_supported_training_depth())
    assert saved.good_moves_acceptable is False
    assert saved.active_training_ply_depth == 3
    assert reloaded.good_moves_acceptable is False
    assert reloaded.active_training_ply_depth == 3


def test_rehabilitation_thresholds_are_exact_and_cumulative():
    item = ReviewItem.create('default', 'k', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    item.urgency_tier = 'extreme_urgency'
    item.consecutive_failures = 6
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier == 'boosted_review'
    for _ in range(2):
        apply_success(item, 'boosted_review')
    assert item.success_streak == 4
    assert item.urgency_tier == 'ordinary_review'
    for _ in range(2):
        apply_success(item, 'ordinary_corpus_play')
    assert item.success_streak == 6
    assert item.frequency_retired_for_current_due_cycle is True


def test_stubborn_extreme_arms_then_forces_one_repeat_then_clears_on_success():
    item = ReviewItem.create('default', 'k2', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    item.urgency_tier = 'extreme_urgency'
    item.consecutive_failures = 4
    apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'extreme_urgency_review')
    assert item.stubborn_extreme_state == 'armed_after_fifth_failure'
    apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'extreme_urgency_review')
    assert item.pending_forced_stubborn_repeat is True
    assert item.stubborn_extreme_state == 'cooldown_until_success'
    item.pending_forced_stubborn_repeat = False
    apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'extreme_urgency_review')
    assert item.pending_forced_stubborn_repeat is False
    apply_success(item, 'extreme_urgency_review')
    assert item.stubborn_extreme_state == 'none'


def test_frequency_retirement_reactivates_only_after_due_cycle_transition():
    item = ReviewItem.create('default', 'k3', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    item.urgency_tier = 'ordinary_review'
    item.consecutive_successes = 5
    apply_success(item, 'ordinary_corpus_play')
    assert item.frequency_retired_for_current_due_cycle is True
    item.due_at_utc = '2099-01-01T00:00:00+00:00'
    sync_due_cycle_transition(item)
    assert item.frequency_retired_for_current_due_cycle is True
    item.due_at_utc = '2000-01-01T00:00:00+00:00'
    sync_due_cycle_transition(item)
    assert item.frequency_retired_for_current_due_cycle is False
    assert item.success_streak == 0


def test_persistence_upgrade_defaults_new_frequency_and_srs_fields(tmp_path):
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    profile_id = storage.get_active_profile_id()
    path = tmp_path / 'runtime' / 'profiles' / profile_id / 'review_items.json'
    legacy = ReviewItem.create('default', 'legacy', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')]).to_dict()
    for key in ('frequency_state', 'frequency_state_entered_at_utc', 'srs_stage_index', 'srs_next_due_at_utc', 'srs_last_reviewed_at_utc', 'srs_last_result', 'srs_lapse_count'):
        legacy.pop(key, None)
    path.write_text(__import__('json').dumps([legacy]), encoding='utf-8')
    item = storage.load_items(profile_id)[0]
    assert item.frequency_state == item.urgency_tier
    assert item.srs_stage_index == 0
    assert item.srs_next_due_at_utc
    assert item.srs_next_due_at_utc != item.due_at_utc
    assert item.srs_last_result == 'none'


def test_frequency_threshold_contract_exact_transitions():
    item = ReviewItem.create('default', 'freq', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    assert item.urgency_tier == 'ordinary_review'
    apply_failure(item, 'fail', None, item.predecessor_path, item.line_preview_san, 'ordinary_corpus_play')
    assert item.urgency_tier == 'boosted_review'
    apply_failure(item, 'fail', None, item.predecessor_path, item.line_preview_san, 'ordinary_corpus_play')
    apply_failure(item, 'fail', None, item.predecessor_path, item.line_preview_san, 'ordinary_corpus_play')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier == 'boosted_review'
    apply_success(item, 'boosted_review')
    apply_success(item, 'boosted_review')
    assert item.urgency_tier == 'ordinary_review'
    apply_success(item, 'ordinary_corpus_play')
    apply_success(item, 'ordinary_corpus_play')
    assert item.frequency_retired_for_current_due_cycle is True


def test_srs_advances_only_on_explicit_spaced_review():
    item = ReviewItem.create('default', 'srs', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    start_stage = item.srs_stage_index
    apply_success(item, 'ordinary_corpus_play')
    apply_success(item, 'boosted_review')
    assert item.srs_stage_index == start_stage
    apply_success(item, 'srs_due_review')
    assert item.srs_stage_index == start_stage + 1


def test_srs_failure_reset_only_on_explicit_srs_due():
    item = ReviewItem.create('default', 'srs-reset', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    apply_success(item, 'srs_due_review')
    assert item.srs_stage_index == 1
    apply_failure(item, 'fail', None, item.predecessor_path, item.line_preview_san, 'boosted_review')
    assert item.srs_stage_index == 1
    apply_failure(item, 'fail', None, item.predecessor_path, item.line_preview_san, 'srs_due_review')
    assert item.srs_stage_index == 0
    assert item.srs_lapse_count == 1


def test_session_history_tags_outcome_channel_for_smart_profile_isolation(tmp_path):
    session = _session(tmp_path)
    session.current_routing = session.router.select(session.active_profile_id, [])
    session.required_player_moves = 1
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(True, True, ReasonCode.ENGINE_PASS, 'Accepted by engine.', best_move_uci='e2e4', best_move_san='e4', played_move_uci='e2e4', played_move_san='e4', cp_loss=0, metadata={'engine_available': True})))
    session.submit_user_move_uci('e2e4')
    lines = (tmp_path / 'runtime' / 'profiles' / session.active_profile_id / 'session_history.jsonl').read_text(encoding='utf-8').strip().splitlines()
    if lines:
        payload = __import__('json').loads(lines[-1])['payload']
        assert payload.get('outcome_channel') in {'ordinary_corpus_play', 'review_or_practice', 'spaced_repetition_review'}


def test_failure_outcome_builds_punishment_line_up_to_five_plies_and_recommended_alternatives(tmp_path):
    session = _session(tmp_path)
    continuation = [
        ('g8f6', 'Nf6', 'rnbqkb1r/pppppppp/5n2/8/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 1 2'),
        ('d2d4', 'd4', 'rnbqkb1r/pppppppp/5n2/8/3PP3/8/PPP2PPP/RNBQKBNR b KQkq - 0 2'),
        ('d7d5', 'd5', 'rnbqkb1r/ppp1pppp/5n2/3p4/3PP3/8/PPP2PPP/RNBQKBNR w KQkq - 0 3'),
        ('e4d5', 'exd5', 'rnbqkb1r/ppp1pppp/5n2/3P4/3P4/8/PPP2PPP/RNBQKBNR b KQkq - 0 3'),
        ('f6d5', 'Nxd5', 'rnbqkb1r/ppp1pppp/8/3n4/3P4/8/PPP2PPP/RNBQKBNR w KQkq - 0 4'),
        ('c2c4', 'c4', 'rnbqkb1r/ppp1pppp/8/3n4/2PP4/8/PP3PPP/RNBQKBNR b KQkq - 0 4'),
    ]
    ranked = [
        ('d2d4', 'd4', 0),
        ('c2c4', 'c4', 28),
        ('g1f3', 'Nf3', 42),
        ('b1c3', 'Nc3', 70),
    ]
    session.evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(False, True, ReasonCode.ENGINE_FAIL, 'Rejected by engine.', best_move_uci='d2d4', best_move_san='d4', played_move_uci='e2e4', played_move_san='e4', cp_loss=170, metadata={'engine_available': True}),
            best_continuation_result=continuation,
            ranked_moves_result=ranked,
        ),
    )

    session.submit_user_move_uci('e2e4')

    assert len(session.last_outcome.punishment_line) == 5
    assert session.last_outcome.punishment_line[0][0] == 'g8f6'
    assert session.last_outcome.excellent_moves == (('c2c4', 'c4'), ('g1f3', 'Nf3'))
    assert session.last_outcome.good_moves == (('b1c3', 'Nc3'),)


def test_failure_outcome_omits_good_alternatives_when_good_moves_disabled(tmp_path):
    session = _session(tmp_path)
    session.config = type(session.config)(**{**session.config.snapshot(), 'good_moves_acceptable': False})
    ranked = [('d2d4', 'd4', 0), ('g1f3', 'Nf3', 20), ('b1c3', 'Nc3', 70)]
    session.evaluator = MoveEvaluator(
        config=session.config,
        book_authority=StubBookAuthority(BOOK_MISS),
        engine_authority=StubEngineAuthority(
            EngineAuthorityResult(False, True, ReasonCode.ENGINE_FAIL, 'Rejected by engine.', best_move_uci='d2d4', best_move_san='d4', played_move_uci='e2e4', played_move_san='e4', cp_loss=170, metadata={'engine_available': True}),
            ranked_moves_result=ranked,
        ),
    )

    session.submit_user_move_uci('e2e4')

    assert session.last_outcome.excellent_moves == (('g1f3', 'Nf3'),)
    assert session.last_outcome.good_moves == ()
