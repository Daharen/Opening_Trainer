from __future__ import annotations

from pathlib import Path

import chess
import pytest

from opening_trainer.evaluation import BookAuthorityResult, EngineAuthorityResult, ReasonCode
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.review.models import ReviewItem, ReviewPathMove
from opening_trainer.review.profile_service import ProfileService
from opening_trainer.review.router import ReviewRouter, RoutingConfig
from opening_trainer.review.scheduler import apply_failure, apply_success
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
    def __init__(self, result, best_reply=(None, None)):
        self.result = result
        self.best_reply_result = best_reply

    def evaluate(self, board_before_move, played_move):
        return self.result

    def best_reply(self, board):
        return self.best_reply_result


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
    for _ in range(4):
        apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'ordinary_corpus_play')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier == 'extreme_urgency'
    apply_success(item, 'extreme_urgency_review')
    apply_success(item, 'extreme_urgency_review')
    assert item.urgency_tier != 'extreme_urgency'


def test_due_scheduling_updates_on_fail_and_pass():
    item = ReviewItem.create('default', 'k', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    first_due = item.due_at_utc
    apply_success(item, 'scheduled_review')
    assert item.due_at_utc > first_due
    apply_failure(item, 'fail', 'e2e4', item.predecessor_path, item.line_preview_san, 'scheduled_review')
    assert item.due_at_utc <= item.updated_at_utc


def test_reentry_path_capture_and_deterministic_replay_plan_reconstruction():
    item = ReviewItem.create('default', 'k', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen'), ReviewPathMove(1, 'black', 'e7e5', 'e5', 'fen2')])
    router = ReviewRouter()
    decision = router.immediate_retry('default', item)
    assert decision.review_plan_present is True
    assert decision.review_plan.predecessor_path[1]['move_uci'] == 'e7e5'


def test_routing_cap_prevents_one_item_from_dominating():
    router = ReviewRouter()
    items = [
        ReviewItem.create('default', 'a', 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')]),
        ReviewItem.create('default', 'b', 'fen', 'white', 'fail', 'd2d4', [], [ReviewPathMove(0, 'white', 'd2d4', 'd4', 'fen')]),
    ]
    first = router.immediate_retry('default', items[0])
    for _ in range(8):
        router.recent_item_ids.append(items[0].review_item_id)
    decision = router.select('default', items)
    assert decision.selected_review_item_id == items[1].review_item_id or decision.routing_source == 'ordinary_corpus_play'


def _due_item(position_key: str, urgency_tier: str) -> ReviewItem:
    item = ReviewItem.create('default', position_key, 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    item.urgency_tier = urgency_tier
    item.due_at_utc = '2000-01-01T00:00:00+00:00'
    item.updated_at_utc = '2026-01-01T00:00:00+00:00'
    item.last_seen_at_utc = '2025-01-01T00:00:00+00:00'
    return item


def test_due_only_ordinary_keeps_corpus_at_twenty_percent():
    router = ReviewRouter()
    decision = router.select('default', [_due_item('a', 'ordinary_review')])
    assert decision.corpus_share == 0.2
    assert decision.review_share == 0.8
    assert decision.boosted_due_count == 0
    assert decision.extreme_due_count == 0


def test_boosted_and_extreme_reduce_corpus_share_with_exact_penalties():
    router = ReviewRouter()
    items = [_due_item('a', 'ordinary_review'), _due_item('b', 'boosted_review'), _due_item('c', 'boosted_review'), _due_item('d', 'extreme_urgency')]
    decision = router.select('default', items)
    assert decision.corpus_share == pytest.approx(0.16)
    assert decision.review_share == pytest.approx(0.84)
    assert decision.boosted_due_count == 2
    assert decision.extreme_due_count == 1


def test_review_selection_prioritizes_extreme_then_boosted_then_ordinary():
    router = ReviewRouter()
    extreme = _due_item('e', 'extreme_urgency')
    boosted = _due_item('b', 'boosted_review')
    ordinary = _due_item('a', 'ordinary_review')
    # Force review path deterministically by creating a 0 corpus share case.
    decision = router.select('default', [extreme] + [_due_item(str(i), 'extreme_urgency') for i in range(10)] + [boosted, ordinary])
    assert decision.routing_source == 'extreme_urgency_override'
    assert decision.urgency_tier == 'extreme_urgency'


def test_boosted_review_routing_reason_is_explicit():
    router = ReviewRouter(RoutingConfig(due_baseline_corpus_share=0.0))
    decision = router.select('default', [_due_item('b', 'boosted_review')])
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
