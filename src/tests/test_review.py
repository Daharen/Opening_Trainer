from __future__ import annotations

from pathlib import Path

import chess

from opening_trainer.evaluation import BookAuthorityResult, EngineAuthorityResult, ReasonCode
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.review.models import ReviewItem, ReviewPathMove
from opening_trainer.review.profile_service import ProfileService
from opening_trainer.review.router import ReviewRouter
from opening_trainer.review.scheduler import apply_failure, apply_success
from opening_trainer.review.storage import ReviewStorage
from opening_trainer.session import TrainingSession
from opening_trainer.session_contracts import OutcomeModalContract


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


def _session(tmp_path: Path) -> TrainingSession:
    storage = ReviewStorage(tmp_path / 'runtime' / 'profiles')
    session = TrainingSession(review_storage=storage)
    session.player_color = chess.WHITE
    session.state = session.state.PLAYER_TURN
    return session


def test_review_item_creation_on_failure(tmp_path):
    session = _session(tmp_path)
    session.evaluator = MoveEvaluator(book_authority=StubBookAuthority(BOOK_MISS), engine_authority=StubEngineAuthority(EngineAuthorityResult(False, True, ReasonCode.ENGINE_FAIL, 'Rejected by engine.', best_move_uci='d2d4', best_move_san='d4', played_move_uci='e2e4', played_move_san='e4', cp_loss=170, metadata={'engine_available': True})))
    session.submit_user_move_uci('e2e4')
    items = session.review_storage.load_items(session.active_profile_id)
    assert len(items) == 1
    assert 'Rejected' in items[0].failure_reason
    assert items[0].preferred_move_uci == 'd2d4'


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
