from __future__ import annotations

import pytest

from opening_trainer.review.models import ReviewItem, ReviewPathMove
from opening_trainer.review.router import ReviewRouter


def _item(position_key: str, tier: str, due_at: str = '2000-01-01T00:00:00+00:00', last_seen: str = '2025-01-01T00:00:00+00:00') -> ReviewItem:
    item = ReviewItem.create('default', position_key, 'fen', 'white', 'fail', 'e2e4', [], [ReviewPathMove(0, 'white', 'e2e4', 'e4', 'fen')])
    item.urgency_tier = tier
    item.due_at_utc = due_at
    item.last_seen_at_utc = last_seen
    return item


def _shares(router: ReviewRouter, d: int, b: int, e: int) -> tuple[float, float]:
    out = router._compute_shares(d, 0, 0, 0, 0, b, e)
    return out['corpus'], out['review']


def test_outer_share_ladder_and_reserve_bands():
    router = ReviewRouter()
    assert _shares(router, 0, 0, 0) == pytest.approx((1.0, 0.0))
    assert _shares(router, 1, 0, 0) == pytest.approx((0.8, 0.2))
    assert _shares(router, 2, 0, 0) == pytest.approx((0.65, 0.35))
    assert _shares(router, 3, 0, 0) == pytest.approx((0.55, 0.45))
    assert _shares(router, 4, 0, 0) == pytest.approx((0.5, 0.5))
    assert _shares(router, 30, 0, 0)[0] == pytest.approx(0.2)
    assert _shares(router, 30, 3, 0)[0] == pytest.approx(0.14)
    assert _shares(router, 30, 3, 4)[0] == pytest.approx(0.10)


def test_tier_weighting_changes_review_distribution():
    router = ReviewRouter()
    shares = router._compute_shares(1, 0, 0, 0, 0, 1, 1)
    assert shares['E'] > shares['B'] > shares['D']


def test_deck_size_rules():
    router = ReviewRouter()
    assert router._deck_size(0) == 20
    assert router._deck_size(4) == 20
    assert router._deck_size(5) == 40
    assert router._deck_size(9) == 40
    assert router._deck_size(10) == 80


def test_token_allocation_remainder_tie_break_prefers_e_b_d_c():
    router = ReviewRouter()
    shares = {'C': 0.25, 'D': 0.25, 'H80': 0.0, 'H60': 0.0, 'H40': 0.0, 'H20': 0.0, 'B': 0.25, 'E': 0.25}
    counts = router._allocate_counts(shares, 7, {'D': 1, 'H80': 0, 'H60': 0, 'H40': 0, 'H20': 0, 'B': 1, 'E': 1})
    # Base floors are 1 each and three remainder slots go to E, B, D.
    assert counts['E'] == 2 and counts['B'] == 2 and counts['D'] == 2


def test_queue_ordering_uses_due_then_last_seen_then_id_and_rotates():
    router = ReviewRouter()
    a = _item('a', 'ordinary_review', due_at='2000-01-01T00:00:00+00:00', last_seen='2024-01-01T00:00:00+00:00')
    b = _item('b', 'ordinary_review', due_at='2000-01-01T00:00:00+00:00', last_seen='2024-01-01T00:00:00+00:00')
    expected_front = min(a.review_item_id, b.review_item_id)
    router.select('default', [a, b])
    assert router.tier_queues['D'][0] == expected_front


def test_new_due_items_join_back_of_existing_queue():
    router = ReviewRouter()
    a = _item('a', 'ordinary_review')
    b = _item('b', 'ordinary_review')
    router.select('default', [a])
    router.select('default', [a, b])
    assert b.review_item_id in router.tier_queues['D']
    assert list(router.tier_queues['D']).index(b.review_item_id) >= 1


def test_rebuild_trigger_thresholds_and_counter_reset():
    router = ReviewRouter()
    a = _item('a', 'ordinary_review')
    router.select('default', [a])  # initial rebuild and reset
    b = _item('b', 'ordinary_review')
    c = _item('c', 'ordinary_review')
    d = _item('d', 'ordinary_review')
    router.select('default', [a, b])
    router.select('default', [a, b, c])
    third = router.select('default', [a, b, c, d])
    assert third.rebuild_trigger == 'due_additions_threshold'
    assert router.addition_counters == {'D': 0, 'B': 0}

    router_b = ReviewRouter()
    anchor = _item('anchor', 'ordinary_review')
    router_b.select('default', [anchor])
    p = _item('p', 'boosted_review')
    q = _item('q', 'boosted_review')
    router_b.select('default', [anchor, p])
    boosted = router_b.select('default', [anchor, p, q])
    assert boosted.rebuild_trigger == 'boosted_additions_threshold'

    router_e = ReviewRouter()
    router_e.select('default', [anchor])
    x = _item('x', 'extreme_urgency')
    extreme = router_e.select('default', [anchor, x])
    assert extreme.rebuild_trigger == 'extreme_state_change'


def test_removal_triggers_mirror_additions():
    router = ReviewRouter()
    items = [_item('a', 'ordinary_review'), _item('b', 'ordinary_review'), _item('c', 'ordinary_review'), _item('d', 'ordinary_review')]
    router.select('default', items)
    router.select('default', items[1:])
    router.select('default', items[2:])
    removal = router.select('default', items[3:])
    assert removal.rebuild_trigger == 'due_removals_threshold'


def test_boundary_rotation_and_same_item_safeguard():
    router = ReviewRouter()
    a = _item('a', 'ordinary_review')
    b = _item('b', 'ordinary_review')
    decisions = [router.select('default', [a, b]) for _ in range(30)]
    first_review = next(dec for dec in decisions if dec.routing_source == 'scheduled_review')
    router.deck.index = len(router.deck.tokens)
    second = router.select('default', [a, b])
    if second.routing_source == 'scheduled_review':
        assert first_review.selected_review_item_id != second.selected_review_item_id


def test_integration_interleaves_corpus_and_review_with_finite_deck():
    router = ReviewRouter()
    items = [_item('a', 'ordinary_review'), _item('b', 'boosted_review'), _item('c', 'extreme_urgency')]
    decisions = [router.select('default', items) for _ in range(30)]
    sources = {decision.routing_source for decision in decisions}
    assert 'ordinary_corpus_play' in sources
    assert 'scheduled_review' in sources or 'boosted_review' in sources or 'extreme_urgency_review' in sources
    assert all(decision.deck_size in (20, 40, 80) for decision in decisions)


def test_minimum_tier_representation_forces_non_empty_tiers_when_possible():
    router = ReviewRouter()
    counts = {'C': 10, 'D': 0, 'H80': 0, 'H60': 0, 'H40': 0, 'H20': 0, 'B': 1, 'E': 9}
    adjusted = router._enforce_minimum_tier_representation(counts, {'D': 1, 'H80': 0, 'H60': 0, 'H40': 0, 'H20': 0, 'B': 1, 'E': 1})
    assert adjusted['D'] == 1
    assert adjusted['E'] == 8
    assert adjusted['B'] == 1


def test_minimum_tier_representation_donor_tie_break_is_e_then_b_then_d():
    router = ReviewRouter()
    counts = {'C': 5, 'D': 3, 'B': 3, 'E': 3, 'H80': 0, 'H60': 0, 'H40': 0, 'H20': 0}
    assert router._choose_min_representation_donor(counts) == 'E'


def test_starvation_bump_selects_due_item_with_highest_skipped_slots():
    router = ReviewRouter()
    d1 = _item('d1', 'ordinary_review')
    d2 = _item('d2', 'ordinary_review')
    d1.skipped_review_slots = 8
    d2.skipped_review_slots = 9
    router.deck.tokens = ['D']
    router.deck.index = 0
    decision = router.select('default', [d1, d2])
    assert decision.selected_review_item_id == d2.review_item_id
    assert decision.rebuild_trigger == 'ORDINARY_DUE_STARVATION_BUMP'


def test_skipped_review_slots_increment_on_review_token_only():
    router = ReviewRouter()
    d1 = _item('d1', 'ordinary_review')
    d2 = _item('d2', 'ordinary_review')
    router.deck.tokens = ['C']
    router.deck.index = 0
    router.select('default', [d1, d2])
    assert d1.skipped_review_slots == 0 and d2.skipped_review_slots == 0
    router.deck.tokens = ['B']
    router.deck.index = 0
    boosted = _item('b', 'boosted_review')
    router.select('default', [d1, d2, boosted])
    assert {d1.skipped_review_slots, d2.skipped_review_slots} == {1}


def test_hijack_ticker_pass_schedules_match_spec():
    router = ReviewRouter()
    item = _item('h', 'ordinary_review')
    item.hijack_stage = 'h80'
    item.hijack_pass_ticker = 0
    assert [router._ticker_is_pass('h80', router._advance_hijack_ticker(item)) for _ in range(5)] == [False, False, False, False, True]

    item.hijack_stage = 'h60'
    item.hijack_pass_ticker = 0
    assert [router._ticker_is_pass('h60', router._advance_hijack_ticker(item)) for _ in range(5)] == [True, False, False, False, True]

    item.hijack_stage = 'h40'
    item.hijack_pass_ticker = 0
    assert [router._ticker_is_pass('h40', router._advance_hijack_ticker(item)) for _ in range(5)] == [True, False, True, False, True]

    item.hijack_stage = 'h20'
    item.hijack_pass_ticker = 0
    assert [router._ticker_is_pass('h20', router._advance_hijack_ticker(item)) for _ in range(5)] == [True, False, True, True, True]


def test_hijack_decay_progression_and_dormancy():
    router = ReviewRouter()
    item = _item('decay', 'ordinary_review')
    item.hijack_stage = 'h80'
    assert router.resolve_hijack_miss_decay(item) == 'h60'
    assert router.resolve_hijack_miss_decay(item) == 'h40'
    assert router.resolve_hijack_miss_decay(item) == 'h20'
    assert router.resolve_hijack_miss_decay(item) == 'dormant'
    assert item.dormant is True


def test_dormant_items_are_excluded_and_can_revive():
    router = ReviewRouter()
    item = _item('dormant', 'ordinary_review')
    item.hijack_stage = 'dormant'
    item.dormant = True
    decision = router.select('default', [item])
    assert decision.token_counts.get('H80', 0) == 0
    router.revive_dormant(item)
    assert item.hijack_stage == 'h80'
    assert item.dormant is False
