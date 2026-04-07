from __future__ import annotations

from types import SimpleNamespace

from opening_trainer.session import TrainingSession
from opening_trainer.ui.review_deck_inspector_window import ReviewDeckInspectorWindow


def test_snapshot_uses_urgent_multiplicity_fallback() -> None:
    session = TrainingSession.__new__(TrainingSession)
    urgent = SimpleNamespace(
        review_item_id='u1',
        position_key='urgent-pos',
        frequency_state='extreme_urgency',
        urgency_tier='extreme_urgency',
        consecutive_failures=3,
        success_streak=0,
    )
    due = SimpleNamespace(
        review_item_id='d1',
        position_key='due-pos',
        frequency_state='ordinary_review',
        urgency_tier='ordinary_review',
        consecutive_failures=1,
        success_streak=2,
    )
    session._items = lambda: [urgent, due]
    session.active_profile_id = 'default'
    session.router = SimpleNamespace(
        export_profile_state=lambda _profile_id: {
            'D': {'active_deck': ['d1'], 'waiting_queue': [], 'round_seen_count': 2, 'round_miss_count': 0, 'capacity': 5},
            'B': {'active_deck': [], 'waiting_queue': [], 'round_seen_count': 0, 'round_miss_count': 0, 'capacity': 3},
            'E': {'active_deck': ['u1'], 'waiting_queue': ['u2'], 'round_seen_count': 1, 'round_miss_count': 1, 'capacity': 2},
            'stable_review_deck': {
                'cards': [
                    {'layer': 0, 'review_item_id': 'd1'},
                    {'layer': 0, 'review_item_id': 'u1'},
                    {'layer': 1, 'review_item_id': 'u1'},
                    {'layer': 2, 'review_item_id': 'u1'},
                    {'layer': 3, 'review_item_id': 'u1'},
                ],
                'cursor': 3,
            },
        },
        deck=SimpleNamespace(index=7),
    )
    session._inspector_last_mutation_reason = 'deck_exhausted'
    session._inspector_last_routing_action = 'extreme_urgency_review'

    snapshot = TrainingSession.review_deck_inspector_snapshot(session)

    rows = {row['review_item_id']: row for row in snapshot['active_rows']}
    assert rows['u1']['deck_cards'] == 4
    assert rows['d1']['deck_cards'] == 1
    assert snapshot['card_count_source'] == 'live_deck_cards'


def test_palette_assignment_is_stable_for_existing_rows() -> None:
    window = ReviewDeckInspectorWindow.__new__(ReviewDeckInspectorWindow)
    window._row_palette = {'existing': ('red', '#d32f2f', 'white')}

    ReviewDeckInspectorWindow._reconcile_palette(window, ['existing', 'new'])

    assert window._row_palette['existing'][0] == 'red'
    assert window._row_palette['new'][0] != 'red'
