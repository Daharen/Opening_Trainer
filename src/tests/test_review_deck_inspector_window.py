from __future__ import annotations

from types import SimpleNamespace

from opening_trainer.session import TrainingSession
from opening_trainer.ui.review_deck_inspector_window import ReviewDeckInspectorWindow, _InspectorLiveState


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
        last_shares=(0.55, 0.45),
        last_share_breakdown={
            'due_active': 1,
            'boosted_active': 0,
            'urgent_active': 1,
            'due_equivalent': 2,
            'boosted_equivalent': 1,
            'urgent_equivalent': 1,
            'due_pct': 35,
            'boosted_pct': 6,
            'urgent_pct': 4,
            'training_pct': 45,
            'corpus_pct': 55,
        },
    )
    session._inspector_last_mutation_reason = 'deck_exhausted'
    session._inspector_last_routing_action = 'extreme_urgency_review'

    snapshot = TrainingSession.review_deck_inspector_snapshot(session)

    rows = {row['review_item_id']: row for row in snapshot['active_rows']}
    assert rows['u1']['deck_cards'] == 4
    assert rows['d1']['deck_cards'] == 1
    assert snapshot['card_count_source'] == 'live_deck_cards'
    assert snapshot['tiers']['boosted']['capacity'] == 3
    assert snapshot['stable_review_deck']['cursor'] == 3
    assert snapshot['summary']['boosted_underfill'] == 3
    assert snapshot['training_share'] == 0.45
    assert snapshot['corpus_share'] == 0.55
    assert snapshot['share_breakdown']['training_pct'] == 45


def test_palette_assignment_is_stable_for_existing_rows() -> None:
    window = ReviewDeckInspectorWindow.__new__(ReviewDeckInspectorWindow)
    window._live_state = _InspectorLiveState(color_by_item_id={'existing': ('red', '#d32f2f', 'white')})

    ReviewDeckInspectorWindow._reconcile_palette(window, ['existing', 'new'])

    assert window._live_state.color_by_item_id['existing'][0] == 'red'
    assert window._live_state.color_by_item_id['new'][0] != 'red'


def test_training_share_tooltip_uses_live_breakdown_values() -> None:
    window = ReviewDeckInspectorWindow.__new__(ReviewDeckInspectorWindow)
    window._latest_share_breakdown = {
        'due_active': 3,
        'boosted_active': 2,
        'urgent_active': 1,
        'due_equivalent': 6,
        'boosted_equivalent': 3,
        'urgent_equivalent': 1,
        'due_pct': 35,
        'boosted_pct': 6,
        'urgent_pct': 1,
        'training_pct': 42,
        'corpus_pct': 58,
    }

    tooltip = ReviewDeckInspectorWindow._training_share_tooltip_text(window)

    assert 'D=3 B=2 E=1' in tooltip
    assert 'Total training=42% | corpus remainder=58%' in tooltip


class _FakeHistoryTable:
    def __init__(self) -> None:
        self.rows: dict[str, tuple[str, str, str]] = {}
        self.tag_styles: dict[str, dict[str, str]] = {}

    def insert(self, _parent, _index, iid: str, values: tuple[str, str, str], tags=()):
        self.rows[iid] = values

    def exists(self, iid: str) -> bool:
        return iid in self.rows

    def delete(self, iid: str) -> None:
        self.rows.pop(iid, None)

    def item(self, iid: str, option=None, **kwargs):
        if 'values' in kwargs:
            self.rows[iid] = kwargs['values']
        if option == 'values':
            return self.rows[iid]
        return {'values': self.rows[iid]}

    def tag_configure(self, tag: str, **kwargs) -> None:
        self.tag_styles[tag] = kwargs

    def see(self, _iid: str) -> None:
        return None

    def get_children(self, _parent=''):
        return list(self.rows.keys())


def test_history_rows_show_pass_fail_and_source_colors() -> None:
    window = ReviewDeckInspectorWindow.__new__(ReviewDeckInspectorWindow)
    window.HISTORY_LIMIT = 400
    window._history_placeholder_iid = 'history_placeholder'
    window.history_table = _FakeHistoryTable()
    window._live_state = _InspectorLiveState()
    window._live_state.placeholder_visible = True
    window.history_table.insert('', 'end', iid='history_placeholder', values=('—', 'placeholder', '—'))

    ReviewDeckInspectorWindow._append_history_row(
        window,
        'Training',
        'e4 e5 Nf3',
        '—',
        '#1e88e5',
        'white',
        review_item_id='item-1',
    )
    ReviewDeckInspectorWindow._apply_training_outcome(window, 'item-1', 'PASS')
    ReviewDeckInspectorWindow._append_history_row(window, 'Corpus', '—', '—', '#000000', 'white')

    training_row = window._live_state.history_rows[0]
    corpus_row = window._live_state.history_rows[1]
    assert window.history_table.rows[training_row['iid']][2] == 'PASS'
    assert window.history_table.rows[corpus_row['iid']] == ('Corpus', '—', '—')
    assert window.history_table.tag_styles[corpus_row['iid']]['background'] == '#000000'


def test_summary_scroll_surface_is_canvas_backed() -> None:
    # Structural assertion: layout code names are present on class after build refactor.
    assert 'summary_canvas' in ReviewDeckInspectorWindow._build_layout.__code__.co_names
