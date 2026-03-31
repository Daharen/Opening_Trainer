from __future__ import annotations

import chess

from opening_trainer.review.models import ReviewItem, ReviewPathMove
from opening_trainer.ui.board_view import square_is_light
from opening_trainer.ui.review_inspector import ReviewInspector


class _FakeTree:
    def __init__(self, focused_id: str):
        self._focused_id = focused_id

    def focus(self):
        return self._focused_id


class _FakeStorage:
    def __init__(self, items):
        self._items = items

    def load_items(self, _profile_id):
        return list(self._items)


class _FakeSession:
    def __init__(self, items):
        self.active_profile_id = 'default'
        self.review_storage = _FakeStorage(items)
        self.calls = []
        self.runtime_context = type(
            "RuntimeContext",
            (),
            {"config": type("Config", (), {"predecessor_master_db_path": "/tmp/predecessor.sqlite"})()},
        )()

    def edit_review_item(self, review_item_id: str, **payload):
        self.calls.append((review_item_id, payload))
        return next(item for item in self.review_storage.load_items(self.active_profile_id) if item.review_item_id == review_item_id)


def _build_item(origin_kind: str = 'auto_captured_failure', presentation_mode: str = 'play_to_position') -> ReviewItem:
    item = ReviewItem.create(
        'default',
        'k',
        chess.STARTING_FEN,
        'white',
        'fail',
        'e2e4',
        [],
        [ReviewPathMove(0, 'white', 'e2e4', 'e4', chess.STARTING_FEN)],
    )
    item.origin_kind = origin_kind
    item.manual_presentation_mode = presentation_mode
    item.manual_forced_player_color = 'black'
    item.operator_note = 'note'
    item.urgency_tier = 'boosted_review'
    item.allow_below_threshold_reach = True
    item.predecessor_line_uci = 'e2e4 e7e5'
    return item


def test_square_color_convention_matches_real_board() -> None:
    assert square_is_light(chess.A1) is False
    assert square_is_light(chess.H1) is True
    assert square_is_light(chess.A8) is True
    assert square_is_light(chess.H8) is False


def test_edit_item_always_opens_regular_manual_dialog(monkeypatch) -> None:
    item = _build_item(origin_kind='manual_target', presentation_mode='manual_setup_start')
    inspector = ReviewInspector.__new__(ReviewInspector)
    inspector.tree = _FakeTree(item.review_item_id)
    inspector.session = _FakeSession([item])
    inspector.refresh_callback = lambda: None

    calls: dict[str, dict] = {}

    def _record_manual(_master, _on_save, *, title, initial, predecessor_master_db_path):
        calls['manual'] = {'title': title, 'initial': initial, 'predecessor_master_db_path': predecessor_master_db_path}

    def _record_board(*_args, **_kwargs):
        calls['board'] = {'called': True}

    monkeypatch.setattr('opening_trainer.ui.review_inspector.ManualTargetDialog', _record_manual)
    monkeypatch.setattr('opening_trainer.ui.review_inspector.BoardSetupEditorDialog', _record_board)

    inspector._edit_item()

    assert 'manual' in calls
    assert 'board' not in calls
    assert calls['manual']['title'] == 'Edit Review Item'
    assert calls['manual']['initial']['target_fen'] == chess.STARTING_FEN
    assert calls['manual']['initial']['manual_forced_player_color'] == 'black'
    assert calls['manual']['predecessor_master_db_path'] == '/tmp/predecessor.sqlite'


def test_board_edit_opens_board_setup_for_any_item(monkeypatch) -> None:
    item = _build_item(origin_kind='auto_captured_failure', presentation_mode='play_to_position')
    inspector = ReviewInspector.__new__(ReviewInspector)
    inspector.tree = _FakeTree(item.review_item_id)
    inspector.session = _FakeSession([item])
    inspector.refresh_callback = lambda: None

    calls: dict[str, dict] = {}

    def _record_board(_master, _on_save, *, title, initial):
        calls['board'] = {'title': title, 'initial': initial}

    monkeypatch.setattr('opening_trainer.ui.review_inspector.BoardSetupEditorDialog', _record_board)

    inspector._edit_item_in_board_setup()

    assert calls['board']['title'] == 'Edit in Board Setup'
    assert calls['board']['initial']['target_fen'] == chess.STARTING_FEN
    assert calls['board']['initial']['predecessor_line_uci'] == 'e2e4 e7e5'
