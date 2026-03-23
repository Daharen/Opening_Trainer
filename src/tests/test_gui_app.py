from __future__ import annotations

import json

import chess

from opening_trainer.models import SessionOutcome, SessionState, SessionView
from opening_trainer.session_contracts import OutcomeBoardContract, OutcomeModalContract
from opening_trainer.ui.gui_app import OpeningTrainerGUI


class FakeButton:
    def __init__(self):
        self.text = None

    def configure(self, **kwargs):
        self.text = kwargs.get('text', self.text)


class FakeGridWidget:
    def __init__(self):
        self.visible = True
        self.grid_calls = []
        self.width = None

    def grid(self, **kwargs):
        self.visible = True
        self.grid_calls.append(kwargs)

    def grid_remove(self):
        self.visible = False

    def configure(self, **kwargs):
        self.width = kwargs.get('width', self.width)


class FakeRoot:
    def __init__(self):
        self.column_settings = []

    def grid_columnconfigure(self, column, **kwargs):
        self.column_settings.append((column, kwargs))


class FakeStorage:
    def __init__(self, root):
        self.root = root


class FakeSession:
    def __init__(self, root):
        self.review_storage = FakeStorage(root)
        self.start_calls = 0

    def start_new_game(self):
        self.start_calls += 1


class RecordingModal:
    def __init__(self, master, contract, on_continue):
        self.master = master
        self.contract = contract
        self.on_continue = on_continue


class FakeViewSession:
    def __init__(self, view):
        self._view = view

    def get_view(self):
        return self._view

    def legal_moves_from(self, square):
        return []


class BoardViewStub:
    def set_selection(self, *args, **kwargs):
        return None

    def render(self, *args, **kwargs):
        return None


def _build_gui(tmp_path):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.panel_visible = True
    gui.side_panel = FakeGridWidget()
    gui.compact_status_panel = FakeGridWidget()
    gui.panel_toggle_button = FakeButton()
    gui.root = FakeRoot()
    gui.side_panel_padx = (0, 12)
    gui.side_panel_pady = (6, 12)
    gui.session = FakeSession(tmp_path)
    gui._refresh_supporting_surfaces = lambda: None
    return gui


def test_outcome_modal_contract_shape_includes_required_acknowledgement_default():
    contract = OutcomeModalContract('FAIL', 'summary', 'reason', 'e4', 'route', 'next', 'impact')

    assert contract.headline == 'FAIL'
    assert contract.summary == 'summary'
    assert contract.reason == 'reason'
    assert contract.preferred_move == 'e4'
    assert contract.requires_acknowledgement is True
    assert contract.review_boards == ()


def test_show_outcome_modal_success_path_builds_valid_contract(monkeypatch, tmp_path):
    outcome = SessionOutcome(
        True,
        'Completed 1 accepted player moves inside the opening window.',
        None,
        None,
        'pass',
        'scheduled_review',
        'ordinary_corpus_play',
        'Default',
        'Review item improved.',
    )
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = object()
    gui._acknowledge_outcome = lambda: None
    monkeypatch.setattr('opening_trainer.ui.gui_app.OutcomeModal', RecordingModal)

    modal = gui._show_outcome_modal(view)

    assert modal.contract.headline == 'SUCCESS'
    assert modal.contract.reason == outcome.reason
    assert modal.contract.preferred_move is None
    assert modal.contract.routing_reason == 'scheduled_review'
    assert 'Profile: Default' in modal.contract.impact_summary
    assert modal.contract.review_boards == ()


def test_show_outcome_modal_fail_path_builds_dual_board_contract(monkeypatch):
    outcome = SessionOutcome(
        False,
        'Rejected by engine.',
        'd4',
        None,
        'fail',
        'ordinary_corpus_play',
        'immediate_retry',
        'Default',
        'Created new review item.',
        pre_fail_fen=chess.STARTING_FEN,
        post_fail_fen='rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b KQkq - 0 1',
        preferred_move_uci='d2d4',
        preferred_move_san='d4',
        punishing_reply_uci='g8f6',
        punishing_reply_san='Nf6',
    )
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = object()
    gui._acknowledge_outcome = lambda: None
    monkeypatch.setattr('opening_trainer.ui.gui_app.OutcomeModal', RecordingModal)

    modal = gui._show_outcome_modal(view)

    assert modal.contract.headline == 'FAIL'
    assert modal.contract.preferred_move == 'd4'
    assert modal.contract.next_routing_reason == 'immediate_retry'
    assert len(modal.contract.review_boards) == 2
    assert modal.contract.review_boards[0] == OutcomeBoardContract('What you should have played', chess.STARTING_FEN, 'd2d4', '#2e7d32', 'Correct move', 'd4')
    assert modal.contract.review_boards[1].board_fen != modal.contract.review_boards[0].board_fen
    assert modal.contract.review_boards[1].arrow_move_uci == 'g8f6'


def test_show_outcome_modal_fail_path_survives_missing_punishing_reply(monkeypatch):
    outcome = SessionOutcome(
        False,
        'Rejected by engine.',
        'd4',
        None,
        'fail',
        'ordinary_corpus_play',
        'immediate_retry',
        'Default',
        'Created new review item.',
        pre_fail_fen=chess.STARTING_FEN,
        post_fail_fen='rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b KQkq - 0 1',
        preferred_move_uci='d2d4',
        preferred_move_san='d4',
        punishing_reply_uci=None,
        punishing_reply_san=None,
    )
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = object()
    gui._acknowledge_outcome = lambda: None
    monkeypatch.setattr('opening_trainer.ui.gui_app.OutcomeModal', RecordingModal)

    modal = gui._show_outcome_modal(view)

    assert len(modal.contract.review_boards) == 1
    assert modal.contract.review_boards[0].arrow_move_uci == 'd2d4'


def test_side_panel_layout_keeps_board_region_dominant(tmp_path):
    gui = _build_gui(tmp_path)

    gui._apply_side_panel_layout(initializing=True)

    assert gui.side_panel.visible is True
    assert gui.side_panel.width == 320
    assert gui.root.column_settings[-2:] == [
        (0, {'weight': 8, 'minsize': 420}),
        (1, {'weight': 1, 'minsize': 240, 'pad': 0}),
    ]


def test_toggle_side_panel_hides_and_restores_panel_state(tmp_path):
    gui = _build_gui(tmp_path)

    gui._toggle_side_panel()
    assert gui.panel_visible is False
    assert gui.side_panel.visible is False
    assert gui.compact_status_panel.visible is True
    assert gui.panel_toggle_button.text == 'Show Panel'

    gui._toggle_side_panel()
    assert gui.panel_visible is True
    assert gui.side_panel.visible is True
    assert gui.panel_toggle_button.text == 'Hide Panel'

    payload = json.loads((tmp_path / 'gui_state.json').read_text(encoding='utf-8'))
    assert payload == {'side_panel_visible': True}


def test_load_panel_visibility_preference_defaults_true_on_invalid_json(tmp_path):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeSession(tmp_path)
    (tmp_path / 'gui_state.json').write_text('{broken', encoding='utf-8')

    assert gui._load_panel_visibility_preference() is True


def test_refresh_view_does_not_advance_until_modal_acknowledged(monkeypatch):
    outcome = SessionOutcome(False, 'Rejected by engine.', 'd4', None, 'fail', 'ordinary_corpus_play', 'immediate_retry', 'Default', 'Created new review item.')
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeViewSession(view)
    gui.selected_square = None
    gui.pending_restart = False
    gui.board_view = BoardViewStub()
    gui._refresh_supporting_surfaces = lambda: None
    called = {'count': 0}
    gui._show_outcome_modal = lambda current_view: called.__setitem__('count', called['count'] + 1)

    gui._refresh_view()

    assert gui.pending_restart is True
    assert called['count'] == 1


def test_acknowledge_outcome_starts_next_game_only_after_modal_callback(tmp_path):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeSession(tmp_path)
    gui.pending_restart = True
    gui.selected_square = chess.E2
    refresh_calls = {'count': 0}
    gui._refresh_view = lambda transient_status=None: refresh_calls.__setitem__('count', refresh_calls['count'] + 1)

    gui._acknowledge_outcome()

    assert gui.pending_restart is False
    assert gui.selected_square is None
    assert gui.session.start_calls == 1
    assert refresh_calls['count'] == 1
