from __future__ import annotations

import chess

from opening_trainer.models import MoveHistoryEntry, SessionOutcome, SessionState, SessionView
from opening_trainer.settings import TrainerSettings
from opening_trainer.session import TrainingSession
from opening_trainer.session_contracts import OutcomeBoardContract, OutcomeModalContract
from opening_trainer.ui.board_view import BoardView, DragState
from opening_trainer.ui.captured_material_panel import captured_pieces_and_material
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


class FakePane:
    def __init__(self):
        self._panes = []

    def panes(self):
        return list(self._panes)

    def add(self, widget, **kwargs):
        token = str(widget)
        if token not in self._panes:
            self._panes.append(token)

    def forget(self, widget):
        token = str(widget)
        if token in self._panes:
            self._panes.remove(token)


class FakeStorage:
    def __init__(self, root):
        self.root = root


class FakeSession:
    def __init__(self, root):
        self.review_storage = FakeStorage(root)
        self.start_calls = 0
        self.settings = TrainerSettings()
        self._max_depth = 5
        self.required_player_moves = 5
        self.config = type('Config', (), {'good_moves_acceptable': True})()
        self.saved_settings = None
        self.settings_store = self
        self.smart_profile = type('SmartProfile', (), {'reset_all': lambda self: None, 'set_level_for_current_track': lambda self, **kwargs: True})()

    def start_new_game(self):
        self.start_calls += 1

    def load(self, maximum_depth=None):
        return self.settings

    def update_settings(self, settings):
        self.saved_settings = settings
        self.settings = settings
        self.config.good_moves_acceptable = settings.good_moves_acceptable
        return settings

    def max_supported_training_depth(self):
        return self._max_depth

    def bundle_retained_ply_depth(self):
        return 10

    def corpus_summary_text(self):
        return 'Corpus: 1000-1200 | Retained depth: 10'

    def smart_profile_status(self):
        return type(
            'SmartProfileStatus',
            (),
            {
                'active': True,
                'track_id': 'rapid',
                'category_id': '600+0',
                'level': 4,
                'wins_toward_promotion': 2,
                'losses_toward_demotion': 1,
                'eligible_now': True,
                'eligibility_reason': 'Eligible ordinary corpus ladder game.',
            },
        )()

    def cancel_pending_opponent_action(self):
        return None

    def _timing_contract_metadata(self):
        return "600+0", "1000-1200"

    def _apply_settings(self, settings):
        self.update_settings(settings)


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
    gui.panel_visible = False
    gui.move_list_visible = True
    gui.compact_status_panel = FakeGridWidget()
    gui.panel_toggle_button = FakeButton()
    gui.root_pane = FakePane()
    gui.move_list_panel = FakeGridWidget()
    gui.inspector = FakeGridWidget()
    gui.session = FakeSession(tmp_path)
    gui._refresh_supporting_surfaces = lambda: None
    gui._set_panel_toggle_label = OpeningTrainerGUI._set_panel_toggle_label.__get__(gui, OpeningTrainerGUI)
    return gui


def test_request_shutdown_routes_to_authoritative_coordinator():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    calls: list[str] = []
    gui._shutdown_coordinator = lambda reason: calls.append(reason)

    gui._request_shutdown()

    assert calls == ['window_close']


def test_timing_override_dialog_entrypoint_opens_from_developer_menu():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    called = {"count": 0}
    gui.timing_override_dialog = type("Dialog", (), {"open": lambda self: called.__setitem__("count", called["count"] + 1)})()

    gui._open_timing_override_dialog()

    assert called["count"] == 1


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
    assert modal.contract.review_boards[0] == OutcomeBoardContract('What you should have played', chess.STARTING_FEN, chess.WHITE, 'd2d4', '#2e7d32', 'Correct move', 'd4')
    assert modal.contract.review_boards[1].board_fen != modal.contract.review_boards[0].board_fen
    assert modal.contract.review_boards[1].player_color is chess.WHITE
    assert modal.contract.review_boards[1].arrow_move_uci == 'g8f6'


def test_show_outcome_modal_fail_path_keeps_black_orientation(monkeypatch):
    outcome = SessionOutcome(
        False, 'Rejected by engine.', 'd4', None, 'fail', 'ordinary_corpus_play', 'immediate_retry', 'Default', 'Created new review item.',
        pre_fail_fen=chess.STARTING_FEN, post_fail_fen='rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b KQkq - 0 1',
        preferred_move_uci='d2d4', punishing_reply_uci='g8f6', player_color=chess.BLACK,
    )
    view = SessionView(chess.STARTING_FEN, chess.BLACK, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = object()
    gui._acknowledge_outcome = lambda: None
    monkeypatch.setattr('opening_trainer.ui.gui_app.OutcomeModal', RecordingModal)

    modal = gui._show_outcome_modal(view)

    assert all(board.player_color is chess.BLACK for board in modal.contract.review_boards)


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


def test_default_shell_layout_keeps_move_list_visible_and_training_panel_hidden(tmp_path):
    gui = _build_gui(tmp_path)

    gui._apply_shell_layout(initializing=True)

    assert gui.move_list_panel.visible is True
    assert gui.inspector.visible is False
    assert gui.panel_toggle_button.text == 'Show Training Panel'


def test_toggle_side_panel_reveals_training_panel_without_hiding_move_list(tmp_path):
    gui = _build_gui(tmp_path)
    gui._apply_shell_layout(initializing=True)

    gui._toggle_side_panel()
    assert gui.panel_visible is True
    assert gui.move_list_panel.visible is True
    assert gui.inspector.visible is True
    assert gui.panel_toggle_button.text == 'Hide Training Panel'
    assert gui.session.saved_settings.side_panel_visible is True
    assert gui.session.saved_settings.move_list_visible is True


def test_load_panel_visibility_preference_defaults_false_for_new_settings(tmp_path):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeSession(tmp_path)
    assert gui._load_panel_visibility_preference() is False
    assert gui._load_move_list_visibility_preference() is True


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


def test_board_coordinate_labels_invert_by_perspective():
    board_view = BoardView.__new__(BoardView)

    white_files, white_ranks = BoardView.coordinate_labels(board_view, chess.WHITE)
    black_files, black_ranks = BoardView.coordinate_labels(board_view, chess.BLACK)

    assert white_files == list('abcdefgh')
    assert white_ranks == ['8', '7', '6', '5', '4', '3', '2', '1']
    assert black_files == list('hgfedcba')
    assert black_ranks == ['1', '2', '3', '4', '5', '6', '7', '8']


def test_drag_release_reports_drop_square_and_active_state():
    board_view = BoardView.__new__(BoardView)
    board_view.drag_state = DragState(chess.E2, chess.E4, 'P', 120, 100, True)
    board_view.square_at_xy = lambda x, y, player_color: chess.E4

    assert BoardView.release_drag(board_view, 120, 90, chess.WHITE) == (chess.E2, chess.E4, True)
    assert board_view.drag_state is None


def test_captured_material_logic_tracks_delta_and_strips():
    board = chess.Board()
    board.remove_piece_at(chess.D8)
    board.remove_piece_at(chess.A1)

    white_captured, black_captured, delta = captured_pieces_and_material(board)

    assert '♛' in white_captured
    assert '♖' in black_captured
    assert delta == 4


def test_move_history_records_player_and_opponent_order():
    session = TrainingSession()
    session.player_color = chess.WHITE
    session.run_path = [
        __import__('opening_trainer.review.models', fromlist=['ReviewPathMove']).ReviewPathMove(0, 'white', 'e2e4', 'e4', chess.STARTING_FEN),
        __import__('opening_trainer.review.models', fromlist=['ReviewPathMove']).ReviewPathMove(1, 'black', 'e7e5', 'e5', chess.STARTING_FEN),
    ]

    assert session.move_history() == [
        MoveHistoryEntry(0, 'white', 'e2e4', 'e4', 'player'),
        MoveHistoryEntry(1, 'black', 'e7e5', 'e5', 'opponent'),
    ]


def test_corpus_summary_prefers_manifest_band_and_falls_back_to_bundle_name(tmp_path):
    session = TrainingSession()
    session.runtime_context = type('Runtime', (), {'config': type('Config', (), {'corpus_bundle_dir': tmp_path / '1200_1400', 'corpus_artifact_path': None})()})()
    metadata = type('Meta', (), {'manifest': {'target_rating_band': {'minimum': 1200, 'maximum': 1400}, 'retained_ply_depth': 12}})()
    session.opponent = type('Opponent', (), {'bundle_provider': type('Provider', (), {'bundle': type('Bundle', (), {'metadata': metadata})()})()})()

    assert session.corpus_summary_text() == 'Corpus: 1200-1400 | Retained depth: 12 | Opponent timing: off'

    session.opponent = type('Opponent', (), {'bundle_provider': type('Provider', (), {'bundle': type('Bundle', (), {'metadata': type('Meta', (), {'manifest': {}})()})()})()})()
    assert '1200 1400' in session.corpus_summary_text()


def test_recent_status_text_is_human_readable_and_hides_internal_keys(tmp_path):
    gui = _build_gui(tmp_path)
    gui.session.required_player_moves = 4
    gui.session._max_depth = 6
    gui.session.settings = TrainerSettings(good_moves_acceptable=True, active_training_ply_depth=4, side_panel_visible=False, move_list_visible=True)

    text = gui._build_recent_status_text("Session route: corpus training")

    assert "Session route: corpus training" in text
    assert "Recent status:" in text
    assert "corpus_share=" not in text
    assert "deck_size=" not in text
    assert "timing_overlay=" not in text



def test_captured_strip_panel_follows_player_perspective():
    from opening_trainer.ui.captured_material_panel import CapturedMaterialPanel

    panel = CapturedMaterialPanel.__new__(CapturedMaterialPanel)
    panel.primary_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    panel.secondary_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    panel.delta_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    board = chess.Board()
    board.remove_piece_at(chess.D8)
    board.remove_piece_at(chess.A1)

    CapturedMaterialPanel.update_board(panel, board, player_color=chess.BLACK, near_side=True)
    assert panel.primary_var.value.startswith('Black captured:')
    assert '♖' in panel.primary_var.value
    assert panel.secondary_var.value.startswith('White captured:')
    assert '♛' in panel.secondary_var.value


def test_remembered_bundle_path_defaults_to_none(tmp_path):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeSession(tmp_path)
    assert gui._remembered_bundle_path() is None



def test_load_selected_bundle_invokes_loading_state(monkeypatch, tmp_path):
    gui = _build_gui(tmp_path)
    gui.session.runtime_context = type('RuntimeContext', (), {'config': type('Config', (), {
        'corpus_artifact_path': None,
        'engine_executable_path': None,
        'opening_book_path': None,
        'engine_depth': None,
        'engine_time_limit_seconds': None,
        'strict_assets': False,
    })()})()
    gui._load_move_list_visibility_preference = lambda: True
    gui._load_panel_visibility_preference = lambda: False
    gui._update_bundle_summary = lambda: None
    gui._apply_shell_layout = lambda initializing=False: None
    gui._hide_bundle_picker = lambda: None
    gui._start_game = lambda loading_message=None: None
    gui.inspector = type('Inspector', (), {'session': None})()
    seen = {}

    monkeypatch.setattr('opening_trainer.ui.gui_app.inspect_corpus_bundle', lambda path: type('Compat', (), {'available': True, 'bundle_dir': path, 'detail': 'ok'})())
    monkeypatch.setattr('opening_trainer.ui.gui_app.TrainingSession', lambda runtime_context, mode, review_storage: type('Session', (), {
        'runtime_context': runtime_context,
        'review_storage': review_storage,
        'settings': gui.session.settings,
        'settings_store': gui.session.settings_store,
        'max_supported_training_depth': lambda self=None: 5,
        'update_settings': lambda self, settings: settings,
    })())
    gui._build_runtime_for_bundle = lambda bundle_path: {'bundle': bundle_path}
    gui._start_loading_job = lambda **kwargs: seen.update(kwargs)

    gui._load_selected_bundle(str(tmp_path / 'bundle'))

    assert 'Loading corpus bundle' in seen['initial_message']



def test_initialize_app_shell_recovers_when_remembered_bundle_missing(tmp_path):
    gui = _build_gui(tmp_path)
    gui.session.settings = TrainerSettings(last_bundle_path=str(tmp_path / 'missing_bundle'))
    seen = {}
    gui._show_bundle_picker = lambda message=None: seen.setdefault('message', message)

    gui._initialize_app_shell()

    assert 'Choose a corpus bundle' in seen['message']


class FakeRoot:
    def __init__(self):
        self.after_calls = []
        self.cancelled = []
        self.updated = 0

    def after(self, delay, callback):
        handle = f'h{len(self.after_calls)}'
        self.after_calls.append((delay, callback, handle))
        return handle

    def after_cancel(self, handle):
        self.cancelled.append(handle)

    def update_idletasks(self):
        self.updated += 1


class FakeVar:
    def __init__(self):
        self.value = None

    def set(self, value):
        self.value = value

    def get(self):
        return self.value


class FakeLoadingFrame:
    def __init__(self):
        self.placed = False
        self.place_calls = []

    def place(self, **kwargs):
        self.placed = True
        self.place_calls.append(kwargs)

    def place_forget(self):
        self.placed = False

    def lift(self):
        return None


class FakeProgress:
    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self, _interval):
        self.started = True

    def stop(self):
        self.stopped = True


class FakeStateButton:
    def __init__(self):
        self.state = None

    def configure(self, **kwargs):
        if 'state' in kwargs:
            self.state = kwargs['state']


class ImmediateThread:
    def __init__(self, target=None, daemon=None):
        self.target = target

    def start(self):
        if self.target:
            self.target()


def test_initialize_shell_autoloads_remembered_bundle_via_loading_job(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._remembered_bundle_path = lambda: '/bundle'
    gui._bundle_path_is_valid = lambda path: True
    calls = []
    gui._load_selected_bundle = lambda path: calls.append(path)
    gui._show_bundle_picker = lambda message: calls.append(message)

    gui._initialize_app_shell()

    assert calls == ['/bundle']


def test_load_selected_bundle_starts_background_loading_job(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    calls = {}
    gui._start_loading_job = lambda **kwargs: calls.update(kwargs)

    gui._load_selected_bundle('/bundle')

    assert 'Loading corpus bundle' in calls['initial_message']
    assert callable(calls['worker'])
    assert callable(calls['on_success'])


def test_start_loading_job_shows_loading_and_polls_without_touching_widgets_from_worker(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui.start_button = FakeStateButton()
    gui.loading_var = FakeVar()
    gui.loading_frame = FakeLoadingFrame()
    gui.loading_progress = FakeProgress()
    gui._loading_job_active = False
    monkeypatch.setattr('opening_trainer.ui.gui_app.threading.Thread', ImmediateThread)

    observed = {'success': None}
    gui._start_loading_job(
        initial_message='Initializing corpus payload…',
        worker=lambda: 'ready',
        on_success=lambda payload: observed.__setitem__('success', payload),
        on_error=lambda exc: observed.__setitem__('error', str(exc)),
    )
    assert gui.loading_frame.placed is True
    assert gui.loading_var.get() == 'Initializing corpus payload…'
    assert gui.start_button.state == 'disabled'
    assert gui.root.after_calls

    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()

    assert observed['success'] == 'ready'
    assert gui.loading_progress.stopped is True
    assert gui.start_button.state == 'normal'


def test_schedule_pending_opponent_commit_defers_commit_until_after_callback():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._pending_opponent_after_handle = None
    committed = {'count': 0}
    pending_box = {'value': None}
    generation = {'id': 0}
    gui._refresh_view = lambda: None
    class Session:
        state = SessionState.OPPONENT_TURN
        pending_opponent_action = None

        def prepare_pending_opponent_action(self):
            generation['id'] += 1
            pending_box['value'] = type('Pending', (), {'visible_delay_seconds': 0.2, 'generation': generation['id']})()
            self.pending_opponent_action = pending_box['value']
            return self.pending_opponent_action

        def commit_pending_opponent_action(self):
            if self.pending_opponent_action is None:
                return False
            committed['count'] += 1
            self.pending_opponent_action = None
            self.state = SessionState.PLAYER_TURN
            return True

        def cancel_pending_opponent_action(self):
            self.pending_opponent_action = None

    gui.session = Session()

    OpeningTrainerGUI._schedule_pending_opponent_commit(gui)

    assert len(gui.root.after_calls) == 1
    assert committed['count'] == 0
    assert gui.session.pending_opponent_action is pending_box['value']
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()
    assert committed['count'] == 1
    assert gui.session.pending_opponent_action is None


def test_schedule_pending_opponent_commit_old_order_would_clear_new_pending_action():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._pending_opponent_after_handle = None
    committed = {'count': 0}
    gui._refresh_view = lambda: None

    class Session:
        state = SessionState.OPPONENT_TURN
        pending_opponent_action = None

        def prepare_pending_opponent_action(self):
            self.pending_opponent_action = type('Pending', (), {'visible_delay_seconds': 0.1})()
            return self.pending_opponent_action

        def commit_pending_opponent_action(self):
            if self.pending_opponent_action is None:
                return False
            committed['count'] += 1
            self.pending_opponent_action = None
            return True

        def cancel_pending_opponent_action(self):
            self.pending_opponent_action = None

    gui.session = Session()

    def broken_order_schedule():
        pending = gui.session.prepare_pending_opponent_action()
        if pending is None:
            return
        OpeningTrainerGUI._cancel_pending_opponent_callback(gui)
        gui._pending_opponent_after_handle = gui._schedule_after(100, gui._commit_scheduled_opponent_action)

    broken_order_schedule()

    assert gui.session.pending_opponent_action is None
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()
    assert committed['count'] == 0


def test_schedule_pending_opponent_commit_replaces_stale_callback_and_stale_pending_action():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = {'h0'}
    gui._is_shutting_down = False
    gui._pending_opponent_after_handle = 'h0'
    gui._refresh_view = lambda: None
    committed_generations: list[int] = []

    class Session:
        state = SessionState.OPPONENT_TURN

        def __init__(self):
            self.pending_opponent_action = type('Pending', (), {'visible_delay_seconds': 0.3, 'generation': 0})()
            self.next_generation = 1

        def prepare_pending_opponent_action(self):
            pending = type('Pending', (), {'visible_delay_seconds': 0.2, 'generation': self.next_generation})()
            self.next_generation += 1
            self.pending_opponent_action = pending
            return pending

        def commit_pending_opponent_action(self):
            pending = self.pending_opponent_action
            if pending is None:
                return False
            committed_generations.append(pending.generation)
            self.pending_opponent_action = None
            self.state = SessionState.PLAYER_TURN
            return True

        def cancel_pending_opponent_action(self):
            self.pending_opponent_action = None

    gui.session = Session()

    OpeningTrainerGUI._schedule_pending_opponent_commit(gui)

    assert gui.root.cancelled == ['h0']
    assert len(gui.root.after_calls) == 1
    assert gui.session.pending_opponent_action is not None
    assert gui.session.pending_opponent_action.generation == 1
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()
    assert committed_generations == [1]
    assert gui.session.pending_opponent_action is None


def test_cancel_pending_opponent_callback_cancels_after_handle():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = {'h5'}
    gui._pending_opponent_after_handle = 'h5'
    cancelled = {'count': 0}
    gui.session = type('Session', (), {'cancel_pending_opponent_action': lambda self=None: cancelled.__setitem__('count', cancelled['count'] + 1)})()

    OpeningTrainerGUI._cancel_pending_opponent_callback(gui)

    assert gui.root.cancelled == ['h5']
    assert gui._pending_opponent_after_handle is None
    assert cancelled['count'] == 1


def test_training_depth_summary_reports_updated_bundle_cap(tmp_path):
    gui = _build_gui(tmp_path)
    gui.session._max_depth = 15
    gui.session.bundle_retained_ply_depth = lambda: 30

    summary = OpeningTrainerGUI._training_depth_summary(gui)

    assert 'App max: 15 player moves' in summary
    assert 'Bundle max: 15 player moves' in summary
