from __future__ import annotations

import inspect
import time

import chess

from opening_trainer.models import MoveHistoryEntry, SessionOutcome, SessionState, SessionView
from opening_trainer.settings import DEFAULT_TRAINING_PANEL_COLUMNS, TrainerSettings
from opening_trainer.session import TrainingSession
from opening_trainer.session_contracts import OutcomeBoardContract, OutcomeModalContract
from opening_trainer.ui.board_view import (
    ANIMATION_START_LEAD_SECONDS,
    BoardView,
    DEFAULT_IMMEDIATE_FRAME_MIN_PROGRESS,
    DragState,
    PIECE_GLYPHS,
    SettleAnimationState,
)
from opening_trainer.ui.captured_material_panel import captured_pieces_and_material
from opening_trainer.ui.gui_app import (
    OPPONENT_COMMITTED_MOVE_DURATION_MS,
    PLAYER_COMMITTED_MOVE_DURATION_MS,
    OpeningTrainerGUI,
)


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
        self.smart_profile = type('SmartProfile', (), {'reset_all': lambda self: None, 'set_level_for_current_track': lambda self, **kwargs: True, 'resolve_expected_bundle': lambda self, _root: type('R', (), {'resolved_entry': None})()})()

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
                'consecutive_eligible_successes': 2,
                'consecutive_eligible_failures': 1,
                'expected_bundle_summary': 'Expected: 600+0 / 1000-1200 -> /tmp/bundle',
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


class FakeStringVar:
    def __init__(self, value: str = ""):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value


class FakeCombo:
    def __init__(self, values=()):
        self._values = tuple(values)

    def configure(self, **kwargs):
        if "values" in kwargs:
            self._values = tuple(kwargs["values"])

    def cget(self, name):
        if name == "values":
            return self._values
        raise KeyError(name)


class FakeBoolVar:
    def __init__(self, value: bool = False):
        self._value = bool(value)

    def get(self):
        return self._value

    def set(self, value):
        self._value = bool(value)


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


def test_toolbar_has_single_corpus_selection_entrypoint():
    source = inspect.getsource(OpeningTrainerGUI.__init__)

    assert "text='Corpus Selection'" in source
    assert "Back to Corpus Selection" not in source
    assert "text='Corpus bundle', command=self._open_bundle_picker" not in source


def test_smart_mode_prefills_expected_catalog_coordinates_and_variant():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.catalog_grouped = {
        "Rapid": {
            "600+0": {
                "400-600": (),
            }
        }
    }
    expected_entry = type(
        "Entry",
        (),
        {
            "bundle_dir": "/tmp/expected",
            "retained_ply_depth": None,
            "rating_policy": None,
            "max_supported_player_moves": None,
        },
    )()
    gui.catalog_leaf_variants = [expected_entry]
    gui.catalog_category_var = FakeStringVar("Rapid")
    gui.catalog_time_control_var = FakeStringVar("600+0")
    gui.catalog_rating_band_var = FakeStringVar("400-600")
    gui.catalog_variant_var = FakeStringVar("")
    gui.catalog_time_control_combo = FakeCombo(values=("600+0",))
    gui.catalog_rating_band_combo = FakeCombo(values=("400-600",))
    gui._refresh_catalog_time_controls = lambda: None
    gui._refresh_catalog_rating_bands = lambda: None
    gui._refresh_catalog_variants = lambda: None
    gui._update_catalog_summary = lambda: None
    gui.session = type(
        "Session",
        (),
        {
            "smart_profile_status": lambda self: type(
                "Status",
                (),
                {
                    "active": True,
                    "category_id": "600+0",
                    "expected_bundle_summary": "Expected: 600+0 / 400-600 -> /tmp/expected",
                },
            )(),
            "smart_profile_expected_bundle_path": lambda self: "/tmp/expected",
        },
    )()

    gui._prefill_catalog_for_mode()

    assert gui.catalog_category_var.get() == "Rapid"
    assert gui.catalog_time_control_var.get() == "600+0"
    assert gui.catalog_rating_band_var.get() == "400-600"
    assert gui.catalog_variant_var.get() == "Default"


def test_load_expected_bundle_action_loads_bundle_when_present():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    loaded = {"path": None}
    gui._load_selected_bundle = lambda path: loaded.__setitem__("path", path)
    gui.root = object()
    gui.session = type(
        "Session",
        (),
        {
            "smart_profile_status": lambda self: type("Status", (), {"active": True, "expected_bundle_summary": "Expected: 600+0 / 400-600 -> /tmp/expected"})(),
            "smart_profile_expected_bundle_path": lambda self: "/tmp/expected",
        },
    )()

    gui._load_expected_smart_profile_bundle()

    assert loaded["path"] == "/tmp/expected"


def test_load_expected_bundle_action_shows_clear_message_when_missing(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = object()
    errors: list[str] = []
    monkeypatch.setattr("opening_trainer.ui.gui_app.messagebox.showerror", lambda _title, message, parent=None: errors.append(message))
    gui.session = type(
        "Session",
        (),
        {
            "smart_profile_status": lambda self: type("Status", (), {"active": True, "expected_bundle_summary": "Expected: 600+0 / 400-600 -> unavailable"})(),
            "smart_profile_expected_bundle_path": lambda self: None,
        },
    )()

    gui._load_expected_smart_profile_bundle()

    assert errors == ["Expected bundle missing. Expected: 600+0 / 400-600 -> unavailable"]


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


def test_start_drag_preserves_piece_attachment_until_threshold_crossed():
    board_view = BoardView.__new__(BoardView)
    board_view.settle_animation = object()

    BoardView.start_drag(board_view, chess.E2, "P", 100, 120)

    assert board_view.settle_animation is None
    assert board_view.drag_state is not None
    assert board_view.drag_state.source_square == chess.E2
    assert board_view.drag_state.moved is False


def test_render_draws_settle_piece_without_drag_state():
    board_view = BoardView.__new__(BoardView)
    board_view.board_size = 480
    board_view.square_size = 53
    board_view.selected_square = None
    board_view.highlight_squares = set()
    board_view.arrow_move_uci = None
    board_view.drag_state = None
    board_view.settle_animation = SettleAnimationState("P", 120.0, 120.0, 220.0, 220.0, chess.E4, start_time=0.0, duration_seconds=0.1)
    board_view.delete = lambda *_args, **_kwargs: None
    board_view.create_rectangle = lambda *_args, **_kwargs: None
    drawn: list[tuple[float, float, str]] = []
    board_view.create_text = lambda x, y, **kwargs: drawn.append((x, y, kwargs["text"]))
    board_view.create_line = lambda *_args, **_kwargs: None
    board_view._draw_coordinates = lambda _player_color: None
    board_view._draw_arrow = lambda _player_color: None
    board_view.sample_animation_position = lambda: (200.0, 190.0)
    board = chess.Board("4k3/8/8/8/4P3/8/8/4K3 w - - 0 1")

    BoardView.render(board_view, board, chess.WHITE)

    assert (200.0, 190.0, PIECE_GLYPHS["P"]) in drawn


def test_draw_settle_piece_sampling_is_non_mutating():
    board_view = BoardView.__new__(BoardView)
    board_view.square_size = 53
    board_view.settle_animation = SettleAnimationState("P", 120.0, 120.0, 220.0, 220.0, chess.E4, start_time=0.0, duration_seconds=0.1)
    sampled = {'count': 0}
    board_view.sample_animation_position = lambda: sampled.__setitem__('count', sampled['count'] + 1) or (200.0, 190.0)
    board_view.create_text = lambda *_args, **_kwargs: None

    BoardView._draw_settle_piece(board_view)

    assert sampled['count'] == 1
    assert board_view.settle_animation is not None


def test_start_committed_move_animation_uses_square_centers():
    board_view = BoardView.__new__(BoardView)
    board_view.board_size = 480
    board_view.square_size = 53
    board_view.settle_animation = None

    BoardView.start_committed_move_animation(
        board_view,
        piece_symbol="n",
        source_square=chess.G8,
        destination_square=chess.F6,
        player_color=chess.WHITE,
        duration_ms=70,
    )

    assert board_view.settle_animation is not None
    assert board_view.settle_animation.piece_symbol == "n"
    assert board_view.settle_animation.destination_square == chess.F6
    start_x, start_y = BoardView._square_center(board_view, chess.G8, chess.WHITE)
    end_x, end_y = BoardView._square_center(board_view, chess.F6, chess.WHITE)
    assert board_view.settle_animation.start_x == start_x
    assert board_view.settle_animation.start_y == start_y
    assert board_view.settle_animation.end_x == end_x
    assert board_view.settle_animation.end_y == end_y


def test_start_committed_move_animation_uses_explicit_release_start_coordinates():
    board_view = BoardView.__new__(BoardView)
    board_view.board_size = 480
    board_view.square_size = 53
    board_view.settle_animation = None

    BoardView.start_committed_move_animation(
        board_view,
        piece_symbol="P",
        source_square=chess.E2,
        destination_square=chess.E4,
        player_color=chess.WHITE,
        start_x=121.0,
        start_y=247.0,
        duration_ms=95,
    )

    assert board_view.settle_animation is not None
    assert board_view.settle_animation.start_x == 121.0
    assert board_view.settle_animation.start_y == 247.0


def test_animation_status_query_does_not_clear_animation_state():
    board_view = BoardView.__new__(BoardView)
    board_view.settle_animation = SettleAnimationState("P", 10.0, 20.0, 80.0, 120.0, chess.E4, start_time=10.0, duration_seconds=0.1)

    assert BoardView.animation_in_progress(board_view) is False
    assert board_view.settle_animation is not None


def test_start_committed_move_animation_biases_start_time_for_immediate_motion():
    board_view = BoardView.__new__(BoardView)
    board_view.board_size = 480
    board_view.square_size = 53
    board_view.settle_animation = None

    BoardView.start_committed_move_animation(
        board_view,
        piece_symbol="P",
        source_square=chess.E2,
        destination_square=chess.E4,
        player_color=chess.WHITE,
    )

    assert board_view.settle_animation is not None
    assert board_view.settle_animation.start_time <= time.monotonic() - (ANIMATION_START_LEAD_SECONDS * 0.8)


def test_force_immediate_visible_frame_advances_animation_progress():
    board_view = BoardView.__new__(BoardView)
    board_view.settle_animation = SettleAnimationState(
        "P",
        10.0,
        15.0,
        50.0,
        95.0,
        chess.E4,
        start_time=time.monotonic(),
        duration_seconds=0.1,
    )

    progress = BoardView.force_immediate_visible_frame(board_view, min_progress=0.2)

    assert progress is not None
    assert progress >= 0.2
    assert board_view.settle_animation is not None
    assert board_view.settle_animation.start_time <= time.monotonic() - 0.018


def test_force_immediate_visible_frame_uses_default_min_progress_window():
    board_view = BoardView.__new__(BoardView)
    board_view.settle_animation = SettleAnimationState(
        "P",
        10.0,
        15.0,
        50.0,
        95.0,
        chess.E4,
        start_time=time.monotonic(),
        duration_seconds=0.2,
    )

    progress = BoardView.force_immediate_visible_frame(board_view)

    assert progress is not None
    assert progress >= DEFAULT_IMMEDIATE_FRAME_MIN_PROGRESS


def test_probe_animation_duration_constants_are_applied():
    assert PLAYER_COMMITTED_MOVE_DURATION_MS == 240
    assert OPPONENT_COMMITTED_MOVE_DURATION_MS == 220


def test_board_resize_coalesces_redraw_and_refreshes_latest_board():
    board_view = BoardView.__new__(BoardView)
    board_view.min_board_size = 360
    board_view.board_size = 480
    board_view.square_size = 53
    board_view._last_board_fen = chess.STARTING_FEN
    board_view._last_player_color = chess.WHITE
    board_view._resize_refresh_after_handle = None
    configure_calls: list[tuple[int, int]] = []
    board_view.configure = lambda **kwargs: configure_calls.append((kwargs['width'], kwargs['height']))
    cancelled: list[str] = []
    board_view.after_cancel = lambda handle: cancelled.append(handle)
    idle_callbacks: list[tuple[str, object]] = []

    def fake_after_idle(callback):
        handle = f'i{len(idle_callbacks)}'
        idle_callbacks.append((handle, callback))
        return handle

    board_view.after_idle = fake_after_idle
    render_calls: list[tuple[str, chess.Color]] = []
    board_view.render = lambda board, player_color: render_calls.append((board.fen(), player_color))

    BoardView._on_resize(board_view, type('Event', (), {'width': 500, 'height': 420})())
    BoardView._on_resize(board_view, type('Event', (), {'width': 530, 'height': 440})())

    assert configure_calls == [(420, 420), (440, 440)]
    assert cancelled == ['i0']
    assert board_view._resize_refresh_after_handle == 'i1'

    _latest_handle, callback = idle_callbacks[-1]
    callback()

    assert board_view._resize_refresh_after_handle is None
    assert render_calls == [(chess.STARTING_FEN, chess.WHITE)]


def test_render_keeps_origin_piece_visible_before_drag_threshold():
    board_view = BoardView.__new__(BoardView)
    board_view.board_size = 480
    board_view.square_size = 53
    board_view.selected_square = None
    board_view.highlight_squares = set()
    board_view.arrow_move_uci = None
    board_view.settle_animation = None
    board_view.drag_state = DragState(chess.E2, chess.E2, "P", 310, 315, moved=False)
    board_view.delete = lambda *_args, **_kwargs: None
    board_view.create_rectangle = lambda *_args, **_kwargs: None
    drawn: list[tuple[float, float, str]] = []
    board_view.create_text = lambda x, y, **kwargs: drawn.append((x, y, kwargs["text"]))
    board_view.create_line = lambda *_args, **_kwargs: None
    board_view._draw_coordinates = lambda _player_color: None
    board_view._draw_arrow = lambda _player_color: None
    board = chess.Board("4k3/8/8/8/8/8/4P3/4K3 w - - 0 1")
    source_x, source_y = BoardView._square_center(board_view, chess.E2, chess.WHITE)

    BoardView.render(board_view, board, chess.WHITE)

    assert (source_x, source_y, PIECE_GLYPHS["P"]) in drawn
    assert (310, 315, PIECE_GLYPHS["P"]) not in drawn


def test_render_suppresses_origin_piece_after_drag_threshold_crossed():
    board_view = BoardView.__new__(BoardView)
    board_view.board_size = 480
    board_view.square_size = 53
    board_view.selected_square = None
    board_view.highlight_squares = set()
    board_view.arrow_move_uci = None
    board_view.settle_animation = None
    board_view.drag_state = DragState(chess.E2, chess.E2, "P", 310, 315, moved=True)
    board_view.delete = lambda *_args, **_kwargs: None
    board_view.create_rectangle = lambda *_args, **_kwargs: None
    drawn: list[tuple[float, float, str]] = []
    board_view.create_text = lambda x, y, **kwargs: drawn.append((x, y, kwargs["text"]))
    board_view.create_line = lambda *_args, **_kwargs: None
    board_view._draw_coordinates = lambda _player_color: None
    board_view._draw_arrow = lambda _player_color: None
    board = chess.Board("4k3/8/8/8/8/8/4P3/4K3 w - - 0 1")
    source_x, source_y = BoardView._square_center(board_view, chess.E2, chess.WHITE)

    BoardView.render(board_view, board, chess.WHITE)

    assert (source_x, source_y, PIECE_GLYPHS["P"]) not in drawn
    assert (310, 315, PIECE_GLYPHS["P"]) in drawn


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
    panel.pieces_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    panel.clock_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    panel.delta_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    board = chess.Board()
    board.remove_piece_at(chess.D8)
    board.remove_piece_at(chess.A1)

    CapturedMaterialPanel.update_board(panel, board, player_color=chess.BLACK, near_side=True, clock_seconds=599.8)
    assert '♖' in panel.pieces_var.value
    assert panel.clock_var.value == '10:00'
    assert panel.delta_var.value == ''


def test_captured_strip_panel_shows_plus_only_for_side_with_advantage():
    from opening_trainer.ui.captured_material_panel import CapturedMaterialPanel

    board = chess.Board()
    board.remove_piece_at(chess.D8)
    top_panel = CapturedMaterialPanel.__new__(CapturedMaterialPanel)
    top_panel.pieces_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    top_panel.clock_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    top_panel.delta_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    bottom_panel = CapturedMaterialPanel.__new__(CapturedMaterialPanel)
    bottom_panel.pieces_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    bottom_panel.clock_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()
    bottom_panel.delta_var = type('Var', (), {'set': lambda self, value: setattr(self, 'value', value)})()

    CapturedMaterialPanel.update_board(top_panel, board, player_color=chess.WHITE, near_side=False, clock_seconds=600)
    CapturedMaterialPanel.update_board(bottom_panel, board, player_color=chess.WHITE, near_side=True, clock_seconds=599)

    assert top_panel.delta_var.value == ''
    assert bottom_panel.delta_var.value == '+9'


def test_remembered_bundle_path_defaults_to_none(tmp_path):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeSession(tmp_path)
    assert gui._remembered_bundle_path() is None


def test_options_dialog_no_longer_owns_smart_contract_controls():
    source = inspect.getsource(OpeningTrainerGUI._open_options)

    assert 'Enable Smart Profile contract mode' not in source
    assert 'Smart track' not in source
    assert 'Training depth (player moves)' not in source
    assert 'Accept Good moves' not in source
    assert 'Training panel columns' in source
    assert 'for column in self.inspector.columns' in source


class _FakeComboStrip(FakeGridWidget):
    def __init__(self, values=()):
        super().__init__()
        self._values = tuple(values)

    def configure(self, **kwargs):
        super().configure(**kwargs)
        if "values" in kwargs:
            self._values = tuple(kwargs["values"])

    def cget(self, name):
        if name == "values":
            return self._values
        raise KeyError(name)


def _build_control_strip_gui():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.panel_visible = False
    gui.move_list_visible = True
    gui.catalog = type("Catalog", (), {"entries": ()})()
    gui.catalog_grouped = {"Rapid": {"600+0": {"400-600": ()}}}
    gui.session = type(
        "Session",
        (),
        {
            "settings": TrainerSettings(training_mode="smart_profile", selected_time_control_id="600+0"),
            "max_supported_training_depth": lambda self: 6,
            "smart_profile_status": lambda self: type(
                "Status",
                (),
                {
                    "active": True,
                    "level": 1,
                    "expected_rating_band": "400-600",
                    "contract_turns": 3,
                    "contract_good_accepted": True,
                },
            )(),
        },
    )()
    gui.smart_mode_var = FakeStringVar()
    gui.top_track_var = FakeStringVar()
    gui.top_level_var = FakeStringVar()
    gui.top_elo_var = FakeStringVar()
    gui.top_depth_var = FakeStringVar()
    gui.top_good_var = FakeStringVar()
    gui.top_time_control_var = FakeStringVar("600+0")
    gui.manual_elo_var = FakeStringVar()
    gui.manual_depth_var = FakeStringVar("3")
    gui.manual_good_var = FakeStringVar("Yes")
    gui.top_time_control_combo = _FakeComboStrip()
    gui.top_elo_combo = _FakeComboStrip()
    gui.top_depth_combo = _FakeComboStrip()
    gui.top_good_combo = _FakeComboStrip()
    gui.top_level_label = FakeGridWidget()
    gui.top_elo_label = FakeGridWidget()
    gui.top_depth_label = FakeGridWidget()
    gui.top_good_label = FakeGridWidget()
    gui.recent_var = FakeStringVar("")
    gui._remembered_bundle_path = lambda: None
    gui._catalog_root_setting = lambda: str(getattr(gui, "catalog_root", "catalog-root"))
    gui._refresh_supporting_surfaces = lambda: None
    return gui


def test_smart_on_shows_labels_and_hides_manual_controls():
    gui = _build_control_strip_gui()

    gui._refresh_top_control_strip()

    assert gui.top_level_label.visible is True
    assert gui.top_elo_label.visible is True
    assert gui.top_depth_label.visible is True
    assert gui.top_good_label.visible is True
    assert gui.top_elo_combo.visible is False
    assert gui.top_depth_combo.visible is False
    assert gui.top_good_combo.visible is False


def test_smart_off_shows_editable_manual_controls():
    gui = _build_control_strip_gui()
    gui.session.smart_profile_status = lambda: type(
        "Status",
        (),
        {
            "active": False,
            "level": None,
            "expected_rating_band": None,
            "contract_turns": None,
            "contract_good_accepted": None,
        },
    )()

    gui._refresh_top_control_strip()

    assert gui.top_elo_combo.visible is True
    assert gui.top_depth_combo.visible is True
    assert gui.top_good_combo.visible is True
    assert gui.top_level_label.visible is False
    assert gui.top_elo_label.visible is False


def test_top_summary_row_manual_mode_is_profile_only(tmp_path):
    gui = _build_gui(tmp_path)
    gui.session.smart_profile_status = lambda: type('Status', (), {'active': False})()
    text = OpeningTrainerGUI._build_top_summary_row(gui, profile_name='Daharen', due=1, boosted=1, extreme=0)
    assert text == 'Profile: Daharen'


def test_top_summary_row_smart_mode_shows_compact_hud(tmp_path):
    gui = _build_gui(tmp_path)
    text = OpeningTrainerGUI._build_top_summary_row(gui, profile_name='Daharen', due=0, boosted=1, extreme=0)
    assert text == 'Profile: Daharen   Level: L4   Success streak: 2   Failure streak: 1   0 due (1 boosted, 0 urgent)'


def test_main_layout_no_longer_includes_repeated_status_surfaces():
    source = inspect.getsource(OpeningTrainerGUI.__init__)
    assert 'StatusPanel' not in source
    assert "text='Corpus bundle'" not in source
    assert "text='Far side'" not in source
    assert "text='Near side'" not in source


def test_training_panel_default_visible_columns_are_core_triage_fields():
    assert DEFAULT_TRAINING_PANEL_COLUMNS == ('position', 'side', 'frequency_state', 'fails', 'success_streak')


def test_track_label_is_derived_from_selected_exact_time_control():
    gui = _build_control_strip_gui()
    gui.catalog = type("Catalog", (), {"entries": (type("Entry", (), {"time_control_id": "120+1", "target_rating_band": "400-600"})(),)})()
    gui.catalog_grouped = {"Bullet": {"120+1": {"400-600": ()}}}
    gui.top_time_control_var.set("120+1")

    gui._refresh_top_control_strip()

    assert gui.top_track_var.get() == "Bullet"


def test_top_strip_smart_time_control_change_autoloads_expected_bundle():
    gui = _build_control_strip_gui()
    gui.smart_mode_var = FakeBoolVar(True)
    gui.top_time_control_var = FakeStringVar("600+1")
    gui.catalog_grouped = {"Rapid": {"600+1": {"400-600": ()}}}
    gui.session = type(
        "Session",
        (),
        {
            "settings": TrainerSettings(training_mode="smart_profile", selected_time_control_id="600+0"),
            "update_settings": lambda self, settings: settings,
            "smart_profile": type(
                "SmartProfile",
                (),
                {
                    "resolve_expected_bundle": lambda self, _root: type(
                        "Resolution", (), {"resolved_entry": type("Entry", (), {"bundle_dir": "/tmp/smart_600_1_400_600"})(), "category_id": "600+1", "expected_rating_band": "400-600"}
                    )()
                },
            )(),
            "smart_profile_status": lambda self: type(
                "Status",
                (),
                {"active": True, "level": 1, "expected_rating_band": "400-600", "contract_turns": 3, "contract_good_accepted": True},
            )(),
            "max_supported_training_depth": lambda self: 6,
        },
    )()
    loaded: list[str] = []
    gui._load_selected_bundle = lambda path: loaded.append(path)
    gui._remembered_bundle_path = lambda: "/tmp/old_bundle"

    gui._apply_top_contract_change(reason="time control changed")

    assert loaded == ["/tmp/smart_600_1_400_600"]


def test_top_strip_manual_contract_change_autoloads_matching_bundle():
    gui = _build_control_strip_gui()
    gui.smart_mode_var = FakeBoolVar(False)
    gui.top_time_control_var = FakeStringVar("600+0")
    gui.manual_elo_var = FakeStringVar("1200-1400")
    gui.catalog = type("Catalog", (), {"entries": ()})()
    manual_entry = type("Entry", (), {"bundle_dir": "/tmp/manual_600_0_1200_1400"})()
    gui.catalog.grouped = lambda: {"Rapid": {"600+0": {"1200-1400": (manual_entry,)}}}
    gui.catalog_grouped = gui.catalog.grouped()
    gui.session = type(
        "Session",
        (),
        {
            "settings": TrainerSettings(training_mode="manual", selected_time_control_id="600+0", smart_profile_enabled=False),
            "update_settings": lambda self, settings: settings,
            "smart_profile_status": lambda self: type(
                "Status",
                (),
                {"active": False, "level": None, "expected_rating_band": None, "contract_turns": None, "contract_good_accepted": None},
            )(),
            "max_supported_training_depth": lambda self: 6,
        },
    )()
    loaded: list[str] = []
    gui._load_selected_bundle = lambda path: loaded.append(path)
    gui._remembered_bundle_path = lambda: "/tmp/old_bundle"

    gui._apply_top_contract_change(reason="manual contract changed")

    assert loaded == ["/tmp/manual_600_0_1200_1400"]


def test_top_strip_contract_change_skips_reload_when_resolved_bundle_is_active():
    gui = _build_control_strip_gui()
    gui.smart_mode_var = FakeBoolVar(True)
    gui.session = type(
        "Session",
        (),
        {
            "settings": TrainerSettings(training_mode="smart_profile", selected_time_control_id="600+0"),
            "update_settings": lambda self, settings: settings,
            "smart_profile": type(
                "SmartProfile",
                (),
                {
                    "resolve_expected_bundle": lambda self, _root: type(
                        "Resolution", (), {"resolved_entry": type("Entry", (), {"bundle_dir": "/tmp/already_active"})(), "category_id": "600+0", "expected_rating_band": "400-600"}
                    )()
                },
            )(),
            "smart_profile_status": lambda self: type(
                "Status",
                (),
                {"active": True, "level": 1, "expected_rating_band": "400-600", "contract_turns": 3, "contract_good_accepted": True},
            )(),
            "max_supported_training_depth": lambda self: 6,
        },
    )()
    calls = {"loads": 0, "refreshes": 0}
    gui._load_selected_bundle = lambda _path: calls.__setitem__("loads", calls["loads"] + 1)
    gui._refresh_supporting_surfaces = lambda: calls.__setitem__("refreshes", calls["refreshes"] + 1)
    gui._remembered_bundle_path = lambda: "/tmp/already_active"

    gui._apply_top_contract_change(reason="time control changed")

    assert calls["loads"] == 0
    assert calls["refreshes"] == 1
    assert "resolved bundle already active" in gui.recent_var.get()


def test_top_strip_contract_change_surfaces_missing_bundle_status():
    gui = _build_control_strip_gui()
    gui.smart_mode_var = FakeBoolVar(False)
    gui.top_time_control_var = FakeStringVar("600+0")
    gui.manual_elo_var = FakeStringVar("1400-1600")
    gui.catalog = type("Catalog", (), {"entries": ()})()
    gui.catalog.grouped = lambda: {"Rapid": {"600+0": {"1200-1400": (type("Entry", (), {"bundle_dir": "/tmp/x"})(),)}}}
    gui.catalog_grouped = gui.catalog.grouped()
    gui.session = type(
        "Session",
        (),
        {
            "settings": TrainerSettings(training_mode="manual", selected_time_control_id="600+0", smart_profile_enabled=False),
            "update_settings": lambda self, settings: settings,
            "smart_profile_status": lambda self: type(
                "Status",
                (),
                {"active": False, "level": None, "expected_rating_band": None, "contract_turns": None, "contract_good_accepted": None},
            )(),
            "max_supported_training_depth": lambda self: 6,
        },
    )()
    loaded: list[str] = []
    gui._load_selected_bundle = lambda path: loaded.append(path)

    gui._apply_top_contract_change(reason="manual contract changed")

    assert loaded == []
    assert "No discovered bundle matches 600+0 / 1400-1600" in gui.recent_var.get()



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


def test_on_board_press_selects_piece_with_board_local_refresh_only():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    gui.session = type(
        'Session',
        (),
        {
            'get_view': lambda self=None: view,
            'current_board': lambda self=None: chess.Board(),
            'legal_moves_from': lambda self, square: [chess.Move(chess.E2, chess.E4)] if square == chess.E2 else [],
        },
    )()
    gui.selected_square = None
    board_local_calls = {'count': 0}
    full_refresh_calls = {'count': 0}
    gui._refresh_board_local = lambda *args, **kwargs: board_local_calls.__setitem__('count', board_local_calls['count'] + 1)
    gui._refresh_view = lambda *args, **kwargs: full_refresh_calls.__setitem__('count', full_refresh_calls['count'] + 1)
    drag_calls = {'count': 0}
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'square_at_xy': lambda self, x, y, player_color: chess.E2,
            'start_drag': lambda self, square, symbol, x, y: drag_calls.__setitem__('count', drag_calls['count'] + 1),
        },
    )()

    OpeningTrainerGUI._on_board_press(gui, type('Event', (), {'x': 10, 'y': 20})())

    assert gui.selected_square == chess.E2
    assert drag_calls['count'] == 1
    assert board_local_calls['count'] == 1
    assert full_refresh_calls['count'] == 0


def test_on_board_release_reselects_friendly_piece_with_board_local_refresh_only():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    board = chess.Board()
    gui.session = type('Session', (), {'get_view': lambda self=None: view, 'current_board': lambda self=None: board})()
    gui.selected_square = chess.E2
    board_local_calls = {'count': 0}
    full_refresh_calls = {'count': 0}
    gui._refresh_board_local = lambda *args, **kwargs: board_local_calls.__setitem__('count', board_local_calls['count'] + 1)
    gui._refresh_view = lambda *args, **kwargs: full_refresh_calls.__setitem__('count', full_refresh_calls['count'] + 1)
    gui.board_view = type('BoardViewStub', (), {'release_drag': lambda self, x, y, player_color: (chess.E2, chess.G1, False)})()

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert gui.selected_square == chess.G1
    assert board_local_calls['count'] == 1
    assert full_refresh_calls['count'] == 0


def test_on_board_release_cancelled_drag_uses_board_local_refresh_only():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    gui.session = type('Session', (), {'get_view': lambda self=None: view, 'current_board': lambda self=None: chess.Board()})()
    gui.selected_square = chess.E2
    board_local_calls = {'count': 0}
    full_refresh_calls = {'count': 0}
    gui._refresh_board_local = lambda *args, **kwargs: board_local_calls.__setitem__('count', board_local_calls['count'] + 1)
    gui._refresh_view = lambda *args, **kwargs: full_refresh_calls.__setitem__('count', full_refresh_calls['count'] + 1)
    gui.board_view = type('BoardViewStub', (), {'release_drag': lambda self, x, y, player_color: (chess.E2, None, True)})()

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert gui.selected_square is None
    assert board_local_calls['count'] == 1
    assert full_refresh_calls['count'] == 0


def test_off_turn_release_queues_premove_instead_of_rejecting():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': False, 'player_color': chess.WHITE})()
    gui.session = type('Session', (), {'get_view': lambda self=None: view, 'current_board': lambda self=None: chess.Board()})()
    gui.selected_square = chess.E2
    gui.premove_queue = []
    gui._refresh_board_local = lambda *args, **kwargs: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'release_drag': lambda self, x, y, player_color: (chess.E2, chess.E4, True),
            'cancel_drag': lambda self: None,
        },
    )()

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert [intent.uci for intent in gui.premove_queue] == ['e2e4']


def test_off_turn_release_can_append_multiple_premoves_in_order():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': False, 'player_color': chess.WHITE})()
    board = chess.Board()
    gui.session = type('Session', (), {'get_view': lambda self=None: view, 'current_board': lambda self=None: board})()
    gui._refresh_board_local = lambda *args, **kwargs: None
    gui.board_view = type('BoardViewStub', (), {'cancel_drag': lambda self: None})()
    gui.premove_queue = []

    gui.selected_square = chess.E2
    gui.board_view.release_drag = lambda x, y, player_color: (chess.E2, chess.E4, True)
    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())
    gui.selected_square = chess.G1
    gui.board_view.release_drag = lambda x, y, player_color: (chess.G1, chess.F3, True)
    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert [intent.uci for intent in gui.premove_queue] == ['e2e4', 'g1f3']


def test_on_board_release_committed_move_still_uses_full_refresh():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    board = chess.Board()
    submitted = {'uci': None}
    gui.session = type(
        'Session',
        (),
        {
            'get_view': lambda self=None: view,
            'current_board': lambda self=None: board,
            'submit_user_move_uci': lambda self, uci: submitted.__setitem__('uci', uci),
        },
    )()
    gui.selected_square = chess.E2
    board_local_calls = {'count': 0}
    full_refresh_calls = {'count': 0}
    board_canvas_calls = {'count': 0}
    deferred_supporting_calls = {'count': 0}
    gui._refresh_board_local = lambda *args, **kwargs: board_local_calls.__setitem__('count', board_local_calls['count'] + 1)
    gui._refresh_view = lambda *args, **kwargs: full_refresh_calls.__setitem__('count', full_refresh_calls['count'] + 1)
    gui._refresh_board_canvas = lambda: board_canvas_calls.__setitem__('count', board_canvas_calls['count'] + 1)
    gui._schedule_board_animation_refresh = lambda: None
    gui._schedule_supporting_surface_refresh = lambda: deferred_supporting_calls.__setitem__('count', deferred_supporting_calls['count'] + 1)
    gui._log_animation_event = lambda *args, **kwargs: None
    gui._schedule_pending_opponent_commit = lambda: None
    gui._supporting_refresh_pending_after_first_tick = False
    animation_calls = {'count': 0, 'kwargs': None}
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'release_drag': lambda self, x, y, player_color: (chess.E2, chess.E4, True),
            'cancel_drag': lambda self: None,
            'start_committed_move_animation': lambda self, **kwargs: (
                animation_calls.__setitem__('count', animation_calls['count'] + 1),
                animation_calls.__setitem__('kwargs', kwargs),
            ),
            'animation_in_progress': lambda self: True,
            'force_immediate_visible_frame': lambda self: 0.2,
        },
    )()
    gui._build_move = lambda from_square, to_square, current_board: chess.Move(from_square, to_square)

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert submitted['uci'] == 'e2e4'
    assert animation_calls['count'] == 1
    assert full_refresh_calls['count'] == 0
    assert board_canvas_calls['count'] == 1
    assert deferred_supporting_calls['count'] == 0
    assert gui._supporting_refresh_pending_after_first_tick is True
    assert board_local_calls['count'] == 0
    assert animation_calls['kwargs'] is not None
    assert animation_calls['kwargs']['start_x'] == 30.0
    assert animation_calls['kwargs']['start_y'] == 40.0
    assert animation_calls['kwargs']['duration_ms'] == PLAYER_COMMITTED_MOVE_DURATION_MS


def test_premove_queue_renders_through_board_view_state():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.selected_square = None
    gui.premove_queue = [
        type('Intent', (), {'uci': 'e2e4'})(),
        type('Intent', (), {'uci': 'g1f3'})(),
    ]
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.PLAYER_TURN, 0, 1, None, None, None)
    calls = {'queue': None}
    gui.session = type('Session', (), {'get_view': lambda self=None: view, 'legal_moves_from': lambda self, sq: []})()
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'set_selection': lambda self, selected, targets: None,
            'set_premove_queue': lambda self, queue: calls.__setitem__('queue', queue),
            'render': lambda self, board, player_color: None,
        },
    )()

    OpeningTrainerGUI._refresh_board_canvas(gui)

    assert calls['queue'] == ['e2e4', 'g1f3']


def test_on_board_release_logs_player_animation_start(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    board = chess.Board()
    gui.session = type(
        'Session',
        (),
        {
            'get_view': lambda self=None: view,
            'current_board': lambda self=None: board,
            'submit_user_move_uci': lambda self, _uci: None,
        },
    )()
    gui.selected_square = chess.E2
    gui._refresh_board_local = lambda *args, **kwargs: None
    gui._refresh_view = lambda *args, **kwargs: None
    gui._refresh_board_canvas = lambda *args, **kwargs: None
    gui._schedule_supporting_surface_refresh = lambda: None
    gui._schedule_board_animation_refresh = lambda: None
    gui._schedule_pending_opponent_commit = lambda: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'release_drag': lambda self, x, y, player_color: (chess.E2, chess.E4, True),
            'cancel_drag': lambda self: None,
            'start_committed_move_animation': lambda self, **kwargs: None,
            'force_immediate_visible_frame': lambda self: 0.23,
            'settle_animation': None,
        },
    )()
    gui._build_move = lambda from_square, to_square, current_board: chess.Move(from_square, to_square)
    lines: list[str] = []
    monkeypatch.setattr('opening_trainer.ui.gui_app.log_line', lambda message, tag='timing': lines.append(message))

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert any(line.startswith('GUI_ANIM_PLAYER_START') for line in lines)
    assert any('start_mode=release_xy' in line for line in lines if line.startswith('GUI_ANIM_PLAYER_START'))
    assert any(line.startswith('GUI_ANIM_PLAYER_POST_START_REPAINT') for line in lines)
    assert any('supporting_refresh=deferred' in line for line in lines if line.startswith('GUI_ANIM_PLAYER_POST_START_REPAINT'))
    assert any('immediate_frame=yes' in line for line in lines if line.startswith('GUI_ANIM_PLAYER_POST_START_REPAINT'))


def test_on_board_release_terminal_restart_pending_defers_modal_when_animation_active():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    start_view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    outcome = SessionOutcome(True, 'Solved.', 'e4', None, 'success', 'ordinary_corpus_play', 'next_line', 'Default', None)
    terminal_view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    board = chess.Board()
    submitted = {'uci': None}
    scheduled = {'count': 0}
    shows = {'count': 0}

    class Session:
        def __init__(self):
            self._calls = 0

        def get_view(self):
            self._calls += 1
            return start_view if self._calls == 1 else terminal_view

        def current_board(self):
            return board

        def submit_user_move_uci(self, uci):
            submitted['uci'] = uci

    gui.session = Session()
    gui.selected_square = chess.E2
    gui.pending_restart = False
    gui._deferred_outcome_view = None
    gui._refresh_board_local = lambda *args, **kwargs: None
    gui._refresh_post_animation_start = lambda **kwargs: None
    gui._schedule_pending_opponent_commit = lambda: scheduled.__setitem__('count', scheduled['count'] + 1)
    gui._show_outcome_modal = lambda _view: shows.__setitem__('count', shows['count'] + 1)
    gui._schedule_board_animation_refresh = lambda: None
    gui._log_animation_event = lambda *args, **kwargs: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'release_drag': lambda self, x, y, player_color: (chess.E2, chess.E4, True),
            'cancel_drag': lambda self: None,
            'start_committed_move_animation': lambda self, **kwargs: None,
            'animation_in_progress': lambda self: True,
            'settle_animation': None,
        },
    )()
    gui._build_move = lambda from_square, to_square, current_board: chess.Move(from_square, to_square)

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert submitted['uci'] == 'e2e4'
    assert gui.pending_restart is True
    assert gui._deferred_outcome_view is terminal_view
    assert shows['count'] == 0
    assert scheduled['count'] == 0


def test_on_board_release_terminal_restart_pending_shows_modal_immediately_without_animation():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    start_view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    outcome = SessionOutcome(True, 'Solved.', 'e4', None, 'success', 'ordinary_corpus_play', 'next_line', 'Default', None)
    terminal_view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    board = chess.Board()
    scheduled = {'count': 0}
    shows = {'count': 0}

    class Session:
        def __init__(self):
            self._calls = 0

        def get_view(self):
            self._calls += 1
            return start_view if self._calls == 1 else terminal_view

        def current_board(self):
            return board

        def submit_user_move_uci(self, _uci):
            return None

    gui.session = Session()
    gui.selected_square = chess.E2
    gui.pending_restart = False
    gui._deferred_outcome_view = None
    gui._refresh_board_local = lambda *args, **kwargs: None
    gui._refresh_post_animation_start = lambda **kwargs: None
    gui._schedule_pending_opponent_commit = lambda: scheduled.__setitem__('count', scheduled['count'] + 1)
    gui._show_outcome_modal = lambda _view: shows.__setitem__('count', shows['count'] + 1)
    gui._schedule_board_animation_refresh = lambda: None
    gui._log_animation_event = lambda *args, **kwargs: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'release_drag': lambda self, x, y, player_color: (chess.E2, chess.E4, True),
            'cancel_drag': lambda self: None,
            'start_committed_move_animation': lambda self, **kwargs: None,
            'animation_in_progress': lambda self: False,
            'settle_animation': None,
        },
    )()
    gui._build_move = lambda from_square, to_square, current_board: chess.Move(from_square, to_square)

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert gui.pending_restart is True
    assert gui._deferred_outcome_view is None
    assert shows['count'] == 1
    assert scheduled['count'] == 0


def test_on_board_release_non_terminal_state_still_schedules_pending_opponent_commit():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    start_view = type('View', (), {'awaiting_user_input': True, 'player_color': chess.WHITE})()
    post_submit_view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.OPPONENT_TURN, 1, 1, None, None, None)
    board = chess.Board()
    scheduled = {'count': 0}

    class Session:
        def __init__(self):
            self._calls = 0

        def get_view(self):
            self._calls += 1
            return start_view if self._calls == 1 else post_submit_view

        def current_board(self):
            return board

        def submit_user_move_uci(self, _uci):
            return None

    gui.session = Session()
    gui.selected_square = chess.E2
    gui.pending_restart = False
    gui._deferred_outcome_view = None
    gui._refresh_board_local = lambda *args, **kwargs: None
    gui._refresh_post_animation_start = lambda **kwargs: None
    gui._schedule_pending_opponent_commit = lambda: scheduled.__setitem__('count', scheduled['count'] + 1)
    gui._show_outcome_modal = lambda _view: None
    gui._schedule_board_animation_refresh = lambda: None
    gui._log_animation_event = lambda *args, **kwargs: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'release_drag': lambda self, x, y, player_color: (chess.E2, chess.E4, True),
            'cancel_drag': lambda self: None,
            'start_committed_move_animation': lambda self, **kwargs: None,
            'animation_in_progress': lambda self: True,
            'settle_animation': None,
        },
    )()
    gui._build_move = lambda from_square, to_square, current_board: chess.Move(from_square, to_square)

    OpeningTrainerGUI._on_board_release(gui, type('Event', (), {'x': 30, 'y': 40})())

    assert gui.pending_restart is False
    assert gui._deferred_outcome_view is None
    assert scheduled['count'] == 1


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


def test_commit_scheduled_opponent_action_uses_deferred_supporting_refresh_after_board_repaint():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._pending_opponent_after_handle = 'h7'
    gui._is_shutting_down = False
    gui._deferred_outcome_view = None
    refresh_calls = {'count': 0}
    gui._refresh_view = lambda: refresh_calls.__setitem__('count', refresh_calls['count'] + 1)
    board_canvas_calls = {'count': 0}
    gui._refresh_board_canvas = lambda: board_canvas_calls.__setitem__('count', board_canvas_calls['count'] + 1)
    deferred_supporting_calls = {'count': 0}
    gui._schedule_supporting_surface_refresh = lambda: deferred_supporting_calls.__setitem__('count', deferred_supporting_calls['count'] + 1)
    animation_schedule_calls = {'count': 0}
    gui._schedule_board_animation_refresh = lambda: animation_schedule_calls.__setitem__('count', animation_schedule_calls['count'] + 1)
    gui._log_animation_event = lambda *args, **kwargs: None
    gui._schedule_pending_opponent_commit = lambda: None
    gui._supporting_refresh_pending_after_first_tick = False

    class Session:
        state = SessionState.PLAYER_TURN
        pending_opponent_action = type(
            'Pending',
            (),
            {
                'choice': type('Choice', (), {'move': chess.Move.from_uci('e7e5')})(),
                'board_before': chess.Board(),
            },
        )()

        def commit_pending_opponent_action(self):
            self.pending_opponent_action = None

        def get_view(self):
            return type('View', (), {'player_color': chess.WHITE})()

    gui.session = Session()
    animation_calls = {'count': 0, 'kwargs': None}
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'start_committed_move_animation': lambda self, **kwargs: (
                animation_calls.__setitem__('count', animation_calls['count'] + 1),
                animation_calls.__setitem__('kwargs', kwargs),
            ),
            'animation_in_progress': lambda self: True,
            'force_immediate_visible_frame': lambda self: 0.22,
        },
    )()

    OpeningTrainerGUI._commit_scheduled_opponent_action(gui)

    assert gui._pending_opponent_after_handle is None
    assert refresh_calls['count'] == 0
    assert board_canvas_calls['count'] == 1
    assert deferred_supporting_calls['count'] == 0
    assert gui._supporting_refresh_pending_after_first_tick is True
    assert animation_calls['count'] == 1
    assert animation_calls['kwargs'] is not None
    assert animation_calls['kwargs'].get('start_x') is None
    assert animation_calls['kwargs'].get('start_y') is None
    assert animation_calls['kwargs'].get('duration_ms') == OPPONENT_COMMITTED_MOVE_DURATION_MS
    assert animation_schedule_calls['count'] == 1


def test_commit_scheduled_opponent_action_logs_animation_metadata(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._pending_opponent_after_handle = None
    gui._is_shutting_down = False
    gui._deferred_outcome_view = None
    gui._refresh_view = lambda: None
    gui._refresh_board_canvas = lambda: None
    gui._schedule_supporting_surface_refresh = lambda: None
    gui._schedule_board_animation_refresh = lambda: None
    gui._schedule_pending_opponent_commit = lambda: None
    lines: list[str] = []
    monkeypatch.setattr('opening_trainer.ui.gui_app.log_line', lambda message, tag='timing': lines.append(message))

    class Session:
        state = SessionState.PLAYER_TURN
        pending_opponent_action = type(
            'Pending',
            (),
            {
                'choice': type('Choice', (), {'move': chess.Move.from_uci('e7e5')})(),
                'board_before': chess.Board(),
            },
        )()

        def commit_pending_opponent_action(self):
            self.pending_opponent_action = None

        def get_view(self):
            return type('View', (), {'player_color': chess.WHITE})()

    gui.session = Session()
    gui.board_view = type('BoardViewStub', (), {'start_committed_move_animation': lambda self, **kwargs: None})()

    OpeningTrainerGUI._commit_scheduled_opponent_action(gui)

    assert any(line.startswith('GUI_ANIM_OPPONENT_PENDING_METADATA') for line in lines)
    assert any(line.startswith('GUI_ANIM_OPPONENT_START') for line in lines)
    assert any(line.startswith('GUI_ANIM_OPPONENT_POST_START_REPAINT') for line in lines)
    assert any('supporting_refresh=deferred' in line for line in lines if line.startswith('GUI_ANIM_OPPONENT_POST_START_REPAINT'))


def test_refresh_view_defers_outcome_modal_until_animation_finishes():
    outcome = SessionOutcome(False, 'Rejected by engine.', 'd4', None, 'fail', 'ordinary_corpus_play', 'immediate_retry', 'Default', 'Created new review item.')
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.session = FakeViewSession(view)
    gui.pending_restart = False
    gui._deferred_outcome_view = None
    gui._refresh_board_canvas = lambda: None
    gui._refresh_supporting_surfaces = lambda: None
    gui._schedule_board_animation_refresh = lambda: None
    calls = {'count': 0}
    gui._show_outcome_modal = lambda current_view: calls.__setitem__('count', calls['count'] + 1)
    gui.board_view = type('BoardViewStub', (), {'animation_in_progress': lambda self: True})()

    OpeningTrainerGUI._refresh_view(gui)

    assert calls['count'] == 0
    assert gui.pending_restart is True
    assert gui._deferred_outcome_view is view


def test_show_deferred_outcome_modal_if_ready_runs_after_animation_settles():
    outcome = SessionOutcome(False, 'Rejected by engine.', 'd4', None, 'fail', 'ordinary_corpus_play', 'immediate_retry', 'Default', 'Created new review item.')
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._is_shutting_down = False
    gui._deferred_outcome_view = view
    calls = {'count': 0}
    gui._show_outcome_modal = lambda current_view: calls.__setitem__('count', calls['count'] + 1)
    gui.board_view = type('BoardViewStub', (), {'animation_in_progress': lambda self: False})()

    OpeningTrainerGUI._show_deferred_outcome_modal_if_ready(gui)

    assert calls['count'] == 1
    assert gui._deferred_outcome_view is None


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


def test_clear_board_transients_cancels_animation_and_clears_board_view():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = {'h9'}
    gui._board_animation_after_handle = 'h9'
    cleared = {'count': 0}
    gui.board_view = type('BoardViewStub', (), {'clear_transient_state': lambda self=None: cleared.__setitem__('count', cleared['count'] + 1)})()

    OpeningTrainerGUI._clear_board_transients(gui, reason='test_reset')

    assert gui.root.cancelled == ['h9']
    assert gui._board_animation_after_handle is None
    assert cleared['count'] == 1


def test_clear_board_transients_logs_reason(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._board_animation_after_handle = None
    gui.board_view = type('BoardViewStub', (), {'clear_transient_state': lambda self=None: None})()
    lines: list[str] = []
    monkeypatch.setattr('opening_trainer.ui.gui_app.log_line', lambda message, tag='timing': lines.append(message))

    OpeningTrainerGUI._clear_board_transients(gui, reason='restart')

    assert any(line.startswith('GUI_ANIM_CLEAR_TRANSIENTS') and 'reason=restart' in line for line in lines)


def test_clear_board_transients_clears_premove_queue():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._board_animation_after_handle = None
    gui.premove_queue = [type('Intent', (), {'uci': 'e2e4'})()]
    gui.board_view = type('BoardViewStub', (), {'clear_transient_state': lambda self=None: None})()

    OpeningTrainerGUI._clear_board_transients(gui, reason='new_game_start')

    assert gui.premove_queue == []


def test_attempt_execute_next_premove_executes_when_legal():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    submitted = {'uci': None}
    scheduled = {'count': 0}
    refreshed = {'count': 0}
    class Session:
        state = SessionState.PLAYER_TURN

        def current_board(self):
            return chess.Board()

        def submit_user_move_uci(self, uci):
            submitted['uci'] = uci
            self.state = SessionState.OPPONENT_TURN

        def get_view(self):
            return SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.OPPONENT_TURN, 1, 1, None, None, None)

    gui.session = Session()
    gui.premove_queue = [type('Intent', (), {'from_square': chess.E2, 'to_square': chess.E4, 'promotion': None, 'uci': 'e2e4'})()]
    gui.board_view = type('BoardViewStub', (), {'start_committed_move_animation': lambda self, **kwargs: None})()
    gui._refresh_post_animation_start = lambda **kwargs: None
    gui._handle_player_terminal_outcome = lambda _view: False
    gui._schedule_pending_opponent_commit = lambda: scheduled.__setitem__('count', scheduled['count'] + 1)
    gui._refresh_view = lambda: refreshed.__setitem__('count', refreshed['count'] + 1)

    executed = OpeningTrainerGUI._attempt_execute_next_premove(gui)

    assert executed is True
    assert submitted['uci'] == 'e2e4'
    assert gui.premove_queue == []
    assert scheduled['count'] == 1
    assert refreshed['count'] == 0


def test_attempt_execute_next_premove_invalidates_entire_queue_when_next_is_illegal():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    board = chess.Board()
    board.push(chess.Move.from_uci('e2e4'))
    gui.session = type('Session', (), {'state': SessionState.PLAYER_TURN, 'current_board': lambda self=None: board})()
    gui.premove_queue = [
        type('Intent', (), {'from_square': chess.E2, 'to_square': chess.E4, 'promotion': None, 'uci': 'e2e4'})(),
        type('Intent', (), {'from_square': chess.G1, 'to_square': chess.F3, 'promotion': None, 'uci': 'g1f3'})(),
    ]
    refreshed = {'count': 0}
    gui._refresh_view = lambda: refreshed.__setitem__('count', refreshed['count'] + 1)

    executed = OpeningTrainerGUI._attempt_execute_next_premove(gui)

    assert executed is False
    assert gui.premove_queue == []
    assert refreshed['count'] == 1


def test_schedule_board_animation_refresh_requeues_until_animation_finishes():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._board_animation_after_handle = None
    refreshes = {'count': 0}
    gui._refresh_board_canvas = lambda: refreshes.__setitem__('count', refreshes['count'] + 1)
    gui._show_deferred_outcome_modal_if_ready = lambda: None
    states = iter([True, False])
    finalized = {'count': 0}
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'animation_in_progress': lambda self=None: next(states),
            'animation_complete': lambda self=None: True,
            'finalize_animation': lambda self=None: finalized.__setitem__('count', finalized['count'] + 1) or True,
        },
    )()

    OpeningTrainerGUI._schedule_board_animation_refresh(gui)

    assert len(gui.root.after_calls) == 1
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()
    assert refreshes['count'] == 1
    assert len(gui.root.after_calls) == 1
    _delay2, callback2, _handle2 = gui.root.after_calls.pop(0)
    callback2()
    assert refreshes['count'] == 2
    assert finalized['count'] == 1


def test_schedule_board_animation_refresh_logs_first_tick_elapsed(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._board_animation_after_handle = None
    gui._refresh_board_canvas = lambda: None
    gui._show_deferred_outcome_modal_if_ready = lambda: None
    gui._deferred_outcome_view = None
    start_time = time.monotonic() - 0.03
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'animation_in_progress': lambda self=None: False,
            'animation_complete': lambda self=None: True,
            'finalize_animation': lambda self=None: True,
            'sample_animation_position': lambda self=None: (12.5, 19.5),
            'settle_animation': type('Anim', (), {'start_time': start_time})(),
        },
    )()
    lines: list[str] = []
    monkeypatch.setattr('opening_trainer.ui.gui_app.log_line', lambda message, tag='timing': lines.append(message))

    OpeningTrainerGUI._schedule_board_animation_refresh(gui)
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()

    tick_line = next(line for line in lines if line.startswith('GUI_ANIM_TICK'))
    assert 'first_tick_elapsed_ms=' in tick_line


def test_refresh_post_animation_start_forces_immediate_visible_frame(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._refresh_board_canvas = lambda: None
    gui._schedule_board_animation_refresh = lambda: None
    supporting = {'count': 0}
    gui._schedule_supporting_surface_refresh = lambda: supporting.__setitem__('count', supporting['count'] + 1)
    gui._supporting_refresh_pending_after_first_tick = False
    start = time.monotonic()
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'force_immediate_visible_frame': lambda self=None: 0.27,
            'animation_in_progress': lambda self=None: True,
            'settle_animation': type('Anim', (), {'start_time': start})(),
        },
    )()
    lines: list[str] = []
    monkeypatch.setattr('opening_trainer.ui.gui_app.log_line', lambda message, tag='timing': lines.append(message))

    OpeningTrainerGUI._refresh_post_animation_start(gui, actor='PLAYER')

    repaint_line = next(line for line in lines if line.startswith('GUI_ANIM_PLAYER_POST_START_REPAINT'))
    assert 'immediate_frame=yes' in repaint_line
    assert 'initial_progress=0.270' in repaint_line
    assert 'supporting_refresh=deferred_until_finalize' in repaint_line
    assert supporting['count'] == 0
    assert gui._supporting_refresh_pending_after_first_tick is True


def test_refresh_post_animation_start_schedules_supporting_refresh_when_animation_inactive():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._refresh_board_canvas = lambda: None
    gui._schedule_board_animation_refresh = lambda: None
    supporting = {'count': 0}
    gui._schedule_supporting_surface_refresh = lambda: supporting.__setitem__('count', supporting['count'] + 1)
    gui._supporting_refresh_pending_after_first_tick = False
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'force_immediate_visible_frame': lambda self=None: None,
            'animation_in_progress': lambda self=None: False,
            'settle_animation': None,
        },
    )()

    OpeningTrainerGUI._refresh_post_animation_start(gui, actor='PLAYER')

    assert supporting['count'] == 1
    assert gui._supporting_refresh_pending_after_first_tick is False


def test_schedule_supporting_surface_refresh_coalesces_callbacks():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._supporting_surfaces_after_handle = None
    refresh_calls = {'count': 0}
    gui._refresh_supporting_surfaces = lambda: refresh_calls.__setitem__('count', refresh_calls['count'] + 1)

    OpeningTrainerGUI._schedule_supporting_surface_refresh(gui)
    OpeningTrainerGUI._schedule_supporting_surface_refresh(gui)

    assert len(gui.root.after_calls) == 1
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()
    assert gui._supporting_surfaces_after_handle is None
    assert refresh_calls['count'] == 1


def test_schedule_board_animation_refresh_finalizes_then_shows_deferred_modal():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._board_animation_after_handle = None
    events: list[str] = []
    gui._refresh_board_canvas = lambda: events.append('refresh')
    gui._deferred_outcome_view = object()
    gui._show_deferred_outcome_modal_if_ready = lambda: events.append('modal')
    gui._supporting_refresh_pending_after_first_tick = False
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'animation_in_progress': lambda self=None: False,
            'animation_complete': lambda self=None: True,
            'finalize_animation': lambda self=None: events.append('finalize') or True,
        },
    )()

    OpeningTrainerGUI._schedule_board_animation_refresh(gui)
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()

    assert events == ['finalize', 'refresh', 'modal']


def test_schedule_board_animation_refresh_defers_supporting_refresh_until_finalize():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._board_animation_after_handle = None
    gui._deferred_outcome_view = None
    gui._supporting_refresh_pending_after_first_tick = True
    refreshes = {'count': 0}
    supporting = {'count': 0}
    gui._refresh_board_canvas = lambda: refreshes.__setitem__('count', refreshes['count'] + 1)
    gui._schedule_supporting_surface_refresh = lambda: supporting.__setitem__('count', supporting['count'] + 1)
    gui._show_deferred_outcome_modal_if_ready = lambda: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'animation_in_progress': lambda self=None: True,
            'sample_animation_position': lambda self=None: None,
            'settle_animation': None,
        },
    )()

    OpeningTrainerGUI._schedule_board_animation_refresh(gui)
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()

    assert supporting['count'] == 0
    assert gui._supporting_refresh_pending_after_first_tick is True
    assert refreshes['count'] == 1


def test_schedule_board_animation_refresh_releases_supporting_refresh_on_finalize():
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.root = FakeRoot()
    gui._after_handles = set()
    gui._is_shutting_down = False
    gui._board_animation_after_handle = None
    gui._deferred_outcome_view = None
    gui._supporting_refresh_pending_after_first_tick = True
    supporting = {'count': 0}
    gui._refresh_board_canvas = lambda: None
    gui._schedule_supporting_surface_refresh = lambda: supporting.__setitem__('count', supporting['count'] + 1)
    gui._show_deferred_outcome_modal_if_ready = lambda: None
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'animation_in_progress': lambda self=None: False,
            'animation_complete': lambda self=None: True,
            'finalize_animation': lambda self=None: True,
            'sample_animation_position': lambda self=None: None,
            'settle_animation': None,
        },
    )()

    OpeningTrainerGUI._schedule_board_animation_refresh(gui)
    _delay, callback, _handle = gui.root.after_calls.pop(0)
    callback()

    assert supporting['count'] == 1
    assert gui._supporting_refresh_pending_after_first_tick is False


def test_show_deferred_outcome_modal_if_ready_clears_deferred_after_animation_finalize():
    outcome = SessionOutcome(False, 'Rejected by engine.', 'd4', None, 'fail', 'ordinary_corpus_play', 'immediate_retry', 'Default', 'Created new review item.')
    view = SessionView(chess.STARTING_FEN, chess.WHITE, SessionState.RESTART_PENDING, 1, 1, None, outcome, None)
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._is_shutting_down = False
    gui._deferred_outcome_view = view
    calls = {'count': 0}
    gui._show_outcome_modal = lambda current_view: calls.__setitem__('count', calls['count'] + 1)
    gui.board_view = type(
        'BoardViewStub',
        (),
        {
            'animation_complete': lambda self: True,
            'finalize_animation': lambda self: True,
            'animation_in_progress': lambda self: False,
        },
    )()

    OpeningTrainerGUI._show_deferred_outcome_modal_if_ready(gui)

    assert calls['count'] == 1
    assert gui._deferred_outcome_view is None


def test_training_depth_summary_reports_updated_bundle_cap(tmp_path):
    gui = _build_gui(tmp_path)
    gui.session._max_depth = 15
    gui.session.bundle_retained_ply_depth = lambda: 30

    summary = OpeningTrainerGUI._training_depth_summary(gui)

    assert 'App max: 15 player moves' in summary
    assert 'Bundle max: 15 player moves' in summary
