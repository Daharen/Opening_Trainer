from __future__ import annotations

import os
import queue
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog
from tkinter import ttk

import chess

from ..models import SessionState
from ..runtime import RuntimeContext, RuntimeOverrides, inspect_corpus_bundle, load_runtime_config
from ..settings import TrainerSettings
from ..session import TrainingSession
from ..session_contracts import OutcomeBoardContract, OutcomeModalContract
from ..session_logging import get_session_logger, log_line
from ..single_instance import (
    acquire_single_instance_guard,
    read_instance_diagnostics,
    release_single_instance_guard,
    remove_instance_diagnostics,
    write_instance_diagnostics,
)
from .board_view import BoardView
from .captured_material_panel import CapturedMaterialPanel
from .dev_console import DevConsoleWindow
from .move_list_panel import MoveListPanel
from .outcome_modal import OutcomeModal
from .profile_dialog import ProfileDialog
from .review_inspector import ReviewInspector
from .status_panel import StatusPanel
from .timing_override_dialog import TimingOverrideDialog

PROMOTION_CHOICES = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}
DEFAULT_BUNDLE_SEARCH_ROOTS = (Path('artifacts'),)


class OpeningTrainerGUI:
    def __init__(self, session: TrainingSession | None = None, runtime_context: RuntimeContext | None = None):
        self.session = session or TrainingSession(runtime_context=runtime_context, mode='gui')
        self.root = tk.Tk()
        self.root.title('Opening Trainer')
        self.selected_square = None
        self.pending_restart = False
        self.panel_visible = self._load_panel_visibility_preference()
        self.move_list_visible = self._load_move_list_visibility_preference()
        self.loading_var = tk.StringVar(value='')
        self.bundle_status_var = tk.StringVar(value='No active corpus bundle selected.')
        self.bundle_detail_var = tk.StringVar(value='Select a corpus bundle to enable corpus-backed opponent play.')
        self.start_button = None
        self.browse_bundle_button = None
        self.bundle_combobox = None
        self.bundle_path_var = tk.StringVar()
        self.available_bundles: list[tuple[str, Path]] = []
        self._loading_queue: queue.Queue | None = None
        self._loading_thread = None
        self._loading_job_active = False
        self.session_logger = get_session_logger()
        self.dev_console = DevConsoleWindow(self.root, self.session_logger)
        self.timing_override_dialog = TimingOverrideDialog(self.root, self.session)
        self._shutdown_started = False
        self._is_shutting_down = False
        self._after_handles: set[str] = set()
        self._pending_opponent_after_handle = None
        self._child_windows: list[tk.Toplevel] = []

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        self._build_menubar()

        toolbar = tk.Frame(self.root)
        toolbar.grid(row=0, column=0, sticky='ew', padx=12, pady=(12, 4))
        self.start_button = tk.Button(toolbar, text='Start drill', command=self._start_game)
        self.start_button.pack(side='left')
        tk.Button(toolbar, text='Profiles', command=self._open_profiles).pack(side='left', padx=6)
        tk.Button(toolbar, text='Options', command=self._open_options).pack(side='left', padx=6)
        tk.Button(toolbar, text='Corpus bundle', command=self._open_bundle_picker).pack(side='left', padx=6)
        self.panel_toggle_button = tk.Button(toolbar, text='', command=self._toggle_side_panel)
        self.panel_toggle_button.pack(side='left', padx=(6, 0))

        self.root_pane = tk.PanedWindow(self.root, orient=tk.HORIZONTAL, sashrelief=tk.RAISED, bd=0)
        self.root_pane.grid(row=1, column=0, sticky='nsew', padx=12, pady=(6, 12))

        self.main_region = tk.Frame(self.root)
        self.main_region.columnconfigure(0, weight=1, minsize=420)
        self.main_region.rowconfigure(2, weight=1)
        self.compact_status_panel = StatusPanel(self.main_region, compact=True)
        self.compact_status_panel.grid(row=0, column=0, sticky='ew', pady=(0, 8))
        self.top_captured_panel = CapturedMaterialPanel(self.main_region, title='Far side')
        self.top_captured_panel.grid(row=1, column=0, sticky='ew', pady=(0, 8))
        self.board_view = BoardView(self.main_region, board_size=560, min_board_size=360)
        self.board_view.grid(row=2, column=0, sticky='nsew')
        self.bottom_captured_panel = CapturedMaterialPanel(self.main_region, title='Near side')
        self.bottom_captured_panel.grid(row=3, column=0, sticky='ew', pady=(8, 0))

        self.side_panel = tk.Frame(self.root, width=360)
        self.side_panel.columnconfigure(0, weight=1)
        self.side_panel.rowconfigure(3, weight=1)
        self.status_panel = StatusPanel(self.side_panel)
        self.status_panel.grid(row=0, column=0, sticky='ew')
        bundle_frame = ttk.LabelFrame(self.side_panel, text='Corpus bundle')
        bundle_frame.grid(row=1, column=0, sticky='ew', pady=(8, 4))
        ttk.Label(bundle_frame, textvariable=self.bundle_status_var, anchor='w', justify='left').pack(fill='x', padx=8, pady=(8, 2))
        ttk.Label(bundle_frame, textvariable=self.bundle_detail_var, anchor='w', justify='left', wraplength=320).pack(fill='x', padx=8, pady=(0, 8))
        self.recent_var = tk.StringVar()
        tk.Label(self.side_panel, textvariable=self.recent_var, justify='left', anchor='w').grid(row=2, column=0, sticky='ew', pady=4)
        self.side_content = ttk.Frame(self.side_panel)
        self.side_content.grid(row=3, column=0, sticky='nsew')
        self.side_content.columnconfigure(0, weight=1)
        self.side_content.rowconfigure(0, weight=1)
        self.side_content.rowconfigure(1, weight=1)
        self.move_list_panel = MoveListPanel(self.side_content)
        self.move_list_panel.grid(row=0, column=0, sticky='nsew', pady=(0, 8))
        self.inspector = ReviewInspector(self.side_content, self.session, self._refresh_supporting_surfaces)
        self.inspector.grid(row=1, column=0, sticky='nsew')

        self.loading_frame = ttk.Frame(self.main_region, padding=18)
        self.loading_frame.place(relx=0.5, rely=0.5, anchor='center')
        ttk.Label(self.loading_frame, text='Opening Trainer', font=('TkDefaultFont', 13, 'bold')).pack(pady=(0, 8))
        ttk.Label(self.loading_frame, textvariable=self.loading_var, justify='center', wraplength=320).pack(pady=(0, 8))
        self.loading_progress = ttk.Progressbar(self.loading_frame, mode='indeterminate', length=260)
        self.loading_progress.pack(fill='x')

        self.bundle_picker = ttk.LabelFrame(self.main_region, text='Select corpus bundle', padding=12)
        self.bundle_picker.columnconfigure(0, weight=1)
        ttk.Label(self.bundle_picker, text='Choose a discovered bundle or browse to a custom bundle path.', wraplength=360, justify='left').grid(row=0, column=0, columnspan=3, sticky='w')
        self.bundle_combobox = ttk.Combobox(self.bundle_picker, state='readonly')
        self.bundle_combobox.grid(row=1, column=0, columnspan=2, sticky='ew', pady=(10, 8))
        ttk.Button(self.bundle_picker, text='Refresh', command=self._populate_bundle_options).grid(row=1, column=2, sticky='e', padx=(8, 0), pady=(10, 8))
        ttk.Label(self.bundle_picker, text='Custom path').grid(row=2, column=0, sticky='w')
        ttk.Entry(self.bundle_picker, textvariable=self.bundle_path_var).grid(row=3, column=0, columnspan=2, sticky='ew', pady=(4, 0))
        self.browse_bundle_button = ttk.Button(self.bundle_picker, text='Browse…', command=self._browse_bundle_path)
        self.browse_bundle_button.grid(row=3, column=2, sticky='e', padx=(8, 0))
        actions = ttk.Frame(self.bundle_picker)
        actions.grid(row=4, column=0, columnspan=3, sticky='ew', pady=(12, 0))
        ttk.Button(actions, text='Use selection', command=self._apply_bundle_selection).pack(side='left')
        ttk.Button(actions, text='Skip bundle', command=self._skip_bundle_selection).pack(side='left', padx=(8, 0))

        self.root_pane.add(self.main_region, minsize=500, stretch='always')
        self.root_pane.add(self.side_panel, minsize=280)

        self.board_view.bind('<ButtonPress-1>', self._on_board_press)
        self.board_view.bind('<B1-Motion>', self._on_board_drag)
        self.board_view.bind('<ButtonRelease-1>', self._on_board_release)
        self._apply_shell_layout(initializing=True)
        self._populate_bundle_options()
        self._update_bundle_summary()
        self.root.protocol('WM_DELETE_WINDOW', self._request_shutdown)

    def run(self) -> None:
        self._schedule_after(0, self._initialize_app_shell)
        self.root.mainloop()

    def _initialize_app_shell(self) -> None:
        remembered = self._remembered_bundle_path()
        if remembered and self._bundle_path_is_valid(remembered):
            self._load_selected_bundle(remembered)
            return
        self._show_bundle_picker('Choose a corpus bundle to start training. You can also skip and use fallback mode.')

    def _load_panel_visibility_preference(self) -> bool:
        return self.session.settings_store.load(maximum_depth=self.session.max_supported_training_depth()).side_panel_visible

    def _load_move_list_visibility_preference(self) -> bool:
        return self.session.settings_store.load(maximum_depth=self.session.max_supported_training_depth()).move_list_visible

    def _remembered_bundle_path(self) -> str | None:
        settings = self.session.settings_store.load(maximum_depth=self.session.max_supported_training_depth())
        value = settings.last_bundle_path
        return value if value else None

    def _save_shell_preferences(self) -> None:
        settings = self.session.settings
        self.session.update_settings(
            TrainerSettings(
                good_moves_acceptable=settings.good_moves_acceptable,
                active_training_ply_depth=settings.active_training_ply_depth,
                side_panel_visible=self.panel_visible,
                move_list_visible=self.move_list_visible,
                last_bundle_path=self._remembered_bundle_path(),
            )
        )

    def _set_last_bundle_path(self, bundle_path: str | None) -> None:
        settings = self.session.settings
        self.session.update_settings(
            TrainerSettings(
                good_moves_acceptable=settings.good_moves_acceptable,
                active_training_ply_depth=settings.active_training_ply_depth,
                side_panel_visible=self.panel_visible,
                move_list_visible=self.move_list_visible,
                last_bundle_path=bundle_path,
            )
        )

    def _toggle_side_panel(self) -> None:
        self.panel_visible = not self.panel_visible
        self._apply_shell_layout()
        self._save_shell_preferences()

    def _apply_shell_layout(self, initializing: bool = False) -> None:
        if self.move_list_visible:
            self.move_list_panel.grid()
        else:
            self.move_list_panel.grid_remove()
        if self.panel_visible:
            self.inspector.grid()
            self._set_panel_toggle_label('Hide Training Panel')
        else:
            self.inspector.grid_remove()
            self._set_panel_toggle_label('Show Training Panel')
        if not initializing:
            self._refresh_supporting_surfaces()

    def _set_panel_toggle_label(self, label: str) -> None:
        if hasattr(self.panel_toggle_button, 'configure'):
            self.panel_toggle_button.configure(text=label)

    def _show_loading(self, message: str) -> None:
        self.loading_var.set(message)
        self.loading_frame.place(relx=0.5, rely=0.5, anchor='center')
        self.loading_frame.lift()
        self.loading_progress.start(10)
        self.root.update_idletasks()

    def _hide_loading(self) -> None:
        self.loading_progress.stop()
        self.loading_frame.place_forget()

    def _show_bundle_picker(self, message: str | None = None) -> None:
        self._hide_loading()
        self._populate_bundle_options()
        if message:
            self.bundle_detail_var.set(message)
        self.bundle_picker.place(relx=0.5, rely=0.5, anchor='center')
        self.start_button.configure(state='disabled')

    def _hide_bundle_picker(self) -> None:
        self.bundle_picker.place_forget()
        self.start_button.configure(state='normal')

    def _bundle_search_roots(self) -> tuple[Path, ...]:
        return tuple(Path.cwd() / root for root in DEFAULT_BUNDLE_SEARCH_ROOTS)

    def _populate_bundle_options(self) -> None:
        discovered: list[tuple[str, Path]] = []
        for root in self._bundle_search_roots():
            if not root.exists():
                continue
            for candidate in sorted((path for path in root.iterdir() if path.is_dir()), key=lambda item: item.name.lower()):
                compatibility = inspect_corpus_bundle(candidate)
                if compatibility.available:
                    discovered.append((candidate.name.replace('_', ' '), compatibility.bundle_dir))
        self.available_bundles = discovered
        values = [f'{label} — {path}' for label, path in discovered]
        self.bundle_combobox.configure(values=values)
        if values:
            self.bundle_combobox.set(values[0])
            self.bundle_path_var.set(str(discovered[0][1]))
        else:
            self.bundle_combobox.set('')
        self.bundle_combobox.bind('<<ComboboxSelected>>', self._on_bundle_combo_selected)

    def _on_bundle_combo_selected(self, _event=None) -> None:
        index = self.bundle_combobox.current()
        if 0 <= index < len(self.available_bundles):
            self.bundle_path_var.set(str(self.available_bundles[index][1]))

    def _browse_bundle_path(self) -> None:
        selected = filedialog.askdirectory(parent=self.root, title='Select corpus bundle directory')
        if selected:
            self.bundle_path_var.set(selected)

    def _bundle_path_is_valid(self, bundle_path: str | Path | None) -> bool:
        if not bundle_path:
            return False
        return inspect_corpus_bundle(Path(bundle_path)).available

    def _apply_bundle_selection(self) -> None:
        bundle_path = self.bundle_path_var.get().strip()
        if not bundle_path:
            messagebox.showerror('Corpus bundle', 'Choose a discovered bundle or browse to a valid bundle directory.', parent=self.root)
            return
        self._load_selected_bundle(bundle_path)

    def _skip_bundle_selection(self) -> None:
        self._set_last_bundle_path(None)
        self._update_bundle_summary()
        self._hide_bundle_picker()
        self._start_game()

    def _build_runtime_for_bundle(self, bundle_path: str | None) -> RuntimeContext:
        current = self.session.runtime_context.config
        return load_runtime_config(
            RuntimeOverrides(
                corpus_bundle_dir=bundle_path,
                corpus_artifact_path=current.corpus_artifact_path,
                engine_executable_path=current.engine_executable_path,
                opening_book_path=current.opening_book_path,
                engine_depth=current.engine_depth,
                engine_time_limit_seconds=current.engine_time_limit_seconds,
                strict_assets=current.strict_assets,
            )
        )

    def _load_selected_bundle(self, bundle_path: str) -> None:
        self._start_loading_job(
            initial_message='Loading corpus bundle…\nDiscovering bundle metadata, initializing corpus payload, and preparing the trainer session.',
            worker=lambda: self._load_bundle_worker(bundle_path),
            on_success=self._apply_loaded_bundle,
            on_error=lambda exc: self._show_bundle_picker(f'Could not load bundle: {exc}'),
        )

    def _load_bundle_worker(self, bundle_path: str) -> dict[str, object]:
        compatibility = inspect_corpus_bundle(Path(bundle_path))
        if not compatibility.available:
            raise ValueError(compatibility.detail)
        runtime_context = self._build_runtime_for_bundle(str(compatibility.bundle_dir))
        session = TrainingSession(runtime_context=runtime_context, mode='gui', review_storage=self.session.review_storage)
        session.start_new_game()
        return {'session': session, 'bundle_path': str(compatibility.bundle_dir)}

    def _apply_loaded_bundle(self, payload: dict[str, object]) -> None:
        self._cancel_pending_opponent_callback()
        self.session = payload['session']
        self.panel_visible = self._load_panel_visibility_preference()
        self.move_list_visible = self._load_move_list_visibility_preference()
        self.inspector.session = self.session
        self._set_last_bundle_path(payload['bundle_path'])
        self._update_bundle_summary()
        self._apply_shell_layout(initializing=True)
        self._hide_bundle_picker()
        self.selected_square = None
        self.pending_restart = False
        self.board_view.cancel_drag()
        self._refresh_view()

    def _update_bundle_summary(self) -> None:
        bundle_path = self._remembered_bundle_path()
        if bundle_path and self._bundle_path_is_valid(bundle_path):
            compatibility = inspect_corpus_bundle(Path(bundle_path))
            self.bundle_status_var.set(f'Active bundle: {Path(bundle_path).name}')
            self.bundle_detail_var.set(compatibility.detail)
            return
        if bundle_path:
            self.bundle_status_var.set('Remembered bundle unavailable')
            self.bundle_detail_var.set(f'{bundle_path} is missing or no longer valid. Choose another bundle or continue in fallback mode.')
            return
        self.bundle_status_var.set('Active bundle: fallback / none')
        self.bundle_detail_var.set('No bundle selected. The trainer will fall back to Stockfish and then random legal moves when needed.')

    def _open_bundle_picker(self):
        self._show_bundle_picker('Choose a corpus bundle for this session. The last valid bundle will be reused on future launches.')

    def _open_profiles(self):
        ProfileDialog(self.root, self.session, self._refresh_supporting_surfaces).open()

    def _start_game(self, loading_message: str | None = None):
        self._cancel_pending_opponent_callback()
        if loading_message is None:
            self.session.start_new_game()
            self.selected_square = None
            self.pending_restart = False
            self.board_view.cancel_drag()
            self._refresh_view()
            return
        self._start_loading_job(
            initial_message=loading_message,
            worker=self._start_game_worker,
            on_success=lambda _payload: self._apply_started_game(),
            on_error=lambda exc: messagebox.showerror('Trainer startup', str(exc), parent=self.root),
        )

    def _start_game_worker(self):
        self.session.start_new_game()
        return None

    def _apply_started_game(self) -> None:
        self.selected_square = None
        self.pending_restart = False
        self.board_view.cancel_drag()
        self._refresh_view()

    def _set_loading_message(self, message: str) -> None:
        self.loading_var.set(message)
        if hasattr(self.root, 'update_idletasks'):
            self.root.update_idletasks()

    def _start_loading_job(self, *, initial_message: str, worker, on_success, on_error) -> None:
        if self._loading_job_active or getattr(self, '_is_shutting_down', False):
            return
        self._loading_job_active = True
        self._show_loading(initial_message)
        self.start_button.configure(state='disabled')
        self._loading_queue = queue.Queue()

        def wrapped_worker():
            try:
                payload = worker()
                self._loading_queue.put(('success', payload))
            except Exception as exc:  # noqa: BLE001
                self._loading_queue.put(('error', exc))

        self._loading_thread = threading.Thread(target=wrapped_worker, daemon=True)
        self._loading_thread.start()
        self._schedule_after(25, lambda: self._poll_loading_job(on_success=on_success, on_error=on_error))

    def _poll_loading_job(self, *, on_success, on_error) -> None:
        if self._loading_queue is None:
            return
        try:
            status, payload = self._loading_queue.get_nowait()
        except queue.Empty:
            if not getattr(self, '_is_shutting_down', False):
                self._schedule_after(25, lambda: self._poll_loading_job(on_success=on_success, on_error=on_error))
            return
        self._loading_job_active = False
        self._hide_loading()
        self.start_button.configure(state='normal')
        self._loading_thread = None
        self._loading_queue = None
        if status == 'success':
            on_success(payload)
            return
        on_error(payload)

    def _build_counts_summary(self, due: int, boosted: int, extreme: int) -> str:
        return f'Due: {due} | Boosted: {boosted} | Extreme: {extreme}'

    def _build_routing_summary(self, routing: str, explain: str) -> str:
        return f'Routing: {routing} | {explain}'

    def _build_compact_bundle_summary(self) -> str:
        return f'Opponent: {self.session.opponent.status_message}'

    def _training_depth_summary(self) -> str:
        retained_ply_depth = self.session.bundle_retained_ply_depth()
        cap = self.session.max_supported_training_depth()
        retained_text = f' | Bundle retained depth: {retained_ply_depth} plies' if retained_ply_depth is not None else ''
        return f'Training depth: {self.session.required_player_moves} player moves | Good accepted: {"yes" if self.session.settings.good_moves_acceptable else "no"} | Max supported: {cap} player moves{retained_text}'

    def _refresh_supporting_surfaces(self):
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        profile_name = self.session.review_storage.load_profile_meta(self.session.active_profile_id).display_name
        due = sum(1 for item in items if item.due_at_utc <= item.updated_at_utc)
        boosted = sum(1 for item in items if item.urgency_tier == 'boosted_review')
        extreme = sum(1 for item in items if item.urgency_tier == 'extreme_urgency')
        routing = self.session.current_routing.routing_source if self.session.current_routing else 'not_started'
        explain = self.session.current_routing.selection_explanation if self.session.current_routing else 'No routing decision yet.'
        if self.session.current_routing and self.session.current_routing.corpus_share is not None and self.session.current_routing.review_share is not None:
            explain = (
                f'corpus_share={self.session.current_routing.corpus_share:.2f}; '
                f'review_share={self.session.current_routing.review_share:.2f}; '
                f'due_count={self.session.current_routing.due_count}; '
                f'boosted_due_count={self.session.current_routing.boosted_due_count}; '
                f'extreme_due_count={self.session.current_routing.extreme_due_count}; '
                f'deck_size={self.session.current_routing.deck_size}; '
                f'{explain}'
            )
        counts_summary = self._build_counts_summary(due, boosted, extreme)
        routing_summary = self._build_routing_summary(routing, explain)
        bundle_summary = f'Opponent source: {self.session.opponent.status_message}'
        corpus_summary = self.session.corpus_summary_text()
        self.status_panel.update_status(profile_name=profile_name, bundle_summary=bundle_summary, corpus_summary=corpus_summary, routing_summary=routing_summary, counts_summary=counts_summary)
        self.compact_status_panel.update_status(profile_name=profile_name, bundle_summary=self._build_compact_bundle_summary(), corpus_summary=corpus_summary, routing_summary=f'Route: {routing}', counts_summary=self._training_depth_summary())
        history_path = self.session.review_storage.root / self.session.active_profile_id / 'session_history.jsonl'
        recent = history_path.read_text(encoding='utf-8').strip().splitlines()[-4:] if history_path.exists() else []
        self.recent_var.set(self._training_depth_summary() + '\n' + corpus_summary + '\n\nRecent events:\n' + ('\n'.join(recent) if recent else 'No recent events.'))
        self.inspector.refresh()
        view = self.session.get_view()
        self.move_list_panel.update_moves(view.move_history)
        board = chess.Board(view.board_fen)
        self.top_captured_panel.update_board(board, player_color=view.player_color, near_side=False)
        self.bottom_captured_panel.update_board(board, player_color=view.player_color, near_side=True)
        self._update_bundle_summary()

    def _open_options(self):
        window = tk.Toplevel(self.root)
        self._child_windows.append(window)
        window.title('Trainer Options')
        window.transient(self.root)
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill='both', expand=True)
        cap = self.session.max_supported_training_depth()
        depth_var = tk.IntVar(value=self.session.settings.active_training_ply_depth)
        good_var = tk.BooleanVar(value=self.session.settings.good_moves_acceptable)
        panel_var = tk.BooleanVar(value=self.panel_visible)
        move_list_var = tk.BooleanVar(value=self.move_list_visible)
        ttk.Checkbutton(frame, text='Accept Good moves (permissive mode)', variable=good_var).pack(anchor='w', pady=(0, 8))
        ttk.Checkbutton(frame, text='Show training/review panel by default', variable=panel_var).pack(anchor='w')
        ttk.Checkbutton(frame, text='Show move list by default', variable=move_list_var).pack(anchor='w', pady=(0, 8))
        ttk.Label(frame, text='Training depth (player moves)').pack(anchor='w')
        ttk.Combobox(frame, state='readonly', textvariable=depth_var, values=list(range(2, cap + 1)), width=8).pack(anchor='w', pady=(0, 8))
        retained_ply_depth = self.session.bundle_retained_ply_depth()
        detail = f'Current bundle supports up to {cap} player moves'
        if retained_ply_depth is not None:
            detail += f' ({retained_ply_depth} retained plies reported by the manifest).'
        else:
            detail += '. Conservative fallback cap applied.'
        ttk.Label(frame, text=detail, wraplength=320, justify='left').pack(anchor='w', pady=(0, 8))

        def save():
            self.panel_visible = panel_var.get()
            self.move_list_visible = move_list_var.get()
            self.session.update_settings(TrainerSettings(good_var.get(), depth_var.get(), self.panel_visible, self.move_list_visible, self._remembered_bundle_path()))
            window.destroy()
            self._apply_shell_layout(initializing=True)
            self._refresh_supporting_surfaces()

        ttk.Button(frame, text='Save', command=save).pack(side='left')
        ttk.Button(frame, text='Cancel', command=window.destroy).pack(side='left', padx=(8, 0))

    def _refresh_view(self, transient_status: str | None = None) -> None:
        view = self.session.get_view()
        board = chess.Board(view.board_fen)
        legal_targets = []
        if self.selected_square is not None and view.awaiting_user_input:
            legal_targets = [move.to_square for move in self.session.legal_moves_from(self.selected_square)]
        self.board_view.set_selection(self.selected_square, legal_targets)
        self.board_view.render(board, view.player_color)
        self._refresh_supporting_surfaces()
        if transient_status:
            self.recent_var.set(transient_status + '\n\n' + self.recent_var.get())
        if view.state == SessionState.RESTART_PENDING and view.last_outcome is not None and not self.pending_restart:
            self.pending_restart = True
            self._show_outcome_modal(view)

    def _show_outcome_modal(self, view):
        outcome = view.last_outcome
        if outcome is None:
            return None
        review_boards: list[OutcomeBoardContract] = []
        if outcome.terminal_kind == 'fail' and outcome.pre_fail_fen and outcome.preferred_move_uci:
            review_boards.append(OutcomeBoardContract(
                title='What you should have played',
                board_fen=outcome.pre_fail_fen,
                player_color=outcome.player_color,
                arrow_move_uci=outcome.preferred_move_uci,
                arrow_color='#2e7d32',
                arrow_label='Correct move',
                move_label=outcome.preferred_move_san or outcome.preferred_move_uci,
            ))
        if outcome.terminal_kind == 'fail' and outcome.post_fail_fen and outcome.punishing_reply_uci:
            review_boards.append(OutcomeBoardContract(
                title='What punishes this',
                board_fen=outcome.post_fail_fen,
                player_color=outcome.player_color,
                arrow_move_uci=outcome.punishing_reply_uci,
                arrow_color='#c62828',
                arrow_label='Likely punishment',
                move_label=outcome.punishing_reply_san or outcome.punishing_reply_uci,
            ))
        contract = OutcomeModalContract(
            headline='SUCCESS' if outcome.passed else 'FAIL',
            summary=outcome.reason,
            reason=outcome.reason,
            preferred_move=outcome.preferred_move,
            routing_reason=outcome.routing_reason,
            next_routing_reason=outcome.next_routing_reason,
            impact_summary=f'Profile: {outcome.profile_name} | {outcome.impact_summary}',
            review_boards=tuple(review_boards),
        )
        return OutcomeModal(self.root, contract, self._acknowledge_outcome)

    def _acknowledge_outcome(self):
        self._cancel_pending_opponent_callback()
        self.pending_restart = False
        self.selected_square = None
        self.session.start_new_game()
        self._refresh_view()

    def _on_board_press(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if not view.awaiting_user_input:
            self._refresh_view('Wait for your turn.')
            return
        square = self.board_view.square_at_xy(event.x, event.y, view.player_color)
        if square is None:
            return
        board = self.session.current_board()
        piece = board.piece_at(square)
        if piece is None or piece.color != view.player_color:
            if self.selected_square is None:
                self._refresh_view('Select one of your own pieces.')
            return
        legal_moves = self.session.legal_moves_from(square)
        if not legal_moves:
            self._refresh_view('That piece has no legal moves.')
            return
        self.selected_square = square
        self.board_view.start_drag(square, piece.symbol(), event.x, event.y)
        self._refresh_view()

    def _on_board_drag(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if self.selected_square is None or not view.awaiting_user_input:
            return
        self.board_view.update_drag(event.x, event.y, view.player_color)
        self.board_view.render(chess.Board(view.board_fen), view.player_color)

    def _on_board_release(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if self.selected_square is None or not view.awaiting_user_input:
            return
        released = self.board_view.release_drag(event.x, event.y, view.player_color)
        if released is None:
            return
        from_square, to_square, was_drag = released
        board = self.session.current_board()
        if not was_drag:
            if to_square == self.selected_square:
                self._refresh_view()
                return
            if to_square is None:
                self.selected_square = None
                self._refresh_view('Selection cleared.')
                return
            destination_piece = board.piece_at(to_square)
            if destination_piece is not None and destination_piece.color == view.player_color:
                self.selected_square = to_square
                self._refresh_view()
                return
        if to_square is None:
            self.selected_square = None
            self._refresh_view('Move cancelled.')
            return
        move = self._build_move(from_square, to_square, board)
        self.selected_square = None
        if move is None:
            self._refresh_view('Illegal move selection.')
            return
        self.board_view.cancel_drag()
        self.session.submit_user_move_uci(move.uci())
        self._refresh_view()
        self._schedule_pending_opponent_commit()

    def _build_move(self, from_square: chess.Square, to_square: chess.Square, board: chess.Board) -> chess.Move | None:
        candidate = chess.Move(from_square, to_square)
        if candidate in board.legal_moves:
            return candidate
        for promotion_piece in PROMOTION_CHOICES.values():
            promoted = chess.Move(from_square, to_square, promotion=promotion_piece)
            if promoted in board.legal_moves:
                choice = simpledialog.askstring('Promotion', 'Promote to (q, r, b, n). Default is q:', parent=self.root)
                code = (choice or 'q').strip().lower()
                return chess.Move(from_square, to_square, promotion=PROMOTION_CHOICES.get(code, chess.QUEEN))
        return None


    def _build_menubar(self) -> None:
        menubar = tk.Menu(self.root)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label='Exit', command=self._request_shutdown)
        menubar.add_cascade(label='File', menu=file_menu)
        dev_menu = tk.Menu(menubar, tearoff=0)
        dev_menu.add_command(label='Open Dev Console', command=self._open_dev_console)
        dev_menu.add_command(label='Timing Override...', command=self._open_timing_override_dialog)
        dev_menu.add_command(label='Open Logs Folder', command=self._open_logs_folder)
        dev_menu.add_command(label='Copy Current Session Log Path', command=self._copy_session_log_path)
        dev_menu.add_command(label='Clear Visible Buffer', command=self._clear_visible_log_buffer)
        menubar.add_cascade(label='Developer', menu=dev_menu)
        self.root.config(menu=menubar)

    def _open_dev_console(self) -> None:
        self.dev_console.open()

    def _open_timing_override_dialog(self) -> None:
        self.timing_override_dialog.open()

    def _open_logs_folder(self) -> None:
        logs_folder = str(self.session_logger.log_path.parent.resolve())
        try:
            if os.name == 'nt':
                os.startfile(logs_folder)
            elif os.uname().sysname == 'Darwin':
                subprocess.Popen(['open', logs_folder])
            else:
                subprocess.Popen(['xdg-open', logs_folder])
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror('Logs Folder', f'Could not open logs folder.\n{exc}', parent=self.root)

    def _copy_session_log_path(self) -> None:
        log_path = str(self.session_logger.log_path.resolve())
        self.root.clipboard_clear()
        self.root.clipboard_append(log_path)
        self.root.update_idletasks()
        messagebox.showinfo('Session Log Path', f'Copied to clipboard:\n{log_path}', parent=self.root)

    def _clear_visible_log_buffer(self) -> None:
        self.session_logger.clear_visible_buffer()

    def _schedule_after(self, delay_ms: int, callback) -> None:
        if getattr(self, '_is_shutting_down', False):
            return
        root = getattr(self, 'root', None)
        if root is None:
            return
        def wrapped_callback():
            self._after_handles.discard(handle)
            callback()

        handle = root.after(delay_ms, wrapped_callback)
        if not hasattr(self, '_after_handles'):
            self._after_handles = set()
        self._after_handles.add(handle)
        return handle

    def _schedule_pending_opponent_commit(self) -> None:
        if self.session.state != SessionState.OPPONENT_TURN:
            return
        pending = self.session.prepare_pending_opponent_action()
        if pending is None:
            return
        self._cancel_pending_opponent_callback()
        delay_ms = max(0, int(round(pending.visible_delay_seconds * 1000)))
        if delay_ms == 0:
            self._commit_scheduled_opponent_action()
            return
        self._pending_opponent_after_handle = self._schedule_after(delay_ms, self._commit_scheduled_opponent_action)

    def _commit_scheduled_opponent_action(self) -> None:
        self._pending_opponent_after_handle = None
        if getattr(self, '_is_shutting_down', False):
            return
        if self.session.pending_opponent_action is None:
            return
        self.session.commit_pending_opponent_action()
        self._refresh_view()
        if self.session.state == SessionState.OPPONENT_TURN:
            self._schedule_pending_opponent_commit()

    def _cancel_pending_opponent_callback(self) -> None:
        handle = getattr(self, '_pending_opponent_after_handle', None)
        self._pending_opponent_after_handle = None
        if handle is not None:
            root = getattr(self, 'root', None)
            try:
                if root is not None:
                    root.after_cancel(handle)
            except Exception:
                pass
            after_handles = getattr(self, '_after_handles', set())
            after_handles.discard(handle)
        session = getattr(self, 'session', None)
        cancel_pending = getattr(session, 'cancel_pending_opponent_action', None) if session is not None else None
        if callable(cancel_pending):
            cancel_pending()

    def _cancel_after_handles(self) -> None:
        handles = list(getattr(self, '_after_handles', set()))
        if not hasattr(self, '_after_handles'):
            self._after_handles = set()
        root = getattr(self, 'root', None)
        for handle in handles:
            try:
                if root is not None:
                    root.after_cancel(handle)
            except Exception as exc:  # noqa: BLE001
                log_line(f'APP_SHUTDOWN_TIMER_CANCEL_FAILED: handle={handle}; error={exc}', tag='error')
            finally:
                self._after_handles.discard(handle)

    def _request_shutdown(self) -> None:
        self._shutdown_coordinator(reason='window_close')

    def _shutdown_coordinator(self, reason: str) -> None:
        if getattr(self, '_shutdown_started', False):
            return
        self._shutdown_started = True
        self._is_shutting_down = True
        log_line(f'APP_SHUTDOWN_BEGIN: reason={reason}', tag='startup')
        self._cancel_pending_opponent_callback()
        self._cancel_after_handles()
        log_line('APP_SHUTDOWN_CANCEL_TIMERS_DONE', tag='startup')
        self._close_optional_component('dev_console')
        self._close_optional_component('timing_override_dialog')
        self._close_child_windows()
        log_line('APP_SHUTDOWN_DIALOGS_DONE', tag='startup')
        log_line('ENGINE_SHUTDOWN_BEGIN', tag='startup')
        try:
            session = getattr(self, 'session', None)
            if session is not None:
                session.close()
        finally:
            log_line('ENGINE_SHUTDOWN_COMPLETE', tag='startup')
        log_line('APP_SHUTDOWN_SESSION_DONE', tag='startup')
        remove_instance_diagnostics()
        release_single_instance_guard()
        log_line('APP_SHUTDOWN_GUARD_RELEASED', tag='startup')
        root = getattr(self, 'root', None)
        if root is not None:
            try:
                root.destroy()
                log_line('APP_SHUTDOWN_ROOT_DESTROYED', tag='startup')
            except Exception as exc:  # noqa: BLE001
                log_line(f'APP_SHUTDOWN_ROOT_DESTROY_FAILED: {exc}', tag='error')
        log_line('APP_SHUTDOWN_COMPLETE', tag='startup')

    def _close_optional_component(self, attr_name: str) -> None:
        component = getattr(self, attr_name, None)
        if component is None:
            return
        close = getattr(component, 'close', None)
        if close is None:
            return
        try:
            close()
        except Exception as exc:  # noqa: BLE001
            log_line(f'APP_SHUTDOWN_OPTIONAL_CLOSE_FAILED: component={attr_name}; error={exc}', tag='error')

    def _close_child_windows(self) -> None:
        child_windows = list(getattr(self, '_child_windows', []))
        for window in child_windows:
            self._destroy_window(window)
        if hasattr(self, '_child_windows'):
            self._child_windows.clear()
        root = getattr(self, 'root', None)
        if root is None:
            return
        winfo_children = getattr(root, 'winfo_children', None)
        if winfo_children is None:
            return
        try:
            children = list(winfo_children())
        except Exception as exc:  # noqa: BLE001
            log_line(f'APP_SHUTDOWN_CHILD_ENUM_FAILED: {exc}', tag='error')
            return
        for child in children:
            if isinstance(child, tk.Toplevel):
                self._destroy_window(child)

    def _destroy_window(self, window) -> None:
        try:
            winfo_exists = getattr(window, 'winfo_exists', None)
            if winfo_exists is not None and not winfo_exists():
                return
            window.destroy()
        except Exception as exc:  # noqa: BLE001
            log_line(f'APP_SHUTDOWN_WINDOW_DESTROY_FAILED: {exc}', tag='error')


def launch_gui(runtime_context: RuntimeContext | None = None) -> None:
    log_line("APP_BOOT_BEGIN", tag="startup")
    if not acquire_single_instance_guard():
        log_line("APP_DUPLICATE_BLOCKED", tag="startup")
        diagnostics = read_instance_diagnostics()
        if diagnostics is not None:
            log_line(
                f"APP_DUPLICATE_OWNER_INFO_AVAILABLE: pid={diagnostics.pid}; "
                f"started_at={diagnostics.startup_utc}; log_path={diagnostics.session_log_path}; session_id={diagnostics.session_id}",
                tag="startup",
            )
        else:
            log_line("APP_DUPLICATE_OWNER_INFO_MISSING", tag="startup")
        log_line("INSTANCE_DUPLICATE: Opening Trainer is already starting or running.", tag="startup")
        return
    log_line("GUI_BOOTSTRAP: creating Tk root.", tag="startup")
    try:
        app = OpeningTrainerGUI(runtime_context=runtime_context)
        write_instance_diagnostics(window_title='Opening Trainer')
        log_line("GUI_READY: Opening Trainer GUI initialized.", tag="startup")
        app.run()
    except Exception as exc:
        log_line(f"GUI_STARTUP_FAILED: {exc}", tag="error")
        remove_instance_diagnostics()
        release_single_instance_guard()
        raise
