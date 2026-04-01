from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import ctypes
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from tkinter import filedialog, messagebox, simpledialog
from tkinter import ttk

import chess

from ..corpus.catalog import (
    DEFAULT_CORPUS_CATALOG_ROOT,
    bundle_variant_label,
    discover_corpus_catalog,
    sort_key_rating_band,
    sort_key_time_control,
)
from ..models import SessionState
from ..opponent import (
    OPPONENT_FALLBACK_ANY_INSTALLED_HUMAN_BUNDLE,
    OPPONENT_FALLBACK_CURRENT_BUNDLE_ONLY,
    OPPONENT_FALLBACK_NEARBY_HUMAN_BUNDLES,
)
from ..runtime import RuntimeContext, RuntimeOverrides, inspect_corpus_bundle, load_runtime_config
from ..settings import DEFAULT_TRAINING_PANEL_COLUMNS, TrainerSettings
from ..session import TrainingSession
from ..session_contracts import OutcomeArrowContract, OutcomeBoardContract, OutcomeModalContract, ReviewSlideContract
from ..session_logging import get_session_logger, log_line
from ..single_instance import (
    acquire_single_instance_guard,
    read_instance_diagnostics,
    release_single_instance_guard,
    remove_instance_diagnostics,
    write_instance_diagnostics,
)
from ..updater import UpdaterInstallStateError, check_for_update, launch_updater_helper, resolve_manifest_path_or_url
from .board_view import BoardView
from .captured_material_panel import CapturedMaterialPanel
from .dev_console import DevConsoleWindow
from .move_list_panel import MoveListPanel
from .outcome_modal import OutcomeModal
from .profile_dialog import ProfileDialog
from .review_inspector import ReviewInspector
from .timing_override_dialog import TimingOverrideDialog

PROMOTION_CHOICES = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}
DEFAULT_BUNDLE_SEARCH_ROOTS = (Path('artifacts'),)
ANIMATION_IMPL_MARKER = 'committed_move_anim_v2'
ANIMATION_LOG_PREFIX = 'GUI_ANIM'
PLAYER_COMMITTED_MOVE_DURATION_MS = 240
OPPONENT_COMMITTED_MOVE_DURATION_MS = 220
LIVE_CLOCK_REFRESH_INTERVAL_MS = 150
PAUSE_OVERLAY_BG = '#111111'
PAUSE_OVERLAY_FG = '#f5f5f5'


@dataclass(frozen=True)
class PremoveIntent:
    from_square: chess.Square
    to_square: chess.Square
    promotion: int | None = None

    @property
    def uci(self) -> str:
        return chess.Move(self.from_square, self.to_square, promotion=self.promotion).uci()


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
        self.top_summary_var = tk.StringVar(value='')
        self.start_button = None
        self.update_button = None
        self._updater_mode_active = False
        self._updater_apply_started = False
        self._updater_status_window: tk.Toplevel | None = None
        self._widgets_disabled_for_update: list[tuple[tk.Widget, str]] = []
        self.browse_bundle_button = None
        self.bundle_combobox = None
        self.bundle_path_var = tk.StringVar()
        self.available_bundles: list[tuple[str, Path]] = []
        self.catalog_category_var = tk.StringVar()
        self.catalog_time_control_var = tk.StringVar()
        self.catalog_rating_band_var = tk.StringVar()
        self.catalog_variant_var = tk.StringVar()
        self.catalog_summary_var = tk.StringVar(value='Catalog selection summary will appear here.')
        self.smart_mode_var = tk.BooleanVar(value=self.session.settings.training_mode == 'smart_profile')
        self.top_track_var = tk.StringVar(value='Rapid')
        self.top_level_var = tk.StringVar(value='—')
        self.top_elo_var = tk.StringVar(value='—')
        self.top_depth_var = tk.StringVar(value='—')
        self.top_good_var = tk.StringVar(value='—')
        self.top_time_control_var = tk.StringVar(value=self.session.settings.selected_time_control_id)
        self.manual_elo_var = tk.StringVar(value='')
        self.manual_depth_var = tk.IntVar(value=self.session.settings.active_training_ply_depth)
        self.manual_good_var = tk.StringVar(value='Yes' if self.session.settings.good_moves_acceptable else 'No')
        self.opponent_fallback_mode_var = tk.StringVar(value=self.session.settings.opponent_fallback_mode)
        self.catalog_root_var = tk.StringVar()
        self.catalog_category_combo = None
        self.catalog_time_control_combo = None
        self.catalog_rating_band_combo = None
        self.catalog_variant_combo = None
        self.catalog_grouped = {}
        self.catalog_leaf_variants = []
        self.catalog = None
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
        self._board_animation_after_handle = None
        self._supporting_surfaces_after_handle = None
        self._supporting_refresh_pending_after_first_tick = False
        self._deferred_outcome_view = None
        self._child_windows: list[tk.Toplevel] = []
        self.premove_queue: list[PremoveIntent] = []
        self._clock_refresh_after_handle = None
        self.first_boot_ready_required = True
        self.ready_overlay_visible = False
        self.paused = False
        self.pause_started_at_monotonic: float | None = None
        self._clock_suspend_started_at_monotonic: float | None = None
        self._frozen_pending_opponent_remaining_delay_seconds: float | None = None
        self._pending_opponent_scheduled_at_monotonic: float | None = None
        self._pause_overlay_window: tk.Toplevel | None = None
        self._ready_overlay_frame = None

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        self._build_menubar()

        toolbar = tk.Frame(self.root)
        toolbar.grid(row=0, column=0, sticky='ew', padx=12, pady=(12, 4))
        self.start_button = tk.Button(
            toolbar,
            text='Start drill',
            command=self._start_game,
            bg='#ff2d55',
            fg='white',
            activebackground='#d90429',
            activeforeground='white',
            disabledforeground='#ffe3ea',
        )
        self.start_button.pack(side='left')
        tk.Button(toolbar, text='Profiles', command=self._open_profiles).pack(side='left', padx=6)
        tk.Button(toolbar, text='Options', command=self._open_options).pack(side='left', padx=6)
        tk.Button(toolbar, text='Corpus Selection', command=self._open_bundle_picker).pack(side='left', padx=6)
        tk.Button(toolbar, text='Report', command=self._show_report_placeholder).pack(side='left', padx=6)
        self.update_button = tk.Button(toolbar, text='Update', command=self._check_for_updates_from_gui)
        self.update_button.pack(side='left', padx=6)
        self.panel_toggle_button = tk.Button(toolbar, text='', command=self._toggle_side_panel)
        self.panel_toggle_button.pack(side='left', padx=(6, 0))

        self.summary_strip = ttk.Frame(self.root)
        self.summary_strip.grid(row=1, column=0, sticky='ew', padx=12, pady=(0, 2))
        ttk.Label(self.summary_strip, textvariable=self.top_summary_var).grid(row=0, column=0, sticky='w')
        self.control_strip = ttk.Frame(self.root)
        self.control_strip.grid(row=2, column=0, sticky='ew', padx=12, pady=(0, 4))
        ttk.Checkbutton(self.control_strip, text='Smart', variable=self.smart_mode_var, command=self._on_top_mode_toggle).grid(row=0, column=0, sticky='w', padx=(8, 6), pady=6)
        ttk.Label(self.control_strip, text='Track').grid(row=0, column=1, sticky='w')
        ttk.Label(self.control_strip, textvariable=self.top_track_var).grid(row=0, column=2, sticky='w', padx=(0, 10))
        ttk.Label(self.control_strip, text='Level').grid(row=0, column=3, sticky='w')
        self.top_level_label = ttk.Label(self.control_strip, textvariable=self.top_level_var)
        self.top_level_label.grid(row=0, column=4, sticky='w', padx=(0, 10))
        ttk.Label(self.control_strip, text='Control').grid(row=0, column=5, sticky='w')
        self.top_time_control_combo = ttk.Combobox(self.control_strip, state='readonly', textvariable=self.top_time_control_var, width=8)
        self.top_time_control_combo.grid(row=0, column=6, sticky='w', padx=(0, 10))
        ttk.Label(self.control_strip, text='ELO').grid(row=0, column=7, sticky='w')
        self.top_elo_label = ttk.Label(self.control_strip, textvariable=self.top_elo_var)
        self.top_elo_label.grid(row=0, column=8, sticky='w', padx=(0, 6))
        self.top_elo_combo = ttk.Combobox(self.control_strip, state='readonly', textvariable=self.manual_elo_var, width=10)
        self.top_elo_combo.grid(row=0, column=8, sticky='w', padx=(0, 6))
        ttk.Label(self.control_strip, text='Depth').grid(row=0, column=9, sticky='w')
        self.top_depth_label = ttk.Label(self.control_strip, textvariable=self.top_depth_var)
        self.top_depth_label.grid(row=0, column=10, sticky='w', padx=(0, 6))
        self.top_depth_combo = ttk.Combobox(self.control_strip, state='readonly', textvariable=self.manual_depth_var, width=4)
        self.top_depth_combo.grid(row=0, column=10, sticky='w', padx=(0, 6))
        ttk.Label(self.control_strip, text='Good Moves Allowed?').grid(row=0, column=11, sticky='w')
        self.top_good_label = ttk.Label(self.control_strip, textvariable=self.top_good_var)
        self.top_good_label.grid(row=0, column=12, sticky='w', padx=(0, 8))
        self.top_good_combo = ttk.Combobox(self.control_strip, state='readonly', textvariable=self.manual_good_var, values=['Yes', 'No'], width=4)
        self.top_good_combo.grid(row=0, column=12, sticky='w', padx=(0, 8))
        ttk.Label(self.control_strip, text='Human fallback').grid(row=0, column=13, sticky='w')
        self.fallback_mode_combo = ttk.Combobox(
            self.control_strip,
            state='readonly',
            textvariable=self.opponent_fallback_mode_var,
            values=(
                OPPONENT_FALLBACK_CURRENT_BUNDLE_ONLY,
                OPPONENT_FALLBACK_NEARBY_HUMAN_BUNDLES,
                OPPONENT_FALLBACK_ANY_INSTALLED_HUMAN_BUNDLE,
            ),
            width=28,
        )
        self.fallback_mode_combo.grid(row=0, column=14, sticky='w', padx=(0, 8))
        self.top_time_control_combo.bind('<<ComboboxSelected>>', self._on_top_time_control_selected)
        self.top_elo_combo.bind('<<ComboboxSelected>>', self._on_manual_contract_changed)
        self.top_depth_combo.bind('<<ComboboxSelected>>', self._on_manual_contract_changed)
        self.top_good_combo.bind('<<ComboboxSelected>>', self._on_manual_contract_changed)
        self.fallback_mode_combo.bind('<<ComboboxSelected>>', self._on_manual_contract_changed)

        self.root_pane = tk.PanedWindow(self.root, orient=tk.HORIZONTAL, sashrelief=tk.RAISED, bd=0)
        self.root_pane.grid(row=3, column=0, sticky='nsew', padx=12, pady=(6, 12))

        self.main_region = tk.Frame(self.root)
        self.main_region.columnconfigure(0, weight=1, minsize=420)
        self.main_region.rowconfigure(1, weight=1)
        self.top_captured_panel = CapturedMaterialPanel(self.main_region)
        self.top_captured_panel.grid(row=0, column=0, sticky='ew', pady=(0, 8))
        self.board_view = BoardView(self.main_region, board_size=560, min_board_size=360)
        self.board_view.grid(row=1, column=0, sticky='nsew')
        self.bottom_captured_panel = CapturedMaterialPanel(self.main_region)
        self.bottom_captured_panel.grid(row=2, column=0, sticky='ew', pady=(8, 0))

        self.side_panel = tk.Frame(self.root, width=360)
        self.side_panel.columnconfigure(0, weight=1)
        self.side_panel.rowconfigure(0, weight=1)
        self.recent_var = tk.StringVar()
        self.side_content = ttk.Frame(self.side_panel)
        self.side_content.grid(row=0, column=0, sticky='nsew')
        self.side_content.columnconfigure(0, weight=1)
        self.side_content.rowconfigure(0, weight=4)
        self.side_content.rowconfigure(1, weight=1)
        self.move_list_panel = MoveListPanel(self.side_content)
        self.move_list_panel.grid(row=0, column=0, sticky='nsew', pady=(0, 8))
        self.inspector = ReviewInspector(
            self.side_content,
            self.session,
            self._refresh_supporting_surfaces,
            visible_columns=self.session.settings.training_panel_visible_columns,
        )
        self.inspector.grid(row=1, column=0, sticky='nsew')

        self.loading_frame = ttk.Frame(self.main_region, padding=18)
        self.loading_frame.place(relx=0.5, rely=0.5, anchor='center')
        ttk.Label(self.loading_frame, text='Opening Trainer', font=('TkDefaultFont', 13, 'bold')).pack(pady=(0, 8))
        ttk.Label(self.loading_frame, textvariable=self.loading_var, justify='center', wraplength=320).pack(pady=(0, 8))
        self.loading_progress = ttk.Progressbar(self.loading_frame, mode='indeterminate', length=260)
        self.loading_progress.pack(fill='x')

        self.bundle_picker = ttk.LabelFrame(self.main_region, text='Select corpus bundle', padding=12)
        self.bundle_picker.columnconfigure(0, weight=1)
        ttk.Label(
            self.bundle_picker,
            text='Opening Trainer auto-discovers installed corpus content first. Use this dialog only when authoritative discovery cannot bind a valid manifest-driven corpus root.',
            wraplength=360,
            justify='left',
        ).grid(row=0, column=0, columnspan=4, sticky='w')
        ttk.Label(self.bundle_picker, text='Catalog root').grid(row=1, column=0, sticky='w', pady=(10, 0))
        ttk.Entry(self.bundle_picker, textvariable=self.catalog_root_var).grid(row=2, column=0, columnspan=3, sticky='ew', pady=(4, 8))
        ttk.Button(self.bundle_picker, text='Browse root…', command=self._browse_catalog_root).grid(row=2, column=3, sticky='e', padx=(8, 0), pady=(4, 8))
        ttk.Button(self.bundle_picker, text='Refresh catalog', command=self._refresh_catalog).grid(row=3, column=3, sticky='e', padx=(8, 0), pady=(0, 8))
        catalog_frame = ttk.LabelFrame(self.bundle_picker, text='Structured catalog selection')
        catalog_frame.grid(row=4, column=0, columnspan=4, sticky='ew', pady=(0, 8))
        catalog_frame.columnconfigure(1, weight=1)
        ttk.Label(catalog_frame, text='Time category').grid(row=0, column=0, sticky='w', padx=(8, 6), pady=(8, 4))
        self.catalog_category_combo = ttk.Combobox(catalog_frame, state='readonly', textvariable=self.catalog_category_var)
        self.catalog_category_combo.grid(row=0, column=1, sticky='ew', padx=(0, 8), pady=(8, 4))
        ttk.Label(catalog_frame, text='Exact control').grid(row=1, column=0, sticky='w', padx=(8, 6), pady=4)
        self.catalog_time_control_combo = ttk.Combobox(catalog_frame, state='readonly', textvariable=self.catalog_time_control_var)
        self.catalog_time_control_combo.grid(row=1, column=1, sticky='ew', padx=(0, 8), pady=4)
        ttk.Label(catalog_frame, text='Rating band').grid(row=2, column=0, sticky='w', padx=(8, 6), pady=4)
        self.catalog_rating_band_combo = ttk.Combobox(catalog_frame, state='readonly', textvariable=self.catalog_rating_band_var)
        self.catalog_rating_band_combo.grid(row=2, column=1, sticky='ew', padx=(0, 8), pady=4)
        ttk.Label(catalog_frame, text='Variant').grid(row=3, column=0, sticky='w', padx=(8, 6), pady=(4, 8))
        self.catalog_variant_combo = ttk.Combobox(catalog_frame, state='readonly', textvariable=self.catalog_variant_var)
        self.catalog_variant_combo.grid(row=3, column=1, sticky='ew', padx=(0, 8), pady=(4, 8))
        ttk.Label(catalog_frame, textvariable=self.catalog_summary_var, wraplength=360, justify='left').grid(
            row=4, column=0, columnspan=2, sticky='ew', padx=8, pady=(0, 8)
        )
        ttk.Button(catalog_frame, text='Use catalog selection', command=self._apply_catalog_selection).grid(row=5, column=0, sticky='w', padx=8, pady=(0, 8))
        ttk.Button(catalog_frame, text='Load expected Smart Profile bundle', command=self._load_expected_smart_profile_bundle).grid(row=5, column=1, sticky='e', padx=8, pady=(0, 8))
        ttk.Separator(self.bundle_picker, orient='horizontal').grid(row=5, column=0, columnspan=4, sticky='ew', pady=(4, 8))
        ttk.Label(self.bundle_picker, text='Legacy direct bundle selection', justify='left').grid(row=6, column=0, columnspan=4, sticky='w')
        ttk.Label(self.bundle_picker, text='Choose a discovered bundle or browse to a custom bundle path.', wraplength=360, justify='left').grid(row=7, column=0, columnspan=4, sticky='w')
        self.bundle_combobox = ttk.Combobox(self.bundle_picker, state='readonly')
        self.bundle_combobox.grid(row=8, column=0, columnspan=3, sticky='ew', pady=(10, 8))
        ttk.Button(self.bundle_picker, text='Refresh', command=self._populate_bundle_options).grid(row=8, column=3, sticky='e', padx=(8, 0), pady=(10, 8))
        ttk.Label(self.bundle_picker, text='Custom path').grid(row=9, column=0, sticky='w')
        ttk.Entry(self.bundle_picker, textvariable=self.bundle_path_var).grid(row=10, column=0, columnspan=3, sticky='ew', pady=(4, 0))
        self.browse_bundle_button = ttk.Button(self.bundle_picker, text='Browse…', command=self._browse_bundle_path)
        self.browse_bundle_button.grid(row=10, column=3, sticky='e', padx=(8, 0))
        actions = ttk.Frame(self.bundle_picker)
        actions.grid(row=11, column=0, columnspan=4, sticky='ew', pady=(12, 0))
        ttk.Button(actions, text='Use selection', command=self._apply_bundle_selection).pack(side='left')
        ttk.Button(actions, text='Skip bundle', command=self._skip_bundle_selection).pack(side='left', padx=(8, 0))

        self.root_pane.add(self.main_region, minsize=500, stretch='always')
        self.root_pane.add(self.side_panel, minsize=280)

        self.board_view.bind('<ButtonPress-1>', self._on_board_press)
        self.board_view.bind('<B1-Motion>', self._on_board_drag)
        self.board_view.bind('<ButtonRelease-1>', self._on_board_release)
        self._apply_shell_layout(initializing=True)
        self._populate_bundle_options()
        self._refresh_catalog()
        self._update_bundle_summary()
        self._refresh_top_control_strip()
        self._start_live_clock_refresh()
        self._bind_pause_hotkeys()
        self.root.protocol('WM_DELETE_WINDOW', self._request_shutdown)

    def _log_animation_event(self, event: str, **fields: object) -> None:
        parts = [f'{ANIMATION_LOG_PREFIX}_{event}']
        for key, value in fields.items():
            parts.append(f'{key}={value}')
        log_line('; '.join(parts), tag='timing')

    def run(self) -> None:
        self._schedule_after(0, self._initialize_app_shell)
        self.root.mainloop()

    def _initialize_app_shell(self) -> None:
        if not hasattr(self, "session"):
            remembered = self._remembered_bundle_path()
            if remembered and self._bundle_path_is_valid(remembered):
                self._load_selected_bundle(remembered)
                return
            self._show_bundle_picker('No valid corpus root was auto-discovered. Browse to the folder that contains bundle subfolders with manifest.json.')
            return
        repair_triggered = self._is_post_update_repair_launch()
        discovered_bundle = self._discover_and_bind_authoritative_corpus(force_repair=repair_triggered)
        if discovered_bundle and self._bundle_path_is_valid(discovered_bundle):
            self._load_selected_bundle(discovered_bundle)
            return
        self._show_bundle_picker(
            'No valid corpus root was auto-discovered. Browse to the folder that contains bundle subfolders with manifest.json. '
            'Until corpus is connected, Smart progression is blocked/degraded and fallback opponent routing is active.'
        )

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
                smart_profile_enabled=settings.smart_profile_enabled,
                training_mode=settings.training_mode,
                selected_smart_track=settings.selected_smart_track,
                selected_time_control_id=settings.selected_time_control_id,
                side_panel_visible=self.panel_visible,
                move_list_visible=self.move_list_visible,
                training_panel_visible_columns=settings.training_panel_visible_columns,
                last_bundle_path=self._remembered_bundle_path(),
                last_corpus_catalog_root=self._catalog_root_setting(),
                opponent_fallback_mode=settings.opponent_fallback_mode,
                last_seen_installed_app_version=settings.last_seen_installed_app_version,
                last_seen_installed_build_id=settings.last_seen_installed_build_id,
            )
        )

    def _set_last_bundle_path(self, bundle_path: str | None) -> None:
        settings = self.session.settings
        self.session.update_settings(
            TrainerSettings(
                good_moves_acceptable=settings.good_moves_acceptable,
                active_training_ply_depth=settings.active_training_ply_depth,
                smart_profile_enabled=settings.smart_profile_enabled,
                training_mode=settings.training_mode,
                selected_smart_track=settings.selected_smart_track,
                selected_time_control_id=settings.selected_time_control_id,
                side_panel_visible=self.panel_visible,
                move_list_visible=self.move_list_visible,
                training_panel_visible_columns=settings.training_panel_visible_columns,
                last_bundle_path=bundle_path,
                last_corpus_catalog_root=self._catalog_root_setting(),
                opponent_fallback_mode=settings.opponent_fallback_mode,
                last_seen_installed_app_version=settings.last_seen_installed_app_version,
                last_seen_installed_build_id=settings.last_seen_installed_build_id,
            )
        )

    def _catalog_root_setting(self) -> str | None:
        settings = self.session.settings_store.load(maximum_depth=self.session.max_supported_training_depth())
        return settings.last_corpus_catalog_root or DEFAULT_CORPUS_CATALOG_ROOT

    def _set_catalog_root_setting(self, catalog_root: str) -> None:
        settings = self.session.settings
        self.session.update_settings(
            TrainerSettings(
                good_moves_acceptable=settings.good_moves_acceptable,
                active_training_ply_depth=settings.active_training_ply_depth,
                smart_profile_enabled=settings.smart_profile_enabled,
                training_mode=settings.training_mode,
                selected_smart_track=settings.selected_smart_track,
                selected_time_control_id=settings.selected_time_control_id,
                side_panel_visible=self.panel_visible,
                move_list_visible=self.move_list_visible,
                training_panel_visible_columns=settings.training_panel_visible_columns,
                last_bundle_path=self._remembered_bundle_path(),
                last_corpus_catalog_root=catalog_root,
                opponent_fallback_mode=settings.opponent_fallback_mode,
                last_seen_installed_app_version=settings.last_seen_installed_app_version,
                last_seen_installed_build_id=settings.last_seen_installed_build_id,
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
        self._refresh_catalog()
        self._prefill_catalog_for_mode()
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

    def _browse_catalog_root(self) -> None:
        selected = filedialog.askdirectory(parent=self.root, title='Select corpus catalog root directory', initialdir=self._best_manual_browse_root(), mustexist=True)
        if selected:
            self.catalog_root_var.set(selected)
            self._refresh_catalog()

    def _refresh_catalog(self) -> None:
        root_value = self.catalog_root_var.get().strip() or self._catalog_root_setting()
        self.catalog_root_var.set(root_value)
        self._set_catalog_root_setting(root_value)
        self.catalog = discover_corpus_catalog(root_value)
        self.catalog_grouped = self.catalog.grouped()
        categories = list(self.catalog_grouped.keys())
        self.catalog_category_combo.configure(values=categories)
        if categories:
            self.catalog_category_var.set(categories[0])
        else:
            self.catalog_category_var.set('')
        self._refresh_catalog_time_controls()
        self.catalog_category_combo.bind('<<ComboboxSelected>>', self._on_catalog_category_selected)
        self.catalog_time_control_combo.bind('<<ComboboxSelected>>', self._on_catalog_time_control_selected)
        self.catalog_rating_band_combo.bind('<<ComboboxSelected>>', self._on_catalog_rating_band_selected)
        self.catalog_variant_combo.bind('<<ComboboxSelected>>', self._on_catalog_variant_selected)
        if self.catalog is not None and not self.catalog.entries:
            self.catalog_summary_var.set('No valid bundles were discovered in this catalog root yet.')
        self._update_catalog_summary()
        self._refresh_top_control_strip()

    def _all_discovered_time_controls(self) -> list[str]:
        controls = sorted({entry.time_control_id for entry in (self.catalog.entries if self.catalog else ())}, key=sort_key_time_control)
        return controls

    def _derive_track_label(self, time_control_id: str) -> str:
        grouped = self.catalog_grouped if isinstance(self.catalog_grouped, dict) else {}
        for category, controls in grouped.items():
            if time_control_id in controls:
                return category
        return 'Other'

    def _on_top_mode_toggle(self) -> None:
        self._apply_top_contract_change(reason='mode toggle')

    def _on_top_time_control_selected(self, _event=None) -> None:
        selected = self.top_time_control_var.get().strip()
        if not selected:
            return
        self._apply_top_contract_change(reason='time control changed')

    def _on_manual_contract_changed(self, _event=None) -> None:
        current_fallback_mode = self._selected_opponent_fallback_mode()
        if self.smart_mode_var.get() and current_fallback_mode == self.session.settings.opponent_fallback_mode:
            return
        self._apply_top_contract_change(reason='manual contract changed')

    def _selected_opponent_fallback_mode(self) -> str:
        var = getattr(self, "opponent_fallback_mode_var", None)
        if var is None or not hasattr(var, "get"):
            return self.session.settings.opponent_fallback_mode
        selected = str(var.get()).strip()
        return selected or self.session.settings.opponent_fallback_mode

    def _apply_top_contract_change(self, *, reason: str) -> None:
        settings = self.session.settings
        selected_time_control_id = self.top_time_control_var.get().strip() or settings.selected_time_control_id
        selected_track = self._derive_track_label(selected_time_control_id).lower()
        if selected_track not in {'rapid', 'blitz', 'bullet'}:
            selected_track = settings.selected_smart_track
        mode = 'smart_profile' if self.smart_mode_var.get() else 'manual'
        updated = self.session.update_settings(
            TrainerSettings(
                good_moves_acceptable=self.manual_good_var.get().strip().lower() == 'yes' if mode == 'manual' else settings.good_moves_acceptable,
                active_training_ply_depth=int(self.manual_depth_var.get()) if mode == 'manual' else settings.active_training_ply_depth,
                smart_profile_enabled=mode == 'smart_profile',
                training_mode=mode,
                selected_smart_track=selected_track,
                selected_time_control_id=selected_time_control_id,
                side_panel_visible=self.panel_visible,
                move_list_visible=self.move_list_visible,
                training_panel_visible_columns=settings.training_panel_visible_columns,
                last_bundle_path=self._remembered_bundle_path(),
                last_corpus_catalog_root=self._catalog_root_setting(),
                opponent_fallback_mode=self._selected_opponent_fallback_mode(),
                last_seen_installed_app_version=settings.last_seen_installed_app_version,
                last_seen_installed_build_id=settings.last_seen_installed_build_id,
            )
        )
        self.top_time_control_var.set(updated.selected_time_control_id)
        if hasattr(self, "opponent_fallback_mode_var"):
            self.opponent_fallback_mode_var.set(updated.opponent_fallback_mode)
        self._refresh_top_control_strip()
        resolved_bundle_path, blocked_message = self._resolve_bundle_for_top_contract(updated)
        remembered_path = self._remembered_bundle_path()
        if resolved_bundle_path:
            if self._bundle_token(remembered_path) == self._bundle_token(resolved_bundle_path):
                self._refresh_supporting_surfaces()
                self._prepend_recent_status(f'Training contract updated ({reason}); resolved bundle already active.')
                return
            self._prepend_recent_status(
                f'Training contract updated ({reason}); loading resolved bundle {Path(resolved_bundle_path).name}.'
            )
            self._load_selected_bundle(resolved_bundle_path)
            return
        self._refresh_supporting_surfaces()
        self._prepend_recent_status(blocked_message or f'Training contract updated ({reason}); corpus unchanged.')

    def _resolve_bundle_for_top_contract(self, settings: TrainerSettings) -> tuple[str | None, str | None]:
        if settings.training_mode == 'smart_profile':
            resolution = self.session.smart_profile.resolve_expected_bundle(settings.last_corpus_catalog_root)
            if resolution.resolved_entry is not None:
                return str(resolution.resolved_entry.bundle_dir), None
            return None, (
                f'Expected bundle unavailable for {resolution.category_id} / {resolution.expected_rating_band}; '
                'ladder blocked until matching bundle is available.'
            )
        entry = self._resolve_manual_bundle_entry(
            time_control_id=settings.selected_time_control_id,
            rating_band=self.manual_elo_var.get().strip(),
        )
        if entry is not None:
            return str(entry.bundle_dir), None
        return None, (
            f'No discovered bundle matches {settings.selected_time_control_id} / {self.manual_elo_var.get().strip() or "n/a"}; '
            'contract fields updated, corpus unchanged.'
        )

    def _resolve_manual_bundle_entry(self, *, time_control_id: str, rating_band: str):
        if not time_control_id or not rating_band:
            return None
        if self.catalog is None:
            self.catalog = discover_corpus_catalog(self._catalog_root_setting())
            self.catalog_grouped = self.catalog.grouped()
        grouped = self.catalog.grouped()
        for category in grouped.values():
            by_band = category.get(time_control_id, {})
            variants = by_band.get(rating_band)
            if variants:
                return variants[0]
        return None

    def _bundle_token(self, bundle_path: str | Path | None) -> str | None:
        if not bundle_path:
            return None
        return str(Path(bundle_path).expanduser())

    def _prepend_recent_status(self, message: str) -> None:
        if not message.strip():
            return
        if not hasattr(self, 'recent_var'):
            return
        current = self.recent_var.get()
        if current:
            self.recent_var.set(message + '\n\n' + current)
            return
        self.recent_var.set(message)

    def _refresh_top_control_strip(self) -> None:
        controls = self._all_discovered_time_controls()
        if not controls:
            controls = [self.session.settings.selected_time_control_id]
        if hasattr(self, "opponent_fallback_mode_var"):
            self.opponent_fallback_mode_var.set(self.session.settings.opponent_fallback_mode)
        self.top_time_control_combo.configure(values=controls)
        if self.top_time_control_var.get().strip() not in controls:
            self.top_time_control_var.set(controls[0])
        track = self._derive_track_label(self.top_time_control_var.get().strip())
        self.top_track_var.set(track)
        status = self.session.smart_profile_status()
        self.smart_mode_var.set(status.active)
        bands = sorted(
            {entry.target_rating_band for entry in (self.catalog.entries if self.catalog else ()) if entry.time_control_id == self.top_time_control_var.get().strip()},
            key=sort_key_rating_band,
        )
        self.top_elo_combo.configure(values=bands)
        if bands and self.manual_elo_var.get() not in bands:
            self.manual_elo_var.set(bands[0])
        max_depth = self.session.max_supported_training_depth()
        self.top_depth_combo.configure(values=list(range(2, max_depth + 1)))
        if status.active:
            self.top_level_var.set(f"L{status.level}" if status.level is not None else '—')
            self.top_elo_var.set(status.expected_rating_band or '—')
            self.top_depth_var.set(str(status.contract_turns) if status.contract_turns is not None else '—')
            self.top_good_var.set('Yes' if status.contract_good_accepted else 'No')
            self.top_level_label.grid()
            self.top_elo_combo.grid_remove()
            self.top_depth_combo.grid_remove()
            self.top_good_combo.grid_remove()
            self.top_elo_label.grid()
            self.top_depth_label.grid()
            self.top_good_label.grid()
        else:
            self.top_level_var.set('')
            self.top_level_label.grid_remove()
            self.top_elo_label.grid_remove()
            self.top_depth_label.grid_remove()
            self.top_good_label.grid_remove()
            self.top_elo_combo.grid()
            self.top_depth_combo.grid()
            self.top_good_combo.grid()

    def _on_catalog_category_selected(self, _event=None) -> None:
        self._refresh_catalog_time_controls()

    def _on_catalog_time_control_selected(self, _event=None) -> None:
        self._refresh_catalog_rating_bands()

    def _on_catalog_rating_band_selected(self, _event=None) -> None:
        self._refresh_catalog_variants()

    def _on_catalog_variant_selected(self, _event=None) -> None:
        self._update_catalog_summary()

    def _refresh_catalog_time_controls(self) -> None:
        selected_category = self.catalog_category_var.get().strip()
        controls = list(self.catalog_grouped.get(selected_category, {}).keys())
        controls.sort(key=sort_key_time_control)
        self.catalog_time_control_combo.configure(values=controls)
        self.catalog_time_control_var.set(controls[0] if controls else '')
        self._refresh_catalog_rating_bands()

    def _refresh_catalog_rating_bands(self) -> None:
        selected_category = self.catalog_category_var.get().strip()
        selected_control = self.catalog_time_control_var.get().strip()
        bands = list(self.catalog_grouped.get(selected_category, {}).get(selected_control, {}).keys())
        bands.sort(key=sort_key_rating_band)
        self.catalog_rating_band_combo.configure(values=bands)
        self.catalog_rating_band_var.set(bands[0] if bands else '')
        self._refresh_catalog_variants()

    def _refresh_catalog_variants(self) -> None:
        selected_category = self.catalog_category_var.get().strip()
        selected_control = self.catalog_time_control_var.get().strip()
        selected_band = self.catalog_rating_band_var.get().strip()
        variants = list(self.catalog_grouped.get(selected_category, {}).get(selected_control, {}).get(selected_band, ()))
        self.catalog_leaf_variants = variants
        labels = [bundle_variant_label(entry) for entry in variants]
        self.catalog_variant_combo.configure(values=labels)
        self.catalog_variant_var.set(labels[0] if labels else '')
        self._update_catalog_summary()

    def _selected_catalog_entry(self):
        label = self.catalog_variant_var.get().strip()
        if not self.catalog_leaf_variants:
            return None
        labels = [bundle_variant_label(entry) for entry in self.catalog_leaf_variants]
        if label in labels:
            return self.catalog_leaf_variants[labels.index(label)]
        return self.catalog_leaf_variants[0]

    def _update_catalog_summary(self) -> None:
        status = self.session.smart_profile_status()
        mode_header = 'Smart Profile ladder active' if status.active else 'Manual mode active'
        mode_lines = [f'Mode: {"Smart Profile" if status.active else "Manual"}']
        if status.active:
            mode_lines.extend(
                [
                    f'Selected track: {status.track_id or "n/a"}',
                    f'Expected bundle: {status.expected_bundle_summary}',
                    f'Success streak: {status.consecutive_eligible_successes}',
                    f'Failure streak: {status.consecutive_eligible_failures}',
                    f'Eligible now: {"yes" if status.eligible_now else "no"} ({status.eligibility_reason})',
                ]
            )
        else:
            mode_lines.append('Smart Profile ladder counting is inactive in Manual mode.')

        entry = self._selected_catalog_entry()
        if entry is None:
            self.catalog_summary_var.set(
                f'{mode_header} | '
                + ' | '.join(mode_lines)
                + ' | Select a discovered category, time control, and rating band.'
            )
            return
        retained = entry.retained_ply_depth if entry.retained_ply_depth is not None else 'n/a'
        policy = entry.rating_policy or 'n/a'
        overlay = 'yes' if entry.timing_overlay_exists else 'no'
        active_bundle = self._remembered_bundle_path() or 'fallback / none'
        self.catalog_summary_var.set(
            f'{mode_header} | '
            + ' | '.join(mode_lines)
            + f' | Active bundle: {active_bundle} | Control: {entry.time_control_id} | Band: {entry.target_rating_band} | Retained depth: {retained} | '
            f'Rating policy: {policy} | Timing-conditioned metadata: {overlay} | Bundle path: {entry.bundle_dir}'
        )

    def _prefill_catalog_for_mode(self) -> None:
        status = self.session.smart_profile_status()
        if not status.active:
            self._update_catalog_summary()
            return
        expected_bundle_path = self.session.smart_profile_expected_bundle_path()
        expected_control = (status.category_id or '').strip()
        expected_band = self._extract_expected_band(status.expected_bundle_summary)
        expected_category = ''
        if expected_control:
            for category, controls in self.catalog_grouped.items():
                if expected_control in controls:
                    expected_category = category
                    break
        if expected_category:
            self.catalog_category_var.set(expected_category)
            self._refresh_catalog_time_controls()
        if expected_control and expected_control in self.catalog_time_control_combo.cget('values'):
            self.catalog_time_control_var.set(expected_control)
            self._refresh_catalog_rating_bands()
        if expected_band and expected_band in self.catalog_rating_band_combo.cget('values'):
            self.catalog_rating_band_var.set(expected_band)
            self._refresh_catalog_variants()
        if expected_bundle_path:
            expected_path_token = str(Path(expected_bundle_path))
            for variant in self.catalog_leaf_variants:
                variant_path_token = str(Path(str(variant.bundle_dir)))
                if variant_path_token == expected_path_token:
                    self.catalog_variant_var.set(bundle_variant_label(variant))
                    break
        self._update_catalog_summary()

    def _extract_expected_band(self, expected_summary: str) -> str:
        if not expected_summary or "Expected:" not in expected_summary:
            return ""
        try:
            body = expected_summary.split("Expected:", 1)[1].strip()
            before_arrow = body.split("->", 1)[0].strip()
            parts = [part.strip() for part in before_arrow.split("/", 1)]
            if len(parts) < 2:
                return ""
            return parts[1]
        except Exception:
            return ""

    def _load_expected_smart_profile_bundle(self) -> None:
        status = self.session.smart_profile_status()
        if not status.active:
            messagebox.showinfo('Smart Profile', 'Manual mode active. Switch to Smart Profile mode to load the expected ladder bundle.', parent=self.root)
            return
        expected = self.session.smart_profile_expected_bundle_path()
        if not expected:
            messagebox.showerror('Smart Profile', f'Expected bundle missing. {status.expected_bundle_summary}', parent=self.root)
            return
        self._load_selected_bundle(expected)

    def _apply_catalog_selection(self) -> None:
        entry = self._selected_catalog_entry()
        if entry is None:
            messagebox.showerror('Corpus catalog', 'Choose a valid catalog entry before loading.', parent=self.root)
            return
        self.bundle_path_var.set(str(entry.bundle_dir))
        self._load_selected_bundle(str(entry.bundle_dir))

    def _on_bundle_combo_selected(self, _event=None) -> None:
        index = self.bundle_combobox.current()
        if 0 <= index < len(self.available_bundles):
            self.bundle_path_var.set(str(self.available_bundles[index][1]))

    def _browse_bundle_path(self) -> None:
        selected = filedialog.askdirectory(parent=self.root, title='Select corpus bundle directory', initialdir=self._best_manual_browse_root(), mustexist=True)
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
                opponent_fallback_mode=self._selected_opponent_fallback_mode(),
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
        self.inspector.set_visible_columns(self.session.settings.training_panel_visible_columns)
        self._set_last_bundle_path(payload['bundle_path'])
        self._update_bundle_summary()
        self._apply_shell_layout(initializing=True)
        self._refresh_top_control_strip()
        self._hide_bundle_picker()
        self.selected_square = None
        self.pending_restart = False
        self._deferred_outcome_view = None
        self._clear_board_transients(reason='bundle_load')
        self._refresh_view()
        self._apply_post_boot_live_gate()

    def _update_bundle_summary(self) -> None:
        bundle_path = self._remembered_bundle_path()
        if bundle_path and self._bundle_path_is_valid(bundle_path):
            compatibility = inspect_corpus_bundle(Path(bundle_path))
            self.bundle_status_var.set('Corpus status: connected and active')
            self.bundle_detail_var.set(compatibility.detail)
            return
        if bundle_path:
            self.bundle_status_var.set('Corpus status: missing / fallback active')
            self.bundle_detail_var.set(f'{bundle_path} is missing or no longer valid. Smart progression is blocked or degraded until a valid bundle is connected.')
            return
        self.bundle_status_var.set('Corpus status: missing / fallback active')
        self.bundle_detail_var.set('No bundle selected. The trainer falls back to Stockfish/random legal moves and Smart progression is blocked or degraded.')

    def _open_bundle_picker(self):
        self._show_bundle_picker('Choose a corpus bundle for this session. The last valid bundle will be reused on future launches.')

    def _installed_manifest_payload(self) -> dict[str, object] | None:
        manifest_path = self.session.runtime_context.runtime_paths.app_state_root / "installed_app_manifest.json"
        if not manifest_path.exists():
            return None
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _is_post_update_repair_launch(self) -> bool:
        if not hasattr(self, "session"):
            return False
        settings = self.session.settings
        payload = self._installed_manifest_payload()
        if not isinstance(payload, dict):
            return False
        installed_version = str(payload.get("app_version") or "").strip() or None
        installed_build_id = str(payload.get("build_id") or "").strip() or None
        changed = (
            installed_version != settings.last_seen_installed_app_version
            or installed_build_id != settings.last_seen_installed_build_id
        )
        if changed:
            log_line(
                f"CORPUS_DISCOVERY_POST_UPDATE_REPAIR app_version={installed_version or 'unknown'} build_id={installed_build_id or 'unknown'}",
                tag="startup",
            )
            self.session.update_settings(
                TrainerSettings(
                    good_moves_acceptable=settings.good_moves_acceptable,
                    active_training_ply_depth=settings.active_training_ply_depth,
                    smart_profile_enabled=settings.smart_profile_enabled,
                    training_mode=settings.training_mode,
                    selected_smart_track=settings.selected_smart_track,
                    selected_time_control_id=settings.selected_time_control_id,
                    side_panel_visible=settings.side_panel_visible,
                    move_list_visible=settings.move_list_visible,
                    training_panel_visible_columns=settings.training_panel_visible_columns,
                    last_bundle_path=settings.last_bundle_path,
                    last_corpus_catalog_root=settings.last_corpus_catalog_root,
                    opponent_fallback_mode=settings.opponent_fallback_mode,
                    last_seen_installed_app_version=installed_version,
                    last_seen_installed_build_id=installed_build_id,
                )
            )
        return changed

    def _discover_and_bind_authoritative_corpus(self, *, force_repair: bool) -> str | None:
        del force_repair
        settings = self.session.settings
        paths = self.session.runtime_context.runtime_paths
        remembered_catalog_root = settings.last_corpus_catalog_root
        canonical_catalog_root = str(paths.corpus_bundle_root)
        runtime_catalog_root = self._runtime_config_catalog_root()
        candidates: list[tuple[str, str | None]] = [
            ("install-owned installed_content_manifest", canonical_catalog_root if (paths.app_state_root / "installed_content_manifest.json").exists() else None),
            ("canonical LocalAppData corpus root", canonical_catalog_root),
            ("remembered corpus catalog root", remembered_catalog_root),
            ("app-owned runtime config corpus root", runtime_catalog_root),
        ]
        valid: dict[str, tuple[str, int]] = {}
        for source, candidate_root in candidates:
            ok, reason, entries = self._validate_catalog_root(candidate_root)
            log_line(f"CORPUS_DISCOVERY_CANDIDATE source={source} root={candidate_root or 'unset'} valid={ok} reason={reason}", tag="startup")
            if ok and candidate_root:
                valid[str(Path(candidate_root).expanduser())] = (source, entries)
        if not valid:
            log_line("CORPUS_DISCOVERY_FALLBACK no authoritative root validated; manual browse required", tag="startup")
            return None
        remembered_token = str(Path(remembered_catalog_root).expanduser()) if remembered_catalog_root else None
        canonical_token = str(Path(canonical_catalog_root).expanduser())
        winning_root = remembered_token if remembered_token and remembered_token in valid else None
        if winning_root is None:
            winning_root = canonical_token if canonical_token in valid else sorted(valid.keys())[0]
        winner_source, entry_count = valid[winning_root]
        log_line(f"CORPUS_DISCOVERY_WIN root={winning_root} source={winner_source} entries={entry_count}", tag="startup")
        self.catalog_root_var.set(winning_root)
        self._set_catalog_root_setting(winning_root)
        self._refresh_catalog()
        expected = self.session.smart_profile_expected_bundle_path()
        if expected and self._bundle_path_is_valid(expected):
            log_line(f"CORPUS_DISCOVERY_EXPECTED_BUNDLE winner={expected}", tag="startup")
            return expected
        if self.catalog and self.catalog.entries:
            winner_bundle = str(self.catalog.entries[0].bundle_dir)
            log_line(f"CORPUS_DISCOVERY_DEFAULT_BUNDLE winner={winner_bundle}", tag="startup")
            return winner_bundle
        log_line("CORPUS_DISCOVERY_FALLBACK root valid but no bundle resolved", tag="startup")
        return None

    def _runtime_config_catalog_root(self) -> str | None:
        runtime_config_path = self.session.runtime_context.runtime_paths.runtime_config_path
        if runtime_config_path is None or not runtime_config_path.exists():
            return None
        try:
            payload = json.loads(runtime_config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        bundle_dir = payload.get("corpus_bundle_dir")
        if not isinstance(bundle_dir, str) or not bundle_dir.strip():
            return None
        return str(Path(bundle_dir).expanduser().parent)

    def _validate_catalog_root(self, root: str | None) -> tuple[bool, str, int]:
        if root is None or not str(root).strip():
            return False, "unset", 0
        path = Path(root).expanduser()
        if not path.exists() or not path.is_dir():
            return False, "missing_or_not_directory", 0
        catalog = discover_corpus_catalog(path)
        if not catalog.entries:
            return False, "no_valid_manifest_driven_bundles", 0
        return True, "catalog_manifest_validation_passed", len(catalog.entries)

    def _best_manual_browse_root(self) -> str:
        candidates = (
            self.catalog_root_var.get().strip(),
            self.session.settings.last_corpus_catalog_root or "",
            str(self.session.runtime_context.runtime_paths.corpus_bundle_root),
            str(self.session.runtime_context.runtime_paths.content_root),
            str(Path.home()),
        )
        for candidate in candidates:
            if candidate and Path(candidate).expanduser().is_dir():
                return str(Path(candidate).expanduser())
        return str(Path.home())

    def _open_profiles(self):
        ProfileDialog(
            self.root,
            self.session,
            self._refresh_supporting_surfaces,
            switch_callback=self._refresh_after_profile_switch,
            reset_callback=self._reset_profile_from_dialog,
        ).open()

    def _refresh_after_profile_switch(self) -> None:
        self._reconcile_smart_profile_state(reason='profile_switch')
        self._refresh_supporting_surfaces()

    def _reset_profile_from_dialog(self, profile_id: str) -> None:
        is_active_profile = self.session.active_profile_id == profile_id
        log_line(
            f'GUI_PROFILE_RESET_BEGIN profile_id={profile_id} is_active={is_active_profile}',
            tag='smart_profile',
        )
        self.session.reset_profile(profile_id)
        log_line(
            f'GUI_PROFILE_RESET_SMART_STATE_RESET profile_id={profile_id}',
            tag='smart_profile',
        )
        if is_active_profile:
            reason = 'profile_reset'
            log_line(
                f'GUI_PROFILE_RESET_ACTIVE_RECONCILE profile_id={profile_id} reason={reason}',
                tag='smart_profile',
            )
            self._reconcile_smart_profile_state(reason=reason)
        self._refresh_supporting_surfaces()

    def _start_game(self, loading_message: str | None = None):
        self._cancel_pending_opponent_callback()
        self._destroy_pause_surface()
        self.paused = False
        self.pause_started_at_monotonic = None
        if loading_message is None:
            self.session.start_new_game()
            self.selected_square = None
            self.pending_restart = False
            self._deferred_outcome_view = None
            self._clear_board_transients(reason='new_game_start')
            self._refresh_view()
            self._apply_post_boot_live_gate()
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
        self._deferred_outcome_view = None
        self._clear_board_transients(reason='new_game_started_worker')
        self._refresh_view()
        self._apply_post_boot_live_gate()

    def _apply_post_boot_live_gate(self) -> None:
        timed = getattr(self.session, 'timed_state', None) is not None
        opponent_starts = getattr(self.session, 'state', None) == SessionState.OPPONENT_TURN
        first_boot_ready_required = getattr(self, 'first_boot_ready_required', True)
        session_state = getattr(getattr(self.session, 'state', None), 'value', str(getattr(self.session, 'state', None)))
        log_line(
            'GUI_READY_GATE_EVALUATED '
            f'timed={str(timed).lower()} '
            f'opponent_starts={str(opponent_starts).lower()} '
            f'first_boot_ready_required={str(first_boot_ready_required).lower()} '
            f'session_state={session_state}',
            tag='timing',
        )
        if timed and opponent_starts and first_boot_ready_required:
            log_line('GUI_READY_GATE_PATH action=show_overlay', tag='timing')
            self._show_ready_overlay()
            return
        if first_boot_ready_required:
            self.first_boot_ready_required = False
            log_line('GUI_READY_GATE_PATH action=consume_without_overlay', tag='timing')
        if opponent_starts and not self.paused and not self.ready_overlay_visible:
            log_line('GUI_READY_GATE_PATH action=schedule_opponent', tag='timing')
            self._schedule_pending_opponent_commit()
            return
        log_line('GUI_READY_GATE_PATH action=no_action', tag='timing')

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
        if due == 0 and boosted == 0 and extreme == 0:
            return 'Review queue: clear'
        return f'Review queue: {due} due ({boosted} boosted, {extreme} urgent)'

    def _build_top_summary_row(self, *, profile_name: str, due: int, boosted: int, extreme: int) -> str:
        status = self.session.smart_profile_status()
        if not status.active:
            return f'Profile: {profile_name}'
        level = f"L{status.level}" if status.level is not None else '—'
        return (
            f'Profile: {profile_name}   '
            f'Level: {level}   '
            f'Success streak: {status.consecutive_eligible_successes}   '
            f'Failure streak: {status.consecutive_eligible_failures}   '
            f'{due} due ({boosted} boosted, {extreme} urgent)'
        )

    def _build_routing_summary(self, routing: str, explain: str) -> str:
        del explain
        labels = {
            'not_started': 'Session route: preparing',
            'ordinary_corpus_play': 'Session route: corpus training',
            'srs_due_review': 'Session route: spaced review due',
            'scheduled_review': 'Session route: review training',
            'boosted_review': 'Session route: boosted review',
            'extreme_urgency_review': 'Session route: urgent review',
            'immediate_retry': 'Session route: immediate retry',
            'manual_target': 'Session route: manual target',
        }
        return labels.get(routing, 'Session route: mixed training')

    def _build_compact_bundle_summary(self) -> str:
        status = self.session.corpus_summary_text()
        if ' | Opponent timing:' in status:
            status = status.split(' | Opponent timing:')[0]
        return status

    def _training_depth_summary(self) -> str:
        retained_ply_depth = self.session.bundle_retained_ply_depth()
        cap = self.session.max_supported_training_depth()
        retained_text = f' | Bundle max: {retained_ply_depth // 2} player moves' if retained_ply_depth is not None else ''
        return f'Training depth: {self.session.required_player_moves} player moves | Good accepted: {"yes" if self.session.config.good_moves_acceptable else "no"} | App max: {cap} player moves{retained_text}'

    def _smart_profile_summary_text(self) -> str:
        status = self.session.smart_profile_status()
        if not status.active:
            return 'Manual mode active | Smart Profile ladder counting inactive.'
        track = f'{status.track_id}/{status.category_id}' if status.track_id and status.category_id else 'unsupported'
        level = f'L{status.level}' if status.level is not None else 'n/a'
        eligible = 'yes' if status.eligible_now else 'no'
        return (
            f'Smart Profile ladder active | Track: {track} | Level: {level} | '
            f'Success streak: {status.consecutive_eligible_successes} | Failure streak: {status.consecutive_eligible_failures} | '
            f'Eligible now: {eligible} ({status.eligibility_reason}) | Expected bundle: {status.expected_bundle_summary}'
        )

    def _build_recent_status_text(self, routing_summary: str) -> str:
        return '\n'.join(
            [
                self._training_depth_summary(),
                self._smart_profile_summary_text(),
                routing_summary,
                'Recent status: Ready for next move.',
            ]
        )

    def _refresh_supporting_surfaces(self):
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        profile_name = self.session.review_storage.load_profile_meta(self.session.active_profile_id).display_name
        due = sum(1 for item in items if item.srs_next_due_at_utc <= item.updated_at_utc)
        boosted = sum(1 for item in items if item.urgency_tier == 'boosted_review')
        extreme = sum(1 for item in items if item.urgency_tier == 'extreme_urgency')
        routing = self.session.current_routing.routing_source if self.session.current_routing else 'not_started'
        routing_summary = self._build_routing_summary(routing, '')
        self.top_summary_var.set(self._build_top_summary_row(profile_name=profile_name, due=due, boosted=boosted, extreme=extreme))
        self.recent_var.set(self._build_recent_status_text(routing_summary))
        self.inspector.refresh()
        view = self.session.get_view()
        self.move_list_panel.update_opening_name(view.opening_name)
        self.move_list_panel.update_moves(view.move_history)
        board = chess.Board(view.board_fen)
        self._refresh_clock_display(board=board, view=view)
        self._update_bundle_summary()

    def _refresh_clock_display(self, board: chess.Board | None = None, view=None) -> None:
        resolved_view = self.session.get_view() if view is None else view
        resolved_board = chess.Board(resolved_view.board_fen) if board is None else board
        displayed_clock_seconds = getattr(self.session, 'displayed_clock_seconds', None)
        if callable(displayed_clock_seconds):
            frozen_now = self._clock_suspend_started_at_monotonic
            if frozen_now is not None and resolved_view.state == SessionState.PLAYER_TURN:
                white_clock_seconds, black_clock_seconds = displayed_clock_seconds(now=frozen_now)
            else:
                white_clock_seconds, black_clock_seconds = displayed_clock_seconds()
        else:
            timed_state = getattr(self.session, 'timed_state', None)
            white_clock_seconds = timed_state.white_remaining_ms / 1000.0 if timed_state is not None else None
            black_clock_seconds = timed_state.black_remaining_ms / 1000.0 if timed_state is not None else None
        top_is_white = resolved_view.player_color == chess.BLACK
        self.top_captured_panel.update_board(
            resolved_board,
            player_color=resolved_view.player_color,
            near_side=False,
            clock_seconds=white_clock_seconds if top_is_white else black_clock_seconds,
        )
        self.bottom_captured_panel.update_board(
            resolved_board,
            player_color=resolved_view.player_color,
            near_side=True,
            clock_seconds=white_clock_seconds if resolved_view.player_color == chess.WHITE else black_clock_seconds,
        )

    def _open_options(self):
        window = tk.Toplevel(self.root)
        self._child_windows.append(window)
        window.title('Trainer Options')
        window.transient(self.root)
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill='both', expand=True)
        panel_var = tk.BooleanVar(value=self.panel_visible)
        move_list_var = tk.BooleanVar(value=self.move_list_visible)
        visible_columns = set(self.session.settings.training_panel_visible_columns or DEFAULT_TRAINING_PANEL_COLUMNS)
        column_vars = {
            column: tk.BooleanVar(value=column in visible_columns)
            for column in self.inspector.columns
        }
        ttk.Checkbutton(frame, text='Show training/review panel by default', variable=panel_var).pack(anchor='w')
        ttk.Checkbutton(frame, text='Show move list by default', variable=move_list_var).pack(anchor='w', pady=(0, 8))
        ttk.Label(frame, text='Training panel columns', justify='left').pack(anchor='w')
        columns_frame = ttk.Frame(frame)
        columns_frame.pack(fill='x', pady=(0, 8))
        for index, column in enumerate(self.inspector.columns):
            ttk.Checkbutton(
                columns_frame,
                text=self.inspector.column_labels[column],
                variable=column_vars[column],
            ).grid(row=index // 3, column=index % 3, sticky='w', padx=(0, 12), pady=2)

        def save():
            self.panel_visible = panel_var.get()
            self.move_list_visible = move_list_var.get()
            selected_columns = tuple(
                column for column in self.inspector.columns if column_vars[column].get()
            ) or DEFAULT_TRAINING_PANEL_COLUMNS
            self.session.update_settings(
                TrainerSettings(
                    good_moves_acceptable=self.session.settings.good_moves_acceptable,
                    active_training_ply_depth=self.session.settings.active_training_ply_depth,
                    smart_profile_enabled=self.session.settings.smart_profile_enabled,
                    training_mode=self.session.settings.training_mode,
                    selected_smart_track=self.session.settings.selected_smart_track,
                    selected_time_control_id=self.session.settings.selected_time_control_id,
                    side_panel_visible=self.panel_visible,
                    move_list_visible=self.move_list_visible,
                    training_panel_visible_columns=selected_columns,
                    last_bundle_path=self._remembered_bundle_path(),
                    last_corpus_catalog_root=self._catalog_root_setting(),
                    opponent_fallback_mode=self.session.settings.opponent_fallback_mode,
                    last_seen_installed_app_version=self.session.settings.last_seen_installed_app_version,
                    last_seen_installed_build_id=self.session.settings.last_seen_installed_build_id,
                )
            )
            window.destroy()
            self.inspector.set_visible_columns(selected_columns)
            self._apply_shell_layout(initializing=True)
            self._refresh_supporting_surfaces()

        ttk.Button(frame, text='Save', command=save).pack(side='left')
        ttk.Button(frame, text='Cancel', command=window.destroy).pack(side='left', padx=(8, 0))

    def _bind_pause_hotkeys(self) -> None:
        self.root.bind_all('<KeyPress-p>', lambda event: self._on_pause_hotkey(event, source='key_p'))
        self.root.bind_all('<Escape>', lambda event: self._on_pause_hotkey(event, source='key_escape'))

    def _ensure_live_flow_state(self) -> None:
        if not hasattr(self, 'paused'):
            self.paused = False
        if not hasattr(self, 'ready_overlay_visible'):
            self.ready_overlay_visible = False
        if not hasattr(self, 'first_boot_ready_required'):
            self.first_boot_ready_required = True
        if not hasattr(self, 'pause_started_at_monotonic'):
            self.pause_started_at_monotonic = None
        if not hasattr(self, '_clock_suspend_started_at_monotonic'):
            self._clock_suspend_started_at_monotonic = None
        if not hasattr(self, '_frozen_pending_opponent_remaining_delay_seconds'):
            self._frozen_pending_opponent_remaining_delay_seconds = None
        if not hasattr(self, '_pending_opponent_scheduled_at_monotonic'):
            self._pending_opponent_scheduled_at_monotonic = None
        if not hasattr(self, '_pending_opponent_after_handle'):
            self._pending_opponent_after_handle = None
        if not hasattr(self, '_pause_overlay_window'):
            self._pause_overlay_window = None
        if not hasattr(self, '_ready_overlay_frame'):
            self._ready_overlay_frame = None

    def _session_side_to_move(self) -> chess.Color:
        board = getattr(getattr(self, 'session', None), 'board', None)
        if board is None:
            return chess.WHITE
        turn_attr = getattr(board, 'turn', None)
        if callable(turn_attr):
            return turn_attr()
        if isinstance(turn_attr, bool):
            return turn_attr
        nested_board = getattr(board, 'board', None)
        if nested_board is not None and isinstance(getattr(nested_board, 'turn', None), bool):
            return nested_board.turn
        return chess.WHITE

    def _on_pause_hotkey(self, event: tk.Event, *, source: str) -> str | None:
        self._ensure_live_flow_state()
        widget = getattr(event, 'widget', None)
        if isinstance(widget, (tk.Entry, tk.Text, ttk.Entry, ttk.Combobox)):
            return None
        if self._loading_job_active or getattr(self, '_is_shutting_down', False):
            return None
        if self.session.state not in {SessionState.PLAYER_TURN, SessionState.OPPONENT_TURN}:
            return None
        self._open_pause_surface(source=source)
        return 'break'

    def _open_pause_surface(self, *, source: str) -> None:
        self._ensure_live_flow_state()
        if self.paused:
            if self._pause_overlay_window is not None and self._pause_overlay_window.winfo_exists():
                self._pause_overlay_window.lift()
                self._pause_overlay_window.focus_set()
            return
        self.paused = True
        self.pause_started_at_monotonic = monotonic()
        self._enter_time_freeze(reason='pause')
        log_line(f'GUI_PAUSE_OPENED source={source}', tag='timing')
        overlay = tk.Toplevel(self.root)
        self._pause_overlay_window = overlay
        self._child_windows.append(overlay)
        overlay.title('Paused')
        overlay.transient(self.root)
        overlay.configure(bg=PAUSE_OVERLAY_BG)
        overlay.resizable(False, False)
        frame = ttk.Frame(overlay, padding=16)
        frame.pack(fill='both', expand=True)
        ttk.Label(frame, text='Paused', font=('TkDefaultFont', 14, 'bold')).pack(anchor='center', pady=(0, 4))
        ttk.Label(frame, text='Game time is suspended.', justify='center').pack(anchor='center', pady=(0, 12))
        ttk.Button(frame, text='Resume', command=self._resume_from_pause).pack(fill='x')
        ttk.Button(frame, text='Options', command=self._open_options).pack(fill='x', pady=(8, 0))
        ttk.Button(frame, text='Corpus Selection', command=self._open_bundle_picker).pack(fill='x', pady=(8, 0))
        ttk.Button(frame, text='Developer', command=self._open_dev_console).pack(fill='x', pady=(8, 0))
        ttk.Button(frame, text='Profiles', command=self._open_profiles).pack(fill='x', pady=(8, 0))
        ttk.Button(frame, text='Exit Game', command=self._exit_active_game_from_pause).pack(fill='x', pady=(8, 0))
        overlay.protocol('WM_DELETE_WINDOW', self._resume_from_pause)

    def _resume_from_pause(self) -> None:
        self._ensure_live_flow_state()
        if not self.paused:
            return
        pause_started = self.pause_started_at_monotonic
        paused_for = max(0.0, monotonic() - pause_started) if pause_started is not None else 0.0
        self.paused = False
        self.pause_started_at_monotonic = None
        self._leave_time_freeze(reason='pause', frozen_seconds=paused_for)
        self._destroy_pause_surface()
        log_line('GUI_PAUSE_RESUMED', tag='timing')
        self._refresh_view()

    def _destroy_pause_surface(self) -> None:
        self._ensure_live_flow_state()
        overlay = self._pause_overlay_window
        self._pause_overlay_window = None
        if overlay is None:
            return
        self._destroy_window(overlay)

    def _exit_active_game_from_pause(self) -> None:
        self._resume_from_pause()
        self._cancel_pending_opponent_callback()
        self.session.cancel_pending_opponent_action()
        self.session.state = SessionState.IDLE
        self.session._player_turn_started_at = None
        self.pending_restart = False
        self.selected_square = None
        self._clear_board_transients(reason='exit_game')
        self._refresh_view(transient_status='Game exited. Start drill when ready.')

    def _show_ready_overlay(self) -> None:
        self._ensure_live_flow_state()
        if self.ready_overlay_visible:
            return
        self.ready_overlay_visible = True
        self._enter_time_freeze(reason='ready_overlay')
        log_line('GUI_READY_OVERLAY_SHOWN reason=first_boot_opponent_start', tag='timing')
        frame = ttk.Frame(self.main_region, padding=16, style='Card.TFrame')
        frame.place(relx=0.5, rely=0.5, anchor='center')
        ttk.Label(frame, text='Ready?', font=('TkDefaultFont', 14, 'bold')).pack(anchor='center', pady=(0, 4))
        ttk.Label(frame, text='Opponent starts. Click Begin to start live play.').pack(anchor='center', pady=(0, 12))
        tk.Button(
            frame,
            text='Begin',
            command=self._acknowledge_ready_overlay,
        ).pack(fill='x')
        self._ready_overlay_frame = frame

    def _acknowledge_ready_overlay(self) -> None:
        self._ensure_live_flow_state()
        if not self.ready_overlay_visible:
            return
        self.ready_overlay_visible = False
        self.first_boot_ready_required = False
        overlay = self._ready_overlay_frame
        self._ready_overlay_frame = None
        if overlay is not None:
            overlay.place_forget()
            overlay.destroy()
        self._leave_time_freeze(reason='ready_overlay', frozen_seconds=0.0)
        perspective = 'opponent' if self._session_side_to_move() != self.session.player_color else 'player'
        log_line(f'GUI_READY_OVERLAY_ACKNOWLEDGED starts={perspective}', tag='timing')
        if self.session.state == SessionState.OPPONENT_TURN:
            self._schedule_pending_opponent_commit()
        self._refresh_view()

    def _enter_time_freeze(self, *, reason: str) -> None:
        self._ensure_live_flow_state()
        if self._clock_suspend_started_at_monotonic is not None:
            return
        self._clock_suspend_started_at_monotonic = monotonic()
        log_line('GUI_CLOCK_PAUSED', tag='timing')
        self._pause_pending_opponent_schedule(reason=reason)

    def _leave_time_freeze(self, *, reason: str, frozen_seconds: float) -> None:
        self._ensure_live_flow_state()
        if self._clock_suspend_started_at_monotonic is None:
            return
        frozen_duration = frozen_seconds
        if frozen_duration <= 0:
            frozen_duration = max(0.0, monotonic() - self._clock_suspend_started_at_monotonic)
        self._clock_suspend_started_at_monotonic = None
        if self.session.state == SessionState.PLAYER_TURN and self.session._player_turn_started_at is not None:
            self.session._player_turn_started_at += frozen_duration
        self._resume_pending_opponent_schedule(reason=reason)
        log_line('GUI_CLOCK_RESUMED', tag='timing')

    def _pause_pending_opponent_schedule(self, *, reason: str) -> None:
        self._ensure_live_flow_state()
        if self._pending_opponent_after_handle is None:
            return
        remaining = 0.0
        if self._pending_opponent_scheduled_at_monotonic is not None and self.session.pending_opponent_action is not None:
            elapsed = max(0.0, monotonic() - self._pending_opponent_scheduled_at_monotonic)
            remaining = max(0.0, self.session.pending_opponent_action.visible_delay_seconds - elapsed)
        self._frozen_pending_opponent_remaining_delay_seconds = remaining
        self._cancel_pending_opponent_callback(keep_session_pending=True)
        log_line(f'GUI_PENDING_OPPONENT_PAUSED reason={reason} remaining_delay_seconds={remaining:.3f}', tag='timing')

    def _resume_pending_opponent_schedule(self, *, reason: str) -> None:
        self._ensure_live_flow_state()
        if self._frozen_pending_opponent_remaining_delay_seconds is None:
            return
        remaining = max(0.0, self._frozen_pending_opponent_remaining_delay_seconds)
        self._frozen_pending_opponent_remaining_delay_seconds = None
        if self.session.state != SessionState.OPPONENT_TURN or self.session.pending_opponent_action is None:
            return
        if remaining <= 0:
            self._commit_scheduled_opponent_action()
            restored = 0.0
        else:
            self._pending_opponent_scheduled_at_monotonic = monotonic()
            delay_ms = max(1, int(round(remaining * 1000)))
            self._pending_opponent_after_handle = self._schedule_after(delay_ms, self._commit_scheduled_opponent_action)
            restored = delay_ms / 1000.0
        log_line(f'GUI_PENDING_OPPONENT_RESUMED reason={reason} restored_delay_seconds={restored:.3f}', tag='timing')

    def _refresh_view(self, transient_status: str | None = None) -> None:
        self._refresh_board_canvas()
        self._refresh_supporting_surfaces()
        if transient_status:
            self._prepend_recent_status(transient_status)
        view = self.session.get_view()
        if view.state == SessionState.RESTART_PENDING and view.last_outcome is not None:
            if not self.pending_restart:
                self.pending_restart = True
                animation_in_progress = getattr(self.board_view, 'animation_in_progress', None)
                if callable(animation_in_progress) and animation_in_progress():
                    outcome_kind = getattr(getattr(view, 'last_outcome', None), 'terminal_kind', 'unknown')
                    self._log_animation_event(
                        'DEFER_MODAL',
                        outcome=outcome_kind,
                        reason='restart_pending_during_animation',
                        active='yes',
                    )
                    self._deferred_outcome_view = view
                    self._schedule_board_animation_refresh()
                else:
                    self._show_outcome_modal(view)
            return
        self._deferred_outcome_view = None

    def _refresh_board_local(self, transient_status: str | None = None, *, repaint_board: bool = True) -> None:
        if repaint_board:
            self._refresh_board_canvas()
        if transient_status:
            self._prepend_recent_status(transient_status)

    def _refresh_board_canvas(self) -> None:
        view = self.session.get_view()
        board = chess.Board(view.board_fen)
        legal_targets = []
        if self.selected_square is not None and view.awaiting_user_input:
            legal_targets = [move.to_square for move in self.session.legal_moves_from(self.selected_square)]
        self.board_view.set_selection(self.selected_square, legal_targets)
        if hasattr(self.board_view, 'set_premove_queue'):
            queue = getattr(self, 'premove_queue', [])
            self.board_view.set_premove_queue([intent.uci for intent in queue])
        self.board_view.render(board, view.player_color)

    def _schedule_supporting_surface_refresh(self) -> None:
        if getattr(self, '_is_shutting_down', False):
            return
        if getattr(self, '_supporting_surfaces_after_handle', None) is not None:
            return

        def deferred_refresh() -> None:
            self._supporting_surfaces_after_handle = None
            if getattr(self, '_is_shutting_down', False):
                return
            self._refresh_supporting_surfaces()

        self._supporting_surfaces_after_handle = self._schedule_after(0, deferred_refresh)

    def _refresh_post_animation_start(self, *, actor: str) -> None:
        force_immediate_visible_frame = getattr(self.board_view, 'force_immediate_visible_frame', None)
        initial_progress = None
        if callable(force_immediate_visible_frame):
            initial_progress = force_immediate_visible_frame()
        settle = getattr(self.board_view, 'settle_animation', None)
        start_time = getattr(settle, 'start_time', None)
        board_repaint_elapsed_ms = 0
        if start_time is not None:
            board_repaint_elapsed_ms = max(0, int(round((monotonic() - start_time) * 1000)))
        self._refresh_board_canvas()
        self._schedule_board_animation_refresh()
        animation_in_progress = getattr(self.board_view, 'animation_in_progress', None)
        animation_active = callable(animation_in_progress) and animation_in_progress()
        supporting_refresh = 'deferred'
        if animation_active:
            self._supporting_refresh_pending_after_first_tick = True
            supporting_refresh = 'deferred_until_finalize'
        else:
            self._schedule_supporting_surface_refresh()
        self._log_animation_event(
            f'{actor}_POST_START_REPAINT',
            board_repaint='yes',
            board_repaint_elapsed_ms=board_repaint_elapsed_ms,
            animation_refresh='scheduled',
            supporting_refresh=supporting_refresh,
            immediate_frame='yes' if initial_progress is not None else 'no',
            initial_progress='n/a' if initial_progress is None else f'{initial_progress:.3f}',
        )

    def _show_outcome_modal(self, view):
        outcome = view.last_outcome
        if outcome is None:
            return None
        review_boards: list[OutcomeBoardContract] = []
        punishment_slides: list[ReviewSlideContract] = []
        corrective_slides: list[ReviewSlideContract] = []
        if outcome.terminal_kind == 'fail' and outcome.pre_fail_fen and outcome.preferred_move_uci:
            arrows = [OutcomeArrowContract(move_uci=outcome.preferred_move_uci, color='#2e7d32', width_scale=1.0)]
            arrows.extend(OutcomeArrowContract(move_uci=uci, color='#66bb6a', width_scale=0.82) for uci, _ in outcome.excellent_moves)
            arrows.extend(OutcomeArrowContract(move_uci=uci, color='#a5d6a7', width_scale=0.72) for uci, _ in outcome.good_moves)
            recommendation_label_parts = [outcome.preferred_move_san or outcome.preferred_move_uci]
            if outcome.excellent_moves:
                recommendation_label_parts.append('Excellent: ' + ', '.join(san for _, san in outcome.excellent_moves))
            if outcome.good_moves:
                recommendation_label_parts.append('Good: ' + ', '.join(san for _, san in outcome.good_moves))
            review_boards.append(OutcomeBoardContract(
                title='What you should have played',
                board_fen=outcome.pre_fail_fen,
                player_color=outcome.player_color,
                arrow_label='Recommended moves',
                move_label=' | '.join(recommendation_label_parts),
                arrows=tuple(arrows),
            ))
        if outcome.terminal_kind == 'fail' and outcome.punishment_line:
            total_steps = len(outcome.punishment_line)
            for index, (move_uci, move_san, board_fen) in enumerate(outcome.punishment_line, start=1):
                punishment_slides.append(ReviewSlideContract(
                    step_index=index,
                    total_steps=total_steps,
                    line_label='Punishment',
                    board_fen=board_fen,
                    current_move_uci=move_uci,
                    current_move_san=move_san,
                    player_color=outcome.player_color,
                ))
        if outcome.terminal_kind == 'fail' and outcome.corrective_line:
            total_steps = len(outcome.corrective_line)
            for index, (move_uci, move_san, board_fen) in enumerate(outcome.corrective_line, start=1):
                corrective_slides.append(ReviewSlideContract(
                    step_index=index,
                    total_steps=total_steps,
                    line_label='Correct move',
                    board_fen=board_fen,
                    current_move_uci=move_uci,
                    current_move_san=move_san,
                    player_color=outcome.player_color,
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
            punishment_slides=tuple(punishment_slides),
            corrective_slides=tuple(corrective_slides),
        )
        return OutcomeModal(self.root, contract, self._acknowledge_outcome)

    def _acknowledge_outcome(self):
        self._cancel_pending_opponent_callback()
        self.pending_restart = False
        self._deferred_outcome_view = None
        self.selected_square = None
        self._clear_board_transients(reason='acknowledge_outcome')
        self._reconcile_smart_profile_state(reason='outcome_acknowledged')
        self.session.start_new_game()
        self._refresh_top_control_strip()
        self._refresh_view()
        self._apply_post_boot_live_gate()

    def _reconcile_smart_profile_state(self, *, reason: str) -> None:
        if self.session.settings.training_mode != 'smart_profile':
            return
        consume_change = getattr(self.session, 'consume_pending_smart_level_change', None)
        level_change = consume_change() if callable(consume_change) else None
        previous_level: int | None = None
        new_level: int | None = None
        if level_change is not None:
            previous_level, new_level = level_change
            log_line(
                f'GUI_SMART_LEVEL_CHANGED old_level=L{previous_level} new_level=L{new_level} '
                f'track={self.session.settings.selected_smart_track} tc={self.session.settings.selected_time_control_id}',
                tag='smart_profile',
            )
        log_line(
            f'GUI_SMART_RECONCILE_BEGIN reason={reason} track={self.session.settings.selected_smart_track} '
            f'control={self.session.settings.selected_time_control_id}',
            tag='smart_profile',
        )
        self.session._apply_settings(self.session.settings)
        status = self.session.smart_profile_status()
        expected_band = getattr(status, 'expected_rating_band', None)
        contract_turns = getattr(status, 'contract_turns', None)
        contract_good = getattr(status, 'contract_good_accepted', None)
        expected_summary = getattr(status, 'expected_bundle_summary', 'n/a')
        category_id = getattr(status, 'category_id', None)
        level = getattr(status, 'level', None)
        success_streak = getattr(status, 'consecutive_eligible_successes', 0)
        failure_streak = getattr(status, 'consecutive_eligible_failures', 0)
        log_line(
            f'GUI_SMART_RECONCILE_STATE level=L{level} success={success_streak} failure={failure_streak} '
            f'expected={expected_band} control={category_id}',
            tag='smart_profile',
        )
        if reason == 'reset':
            log_line(
                f'GUI_SMART_RESET_APPLIED level=L{level} success={success_streak} failure={failure_streak} '
                f'control={category_id}',
                tag='smart_profile',
            )
        updated_settings = self.session.settings
        resolved_bundle_path, blocked_message = self._resolve_bundle_for_top_contract(updated_settings)
        switched = False
        missing = False
        resolved_label = 'n/a'
        if resolved_bundle_path:
            resolved_label = Path(resolved_bundle_path).name
            remembered_path = self._remembered_bundle_path()
            switched = self._bundle_token(remembered_path) != self._bundle_token(resolved_bundle_path)
            if switched:
                self._load_selected_bundle(resolved_bundle_path)
        else:
            missing = True
            if blocked_message:
                self._prepend_recent_status(blocked_message)
        log_line(
            f'GUI_SMART_RECONCILE_BUNDLE expected={expected_summary} resolved={resolved_label} '
            f'switched={str(switched).lower()} missing={str(missing).lower()}',
            tag='smart_profile',
        )
        if reason == 'reset':
            log_line(
                f'GUI_SMART_RESET_BUNDLE expected={expected_summary} switched={str(switched).lower()} '
                f'missing={str(missing).lower()}',
                tag='smart_profile',
            )
        self._refresh_top_control_strip()
        refreshed = self.session.smart_profile_status()
        refreshed_level = getattr(refreshed, 'level', None)
        refreshed_band = getattr(refreshed, 'expected_rating_band', None)
        refreshed_turns = getattr(refreshed, 'contract_turns', None)
        refreshed_good = getattr(refreshed, 'contract_good_accepted', None)
        refreshed_control = getattr(refreshed, 'category_id', category_id)
        refreshed_success = getattr(refreshed, 'consecutive_eligible_successes', success_streak)
        refreshed_failure = getattr(refreshed, 'consecutive_eligible_failures', failure_streak)
        level_change_kind = 'none'
        if previous_level is not None and new_level is not None:
            level_change_kind = 'promotion_or_demotion'
        elif reason == 'outcome_acknowledged':
            level_change_kind = 'streak_only_or_steady'
        log_line(
            f'GUI_SMART_RECONCILE_VISIBLE_REFRESH reason={reason} change_kind={level_change_kind} '
            f'level=L{refreshed_level} band={refreshed_band} turns={refreshed_turns} good={refreshed_good} '
            f'control={refreshed_control} success={refreshed_success} failure={refreshed_failure}',
            tag='smart_profile',
        )
        if reason == 'reset':
            log_line(
                f'GUI_SMART_RESET_VISIBLE_REFRESH level=L{refreshed_level} band={refreshed_band} '
                f'depth={refreshed_turns} good={refreshed_good}',
                tag='smart_profile',
            )
        log_line(
            f'GUI_SMART_CONTRACT_STRIP_REFRESHED level={self.top_level_var.get()} '
            f'elo={self.top_elo_var.get()} depth={self.top_depth_var.get()} good={self.top_good_var.get()}',
            tag='smart_profile',
        )

    def _on_board_press(self, event: tk.Event) -> None:
        view = self.session.get_view()
        square = self.board_view.square_at_xy(event.x, event.y, view.player_color)
        if square is None:
            return
        board = self.session.current_board()
        piece = board.piece_at(square)
        if piece is None or piece.color != view.player_color:
            if self.selected_square is None:
                self._refresh_board_local('Select one of your own pieces.', repaint_board=False)
            return
        if view.awaiting_user_input:
            legal_moves = self.session.legal_moves_from(square)
        else:
            preview_board = board.copy(stack=False)
            preview_board.turn = view.player_color
            legal_moves = [move for move in preview_board.legal_moves if move.from_square == square]
        if not legal_moves:
            message = 'That piece has no legal moves.' if view.awaiting_user_input else 'No premove available from that square.'
            self._refresh_board_local(message, repaint_board=False)
            return
        self.selected_square = square
        self.board_view.start_drag(square, piece.symbol(), event.x, event.y)
        self._refresh_board_local()

    def _on_board_drag(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if self.selected_square is None:
            return
        self.board_view.update_drag(event.x, event.y, view.player_color)
        self._refresh_board_canvas()

    def _on_board_release(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if self.selected_square is None:
            return
        released = self.board_view.release_drag(event.x, event.y, view.player_color)
        if released is None:
            return
        from_square, to_square, was_drag = released
        board = self.session.current_board()
        if not was_drag:
            if to_square == self.selected_square:
                self._refresh_board_local()
                return
            if to_square is None:
                self.selected_square = None
                self._refresh_board_local('Selection cleared.')
                return
            destination_piece = board.piece_at(to_square)
            if destination_piece is not None and destination_piece.color == view.player_color:
                self.selected_square = to_square
                self._refresh_board_local()
                return
        if to_square is None:
            self.selected_square = None
            self._refresh_board_local('Move cancelled.')
            return
        if view.awaiting_user_input:
            move = self._build_move(from_square, to_square, board)
        else:
            move = self._build_premove_move(from_square, to_square, board, view.player_color)
        self.selected_square = None
        if move is None:
            message = 'Illegal move selection.' if view.awaiting_user_input else 'Premove not possible from this position.'
            self._refresh_board_local(message)
            return
        if not view.awaiting_user_input:
            self.board_view.cancel_drag()
            self._queue_premove(move)
            self._refresh_board_local('Premove queued.')
            return
        moved_piece = board.piece_at(from_square)
        self.board_view.cancel_drag()
        self.session.submit_user_move_uci(move.uci())
        if moved_piece is not None:
            prior_animation_exists = getattr(self.board_view, 'settle_animation', None) is not None
            release_start_x = float(event.x) if was_drag else None
            release_start_y = float(event.y) if was_drag else None
            self.board_view.start_committed_move_animation(
                piece_symbol=moved_piece.symbol(),
                source_square=from_square,
                destination_square=move.to_square,
                player_color=view.player_color,
                start_x=release_start_x,
                start_y=release_start_y,
                duration_ms=PLAYER_COMMITTED_MOVE_DURATION_MS,
            )
            self._log_animation_event(
                'PLAYER_START',
                piece=moved_piece.symbol(),
                from_sq=chess.square_name(from_square),
                to_sq=chess.square_name(move.to_square),
                duration_ms=PLAYER_COMMITTED_MOVE_DURATION_MS,
                color='white' if view.player_color == chess.WHITE else 'black',
                prior_transient='yes' if prior_animation_exists else 'no',
                start_mode='release_xy' if was_drag else 'source_center',
            )
        self._refresh_post_animation_start(actor='PLAYER')
        post_submit_view = self.session.get_view()
        if self._handle_player_terminal_outcome(post_submit_view):
            return
        self._schedule_pending_opponent_commit()

    def _build_premove_move(
        self,
        from_square: chess.Square,
        to_square: chess.Square,
        board: chess.Board,
        player_color: chess.Color,
    ) -> chess.Move | None:
        preview_board = board.copy(stack=False)
        preview_board.turn = player_color
        return self._build_move(from_square, to_square, preview_board)

    def _queue_premove(self, move: chess.Move) -> None:
        if not hasattr(self, 'premove_queue'):
            self.premove_queue = []
        intent = PremoveIntent(move.from_square, move.to_square, promotion=move.promotion)
        self.premove_queue.append(intent)
        log_line(
            f'GUI_PREMOVE_QUEUED: move={intent.uci}; queue_len={len(self.premove_queue)}; appended=yes',
            tag='timing',
        )

    def _clear_premove_queue(self, *, reason: str) -> None:
        if not hasattr(self, 'premove_queue'):
            self.premove_queue = []
        cleared = len(self.premove_queue)
        if cleared == 0:
            return
        self.premove_queue.clear()
        log_line(f'GUI_PREMOVE_CLEARED: reason={reason}; cleared={cleared}', tag='timing')

    def _attempt_execute_next_premove(self) -> bool:
        if not hasattr(self, 'premove_queue'):
            self.premove_queue = []
        if self.session.state != SessionState.PLAYER_TURN or not self.premove_queue:
            return False
        next_intent = self.premove_queue[0]
        log_line(f'GUI_PREMOVE_EXEC_ATTEMPT: move={next_intent.uci}; queue_len={len(self.premove_queue)}', tag='timing')
        board = self.session.current_board()
        move = chess.Move(next_intent.from_square, next_intent.to_square, promotion=next_intent.promotion)
        moved_piece = board.piece_at(next_intent.from_square)
        if moved_piece is None:
            cleared = len(self.premove_queue)
            log_line(f'GUI_PREMOVE_INVALIDATED: move={next_intent.uci}; reason=impossible; cleared={cleared}', tag='timing')
            self._clear_premove_queue(reason='invalidated_impossible')
            self._refresh_view()
            return False
        if move not in board.legal_moves:
            cleared = len(self.premove_queue)
            log_line(f'GUI_PREMOVE_INVALIDATED: move={next_intent.uci}; reason=illegal; cleared={cleared}', tag='timing')
            self._clear_premove_queue(reason='invalidated_illegal')
            self._refresh_view()
            return False
        self.premove_queue.pop(0)
        self.session.submit_user_move_uci(move.uci(), premove_executed=True)
        if getattr(self.session, 'timed_state', None) is not None:
            white_seconds, black_seconds = self.session.displayed_clock_seconds()
            player_remaining_seconds = white_seconds if self.session.player_color == chess.WHITE else black_seconds
            log_line(
                "GUI_PREMOVE_TIME_APPLIED: "
                f"deducted_seconds={self.session.premove_execution_time_cost_seconds:.1f}; "
                f"player_remaining_seconds={player_remaining_seconds:.3f}",
                tag='timing',
            )
        self.board_view.start_committed_move_animation(
            piece_symbol=moved_piece.symbol(),
            source_square=move.from_square,
            destination_square=move.to_square,
            player_color=self.session.get_view().player_color,
            duration_ms=PLAYER_COMMITTED_MOVE_DURATION_MS,
        )
        self._refresh_post_animation_start(actor='PREMOVE')
        log_line(f'GUI_PREMOVE_EXECUTED: move={next_intent.uci}; remaining={len(self.premove_queue)}', tag='timing')
        post_submit_view = self.session.get_view()
        if self._handle_player_terminal_outcome(post_submit_view):
            return True
        if self.session.state == SessionState.OPPONENT_TURN:
            self._schedule_pending_opponent_commit()
        else:
            self._refresh_view()
        return True

    def _handle_player_terminal_outcome(self, view) -> bool:
        if getattr(view, 'state', None) != SessionState.RESTART_PENDING or getattr(view, 'last_outcome', None) is None:
            return False
        outcome_kind = getattr(getattr(view, 'last_outcome', None), 'terminal_kind', 'unknown')
        self.pending_restart = True
        self._log_animation_event(
            'PLAYER_TERMINAL_DETECTED',
            outcome=outcome_kind,
            state=getattr(SessionState.RESTART_PENDING, 'value', 'restart_pending'),
        )
        animation_in_progress = getattr(self.board_view, 'animation_in_progress', None)
        if callable(animation_in_progress) and animation_in_progress():
            self._deferred_outcome_view = view
            self._log_animation_event(
                'PLAYER_TERMINAL_DEFERRED',
                outcome=outcome_kind,
                reason='restart_pending_during_animation',
                active='yes',
            )
            self._schedule_board_animation_refresh()
            return True
        self._deferred_outcome_view = None
        self._log_animation_event(
            'PLAYER_TERMINAL_IMMEDIATE_MODAL',
            outcome=outcome_kind,
            reason='restart_pending_without_animation',
            active='no',
        )
        self._show_outcome_modal(view)
        return True

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
        file_menu.add_command(label='Check for Updates', command=self._check_for_updates_from_gui)
        file_menu.add_command(label='Exit', command=self._request_shutdown)
        menubar.add_cascade(label='File', menu=file_menu)
        dev_menu = tk.Menu(menubar, tearoff=0)
        dev_menu.add_command(label='Open Dev Console', command=self._open_dev_console)
        dev_menu.add_command(label='Timing Override...', command=self._open_timing_override_dialog)
        dev_menu.add_command(label='Open Logs Folder', command=self._open_logs_folder)
        dev_menu.add_command(label='Copy Current Session Log Path', command=self._copy_session_log_path)
        dev_menu.add_command(label='Clear Visible Buffer', command=self._clear_visible_log_buffer)
        dev_menu.add_separator()
        dev_menu.add_command(label='Open Board Setup Editor', command=self._open_board_setup_editor)
        dev_menu.add_command(label='Reset Smart Profile State', command=self._reset_smart_profile_state)
        dev_menu.add_command(label='Set Smart Profile Level…', command=self._set_smart_profile_level)
        menubar.add_cascade(label='Developer', menu=dev_menu)
        self.root.config(menu=menubar)

    def _app_state_root(self) -> Path:
        runtime_paths = getattr(getattr(self.session, "runtime_context", None), "runtime_paths", None)
        if runtime_paths is not None and getattr(runtime_paths, "app_state_root", None) is not None:
            return Path(runtime_paths.app_state_root)
        local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        return local_app_data / "OpeningTrainer"

    def _show_report_placeholder(self) -> None:
        messagebox.showinfo(
            "Report",
            "Reporting is not implemented yet.",
            parent=self.root,
        )

    def _check_for_updates_from_gui(self) -> None:
        if getattr(self, "_updater_mode_active", False):
            messagebox.showinfo("Updater", "An update attempt is already in progress.", parent=self.root)
            return
        try:
            app_state_root = self._app_state_root()
            manifest_ref = resolve_manifest_path_or_url(None, app_state_root=app_state_root)
            has_update, manifest, installed = check_for_update(manifest_ref, app_state_root=app_state_root)
            installed_version = "unknown" if not installed else str(installed.get("app_version") or "unknown")
            installed_build = "unknown" if not installed else str(installed.get("build_id") or "unknown")
            latest = f"{manifest.app_version} ({manifest.build_id or 'no-build-id'})"
            current = f"{installed_version} ({installed_build})"
            if not has_update:
                messagebox.showinfo(
                    "Check for Updates",
                    f"No updates are available.\n\nInstalled: {current}\nLatest: {latest}",
                    parent=self.root,
                )
                return
            confirm = messagebox.askyesno(
                "Update Available",
                f"Installed: {current}\nAvailable: {latest}\n\nDownload and apply update now?",
                parent=self.root,
            )
            if not confirm:
                return
            log_line("GUI_UPDATE_CONFIRM_ACCEPTED", tag="startup")
            self._enter_updater_mode()
            launch_updater_helper(
                manifest_ref,
                app_state_root=app_state_root,
                wait_for_pid=os.getpid(),
                relaunch_exe_path=Path(sys.executable),
                relaunch_args=["--runtime-mode", "consumer"],
            )
            self._updater_apply_started = True
            log_line("GUI_UPDATE_HELPER_LAUNCHED", tag="startup")
            self._set_updater_status_text("Applying update and restarting Opening Trainer…")
            self._shutdown_coordinator(reason="apply_update")
        except UpdaterInstallStateError as exc:
            self._exit_updater_mode()
            log_line(f"GUI_UPDATE_INSTALL_STATE_ERROR: {exc}", tag="error")
            messagebox.showerror("Update Error", f"Update cannot continue.\n{exc}", parent=self.root)
        except Exception as exc:  # noqa: BLE001
            self._exit_updater_mode()
            log_line(f"GUI_UPDATE_FAILED: {exc}", tag="error")
            messagebox.showerror("Update Error", f"Update check/apply failed.\n{exc}", parent=self.root)

    def _open_board_setup_editor(self) -> None:
        self.inspector.open_board_setup_editor()

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

    def _reset_smart_profile_state(self) -> None:
        pre_status = self.session.smart_profile_status()
        log_line(
            f'GUI_SMART_RESET_BEGIN track={getattr(pre_status, "track_id", None)} '
            f'control={getattr(pre_status, "category_id", None)}',
            tag='smart_profile',
        )
        self.session.smart_profile.reset_all()
        self._reconcile_smart_profile_state(reason='reset')
        self._refresh_supporting_surfaces()

    def _set_smart_profile_level(self) -> None:
        time_control_id, _rating_band = self.session._timing_contract_metadata()
        raw = simpledialog.askstring('Smart Profile Level', 'Set level for current track/category (1-28):', parent=self.root)
        if raw is None:
            return
        try:
            level = int(raw)
        except ValueError:
            messagebox.showerror('Smart Profile', 'Level must be an integer.', parent=self.root)
            return
        if not self.session.smart_profile.set_level_for_current_track(time_control_id=time_control_id, level=level):
            messagebox.showerror('Smart Profile', 'Current bundle time control is unsupported for Smart Profile.', parent=self.root)
            return
        self._reconcile_smart_profile_state(reason='set_level')
        self._refresh_supporting_surfaces()

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

    def _schedule_board_animation_refresh(self) -> None:
        if getattr(self, '_is_shutting_down', False):
            return
        if getattr(self, '_board_animation_after_handle', None) is not None:
            return

        def tick() -> None:
            self._board_animation_after_handle = None
            if getattr(self, '_is_shutting_down', False):
                return
            active = self.board_view.animation_in_progress()
            sample_animation_position = getattr(self.board_view, 'sample_animation_position', None)
            sampled = sample_animation_position() if callable(sample_animation_position) else None
            settle = getattr(self.board_view, 'settle_animation', None)
            elapsed_ms = 0
            if settle is not None:
                elapsed_ms = max(0, int(round((monotonic() - settle.start_time) * 1000)))
            sampled_text = 'none' if sampled is None else f'{sampled[0]:.1f},{sampled[1]:.1f}'
            self._log_animation_event(
                'TICK',
                active='yes' if active else 'no',
                elapsed_ms=elapsed_ms,
                first_tick_elapsed_ms=elapsed_ms,
                sample=sampled_text,
                deferred_modal='yes' if getattr(self, '_deferred_outcome_view', None) is not None else 'no',
            )
            if active:
                self._refresh_board_canvas()
                self._board_animation_after_handle = self._schedule_after(16, tick)
            else:
                if getattr(self, '_supporting_refresh_pending_after_first_tick', False):
                    self._supporting_refresh_pending_after_first_tick = False
                    self._schedule_supporting_surface_refresh()
                    self._log_animation_event(
                        'SUPPORTING_REFRESH_RELEASED',
                        phase='finalize',
                        elapsed_ms=elapsed_ms,
                    )
                self._finalize_board_animation_if_complete()
                self._refresh_board_canvas()
                self._show_deferred_outcome_modal_if_ready()

        self._board_animation_after_handle = self._schedule_after(16, tick)

    def _finalize_board_animation_if_complete(self) -> bool:
        board_view = getattr(self, 'board_view', None)
        if board_view is None:
            return False
        animation_complete = getattr(board_view, 'animation_complete', None)
        finalize_animation = getattr(board_view, 'finalize_animation', None)
        if not callable(animation_complete) or not callable(finalize_animation):
            return False
        if not animation_complete():
            return False
        destination_square = getattr(getattr(board_view, 'settle_animation', None), 'destination_square', None)
        finalized = bool(finalize_animation())
        destination = 'none' if destination_square is None else chess.square_name(destination_square)
        self._log_animation_event(
            'FINALIZE',
            finalized='yes' if finalized else 'no',
            destination=destination,
            deferred_modal='yes' if getattr(self, '_deferred_outcome_view', None) is not None else 'no',
        )
        return finalized

    def _schedule_pending_opponent_commit(self) -> None:
        self._ensure_live_flow_state()
        if self.session.state != SessionState.OPPONENT_TURN:
            return
        if getattr(self, 'paused', False) or getattr(self, 'ready_overlay_visible', False):
            return
        self._cancel_pending_opponent_callback()
        pending = self.session.pending_opponent_action or self.session.prepare_pending_opponent_action()
        if pending is None:
            log_line('GUI_OPPONENT_PENDING_PREPARE_SKIPPED: no pending action available', tag='timing')
            return
        log_line('GUI_OPPONENT_PENDING_PREPARED', tag='timing')
        frozen_delay_seconds = getattr(self, '_frozen_pending_opponent_remaining_delay_seconds', None)
        if frozen_delay_seconds is not None:
            delay_seconds = frozen_delay_seconds
            self._frozen_pending_opponent_remaining_delay_seconds = None
        else:
            delay_seconds = pending.visible_delay_seconds
        delay_ms = max(0, int(round(delay_seconds * 1000)))
        if delay_ms == 0:
            log_line('GUI_OPPONENT_COMMIT_SCHEDULED: delay_ms=0; committing immediately', tag='timing')
            self._commit_scheduled_opponent_action()
            return
        self._pending_opponent_scheduled_at_monotonic = monotonic()
        self._pending_opponent_after_handle = self._schedule_after(delay_ms, self._commit_scheduled_opponent_action)
        log_line(f'GUI_OPPONENT_COMMIT_SCHEDULED: delay_ms={delay_ms}', tag='timing')

    def _commit_scheduled_opponent_action(self) -> None:
        self._ensure_live_flow_state()
        self._pending_opponent_after_handle = None
        self._pending_opponent_scheduled_at_monotonic = None
        if getattr(self, 'paused', False) or getattr(self, 'ready_overlay_visible', False):
            return
        if getattr(self, '_is_shutting_down', False):
            log_line('GUI_OPPONENT_COMMIT_SKIPPED: app shutting down', tag='timing')
            return
        pending = self.session.pending_opponent_action
        if pending is None:
            log_line('GUI_OPPONENT_COMMIT_SKIPPED: no pending action', tag='timing')
            return
        committed_choice = getattr(pending, 'choice', None)
        committed_move = getattr(committed_choice, 'move', None)
        board_before = getattr(pending, 'board_before', None)
        metadata_complete = committed_move is not None and board_before is not None
        self._log_animation_event('OPPONENT_PENDING_METADATA', complete='yes' if metadata_complete else 'no')
        moved_piece = None
        if committed_move is not None and board_before is not None:
            moved_piece = board_before.piece_at(committed_move.from_square)
        self.session.commit_pending_opponent_action()
        log_line('GUI_OPPONENT_COMMIT_EXECUTED', tag='timing')
        animation_started = False
        if moved_piece is not None and committed_move is not None:
            get_view = getattr(self.session, 'get_view', None)
            if not callable(get_view):
                view = None
            else:
                view = get_view()
            player_color = getattr(view, 'player_color', chess.WHITE)
            self.board_view.start_committed_move_animation(
                piece_symbol=moved_piece.symbol(),
                source_square=committed_move.from_square,
                destination_square=committed_move.to_square,
                player_color=player_color,
                duration_ms=OPPONENT_COMMITTED_MOVE_DURATION_MS,
            )
            animation_started = True
            self._log_animation_event(
                'OPPONENT_START',
                piece=moved_piece.symbol(),
                from_sq=chess.square_name(committed_move.from_square),
                to_sq=chess.square_name(committed_move.to_square),
                duration_ms=OPPONENT_COMMITTED_MOVE_DURATION_MS,
                color='white' if player_color == chess.WHITE else 'black',
                metadata_complete='yes',
            )
        else:
            self._log_animation_event('OPPONENT_START', metadata_complete='no', started='no')
        if animation_started:
            self._refresh_post_animation_start(actor='OPPONENT')
        else:
            self._refresh_view()
            self._schedule_board_animation_refresh()
            self._log_animation_event(
                'OPPONENT_POST_COMMIT_REFRESH',
                repaint='yes',
                animation_refresh='scheduled',
                animation_started='no',
                supporting_refresh='inline',
            )
        if self.session.state == SessionState.OPPONENT_TURN:
            self._schedule_pending_opponent_commit()
        elif self.session.state == SessionState.PLAYER_TURN:
            self._attempt_execute_next_premove()

    def _cancel_pending_opponent_callback(self, *, keep_session_pending: bool = False) -> None:
        self._ensure_live_flow_state()
        handle = getattr(self, '_pending_opponent_after_handle', None)
        self._pending_opponent_after_handle = None
        self._pending_opponent_scheduled_at_monotonic = None
        if handle is not None:
            root = getattr(self, 'root', None)
            try:
                if root is not None:
                    root.after_cancel(handle)
            except Exception:
                pass
            after_handles = getattr(self, '_after_handles', set())
            after_handles.discard(handle)
            log_line('GUI_OPPONENT_CALLBACK_CANCELLED: cancelled stale scheduled callback', tag='timing')
        session = getattr(self, 'session', None)
        cancel_pending = getattr(session, 'cancel_pending_opponent_action', None) if session is not None else None
        if callable(cancel_pending) and not keep_session_pending:
            cancel_pending()
            log_line('GUI_OPPONENT_PENDING_DISCARDED', tag='timing')

    def _cancel_board_animation_callback(self) -> None:
        handle = getattr(self, '_board_animation_after_handle', None)
        self._board_animation_after_handle = None
        if handle is None:
            return
        root = getattr(self, 'root', None)
        try:
            if root is not None:
                root.after_cancel(handle)
        except Exception:
            pass
        after_handles = getattr(self, '_after_handles', set())
        after_handles.discard(handle)

    def _clear_board_transients(self, *, reason: str = 'unspecified') -> None:
        self._ensure_live_flow_state()
        self._log_animation_event('CLEAR_TRANSIENTS', reason=reason)
        self._clear_premove_queue(reason=reason)
        self._cancel_board_animation_callback()
        self._supporting_refresh_pending_after_first_tick = False
        self._deferred_outcome_view = None
        ready_overlay_frame = getattr(self, '_ready_overlay_frame', None)
        if ready_overlay_frame is not None:
            ready_overlay_frame.place_forget()
            ready_overlay_frame.destroy()
            self._ready_overlay_frame = None
        self.ready_overlay_visible = False
        self.paused = False
        self.pause_started_at_monotonic = None
        self._clock_suspend_started_at_monotonic = None
        self._frozen_pending_opponent_remaining_delay_seconds = None
        board_view = getattr(self, 'board_view', None)
        if board_view is None:
            return
        if hasattr(board_view, 'clear_transient_state'):
            board_view.clear_transient_state()
        else:
            board_view.cancel_drag()

    def _show_deferred_outcome_modal_if_ready(self) -> None:
        deferred = getattr(self, '_deferred_outcome_view', None)
        if deferred is None:
            return
        if getattr(self, '_is_shutting_down', False):
            return
        self._finalize_board_animation_if_complete()
        animation_in_progress = getattr(self.board_view, 'animation_in_progress', None)
        if callable(animation_in_progress) and animation_in_progress():
            return
        settle_animation = getattr(self.board_view, 'settle_animation', None)
        outcome_kind = getattr(getattr(deferred, 'last_outcome', None), 'terminal_kind', 'unknown')
        self._log_animation_event(
            'SHOW_DEFERRED_MODAL',
            outcome=outcome_kind,
            finalized='yes' if settle_animation is None else 'no',
        )
        self._deferred_outcome_view = None
        self._show_outcome_modal(deferred)

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

    def _start_live_clock_refresh(self) -> None:
        if getattr(self, '_clock_refresh_after_handle', None) is not None:
            return
        log_line('GUI_CLOCK_LIVE_REFRESH_STARTED', tag='timing')

        def tick() -> None:
            self._clock_refresh_after_handle = None
            if getattr(self, '_is_shutting_down', False):
                return
            self._refresh_clock_display()
            self._clock_refresh_after_handle = self._schedule_after(LIVE_CLOCK_REFRESH_INTERVAL_MS, tick)

        self._clock_refresh_after_handle = self._schedule_after(LIVE_CLOCK_REFRESH_INTERVAL_MS, tick)

    def _stop_live_clock_refresh(self) -> None:
        handle = getattr(self, '_clock_refresh_after_handle', None)
        self._clock_refresh_after_handle = None
        if handle is None:
            return
        root = getattr(self, 'root', None)
        try:
            if root is not None:
                root.after_cancel(handle)
        except Exception:
            pass
        after_handles = getattr(self, '_after_handles', set())
        after_handles.discard(handle)
        log_line('GUI_CLOCK_LIVE_REFRESH_STOPPED', tag='timing')

    def _request_shutdown(self) -> None:
        if getattr(self, "_updater_mode_active", False) and not getattr(self, "_updater_apply_started", False):
            cancel = messagebox.askyesno(
                "Cancel Update",
                "Update preparation is in progress. Cancel this update attempt and keep the current install?",
                parent=self.root,
            )
            if cancel:
                log_line("GUI_UPDATE_CANCELLED_BEFORE_APPLY", tag="startup")
                self._exit_updater_mode()
            return
        if getattr(self, "_updater_apply_started", False):
            messagebox.showinfo(
                "Updater",
                "Opening Trainer is applying an update. Please wait for it to finish.",
                parent=self.root,
            )
            return
        self._shutdown_coordinator(reason='window_close')

    def _shutdown_coordinator(self, reason: str) -> None:
        if getattr(self, '_shutdown_started', False):
            return
        self._shutdown_started = True
        self._is_shutting_down = True
        if reason == "apply_update":
            log_line("GUI_UPDATE_AUTHORITATIVE_SHUTDOWN_REQUESTED", tag="startup")
        log_line(f'APP_SHUTDOWN_BEGIN: reason={reason}', tag='startup')
        self._stop_live_clock_refresh()
        self._destroy_pause_surface()
        self._cancel_pending_opponent_callback()
        self._clear_board_transients(reason='shutdown')
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

    def _set_updater_status_text(self, message: str) -> None:
        status_window = getattr(self, "_updater_status_window", None)
        if status_window is None:
            return
        status_var = getattr(self, "_updater_status_var", None)
        if status_var is not None:
            status_var.set(message)
        if hasattr(self.root, "update_idletasks"):
            self.root.update_idletasks()

    def _enter_updater_mode(self) -> None:
        if self._updater_mode_active:
            return
        self._updater_mode_active = True
        log_line("GUI_UPDATE_MODE_ENTERED", tag="startup")
        self._widgets_disabled_for_update = []
        for widget in self.root.winfo_children():
            self._disable_widget_tree_for_update(widget)
        status_window = tk.Toplevel(self.root)
        status_window.title("Applying Update")
        status_window.resizable(False, False)
        status_window.transient(self.root)
        status_window.protocol("WM_DELETE_WINDOW", lambda: None)
        frame = ttk.Frame(status_window, padding=16)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Opening Trainer update in progress", font=("TkDefaultFont", 11, "bold")).pack(anchor="w")
        self._updater_status_var = tk.StringVar(
            value="Preparing updater handoff. The app will restart automatically."
        )
        ttk.Label(frame, textvariable=self._updater_status_var, wraplength=360, justify="left").pack(anchor="w", pady=(8, 10))
        progress = ttk.Progressbar(frame, mode="indeterminate", length=320)
        progress.pack(fill="x")
        progress.start(12)
        self._updater_status_progress = progress
        self._updater_status_window = status_window
        log_line("GUI_UPDATE_MODE_VISIBLE", tag="startup")

    def _disable_widget_tree_for_update(self, widget: tk.Widget) -> None:
        if widget is getattr(self, "_updater_status_window", None):
            return
        for child in widget.winfo_children():
            self._disable_widget_tree_for_update(child)
        if getattr(widget, "winfo_class", lambda: "")() in {"Button", "TButton", "Checkbutton", "TCheckbutton", "Entry", "TCombobox"}:
            state = widget.cget("state")
            if state != "disabled":
                self._widgets_disabled_for_update.append((widget, state))
                widget.configure(state="disabled")

    def _exit_updater_mode(self) -> None:
        if not self._updater_mode_active:
            return
        self._updater_mode_active = False
        self._updater_apply_started = False
        for widget, state in reversed(self._widgets_disabled_for_update):
            try:
                widget.configure(state=state)
            except Exception:
                pass
        self._widgets_disabled_for_update = []
        progress = getattr(self, "_updater_status_progress", None)
        if progress is not None:
            try:
                progress.stop()
            except Exception:
                pass
        self._updater_status_progress = None
        status_window = getattr(self, "_updater_status_window", None)
        if status_window is not None:
            try:
                status_window.destroy()
            except Exception:
                pass
        self._updater_status_window = None
        self._updater_status_var = None
        log_line("GUI_UPDATE_MODE_EXITED", tag="startup")

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


class DuplicateInstanceLaunchBlockedError(RuntimeError):
    """Raised when GUI startup is blocked by an existing instance guard."""


def _is_frozen_or_consumer_runtime(runtime_context: RuntimeContext | None) -> bool:
    runtime_mode_value = getattr(getattr(runtime_context, "runtime_mode", None), "value", "")
    return bool(getattr(sys, "frozen", False)) or runtime_mode_value == "consumer"


def _format_duplicate_instance_message(diagnostics) -> str:
    details = ["Opening Trainer is already starting or running."]
    if diagnostics is not None:
        details.append("")
        details.append(f"Existing PID: {diagnostics.pid}")
        if diagnostics.startup_utc:
            details.append(f"Started (UTC): {diagnostics.startup_utc}")
        if diagnostics.session_log_path:
            details.append(f"Session log: {diagnostics.session_log_path}")
        if diagnostics.session_id:
            details.append(f"Session ID: {diagnostics.session_id}")
    return "\n".join(details)


def _show_duplicate_instance_dialog(message: str) -> None:
    if os.name == "nt":
        try:
            ctypes.windll.user32.MessageBoxW(0, message, "Opening Trainer", 0x10 | 0x0)
            return
        except Exception:
            pass
    try:
        messagebox.showerror("Opening Trainer", message)
        return
    except Exception:
        pass
    print(message, file=sys.stderr)


def launch_gui(runtime_context: RuntimeContext | None = None, probe_real_startup: bool = False) -> None:
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
        duplicate_message = _format_duplicate_instance_message(diagnostics)
        if _is_frozen_or_consumer_runtime(runtime_context):
            _show_duplicate_instance_dialog(duplicate_message)
            raise DuplicateInstanceLaunchBlockedError(duplicate_message)
        if probe_real_startup:
            raise DuplicateInstanceLaunchBlockedError(duplicate_message)
        return
    repo_root = Path(__file__).resolve().parents[3]
    log_line(
        f'GUI_ANIM_IMPL_VERSION: marker={ANIMATION_IMPL_MARKER}; repo_root={repo_root}; executable={sys.executable}',
        tag='startup',
    )
    log_line("GUI_BOOTSTRAP: creating Tk root.", tag="startup")
    try:
        app = OpeningTrainerGUI(runtime_context=runtime_context)
        write_instance_diagnostics(window_title='Opening Trainer')
        if probe_real_startup:
            app.root.update_idletasks()
            app.root.destroy()
            remove_instance_diagnostics()
            release_single_instance_guard()
            log_line("GUI_PROBE_REAL_STARTUP_OK", tag="startup")
            return
        log_line("GUI_READY: Opening Trainer GUI initialized.", tag="startup")
        app.run()
    except Exception as exc:
        log_line(f"GUI_STARTUP_FAILED: {exc}", tag="error")
        remove_instance_diagnostics()
        release_single_instance_guard()
        raise
