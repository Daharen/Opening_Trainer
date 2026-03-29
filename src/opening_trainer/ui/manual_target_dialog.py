from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import chess

from ..review.manual_target import validate_manual_target
from ..review.models import ManualForcedPlayerColor, ManualPresentationMode, UrgencyTier


class ManualTargetDialog(tk.Toplevel):
    def __init__(self, master, on_save, *, title: str = 'Add Manual Target', initial: dict | None = None):
        super().__init__(master)
        self.title(title)
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()
        self.on_save = on_save
        initial = initial or {}

        self.fen_var = tk.StringVar(value=initial.get('target_fen', ''))
        self.side_to_move_var = tk.StringVar(value='—')
        self.predecessor_var = tk.StringVar(value=initial.get('predecessor_line_uci', ''))
        self.urgency_var = tk.StringVar(value=initial.get('urgency_tier', UrgencyTier.ORDINARY.value))
        self.presentation_mode_var = tk.StringVar(value=initial.get('manual_presentation_mode', ManualPresentationMode.PLAY_TO_POSITION.value))
        self.forced_color_var = tk.StringVar(value=initial.get('manual_forced_player_color', ManualForcedPlayerColor.AUTO.value))
        self.allow_below_var = tk.BooleanVar(value=bool(initial.get('allow_below_threshold_reach', False)))
        self.note_var = tk.StringVar(value=initial.get('operator_note', ''))
        self.error_var = tk.StringVar(value='')

        frame = ttk.Frame(self, padding=12)
        frame.grid(row=0, column=0, sticky='nsew')

        ttk.Label(frame, text='Target FEN').grid(row=0, column=0, sticky='w')
        ttk.Entry(frame, textvariable=self.fen_var, width=80).grid(row=1, column=0, columnspan=2, sticky='ew', pady=(2, 8))
        ttk.Label(frame, text='Side to move').grid(row=2, column=0, sticky='w')
        ttk.Label(frame, textvariable=self.side_to_move_var).grid(row=2, column=1, sticky='w')

        ttk.Label(frame, text='Predecessor line (UCI, optional)').grid(row=3, column=0, sticky='w', pady=(8, 0))
        ttk.Entry(frame, textvariable=self.predecessor_var, width=80).grid(row=4, column=0, columnspan=2, sticky='ew', pady=(2, 8))

        ttk.Label(frame, text='Presentation mode').grid(row=5, column=0, sticky='w')
        ttk.Combobox(
            frame,
            state='readonly',
            textvariable=self.presentation_mode_var,
            values=[ManualPresentationMode.PLAY_TO_POSITION.value, ManualPresentationMode.FORCE_TARGET_START.value],
            width=28,
        ).grid(row=5, column=1, sticky='w')

        ttk.Label(frame, text='Forced player color').grid(row=6, column=0, sticky='w', pady=(8, 0))
        ttk.Combobox(
            frame,
            state='readonly',
            textvariable=self.forced_color_var,
            values=[ManualForcedPlayerColor.AUTO.value, ManualForcedPlayerColor.WHITE.value, ManualForcedPlayerColor.BLACK.value],
            width=20,
        ).grid(row=6, column=1, sticky='w', pady=(8, 0))

        ttk.Label(frame, text='Initial urgency').grid(row=7, column=0, sticky='w')
        ttk.Combobox(
            frame,
            state='readonly',
            textvariable=self.urgency_var,
            values=[UrgencyTier.ORDINARY.value, UrgencyTier.BOOSTED.value, UrgencyTier.EXTREME.value],
            width=20,
        ).grid(row=7, column=1, sticky='w')

        ttk.Checkbutton(
            frame,
            text='Allow below-threshold setup moves required to reach target',
            variable=self.allow_below_var,
        ).grid(row=8, column=0, columnspan=2, sticky='w', pady=(8, 4))

        ttk.Label(frame, text='Note (optional)').grid(row=9, column=0, sticky='w')
        ttk.Entry(frame, textvariable=self.note_var, width=80).grid(row=10, column=0, columnspan=2, sticky='ew', pady=(2, 8))

        ttk.Label(frame, textvariable=self.error_var, foreground='red', wraplength=560).grid(row=11, column=0, columnspan=2, sticky='w')

        buttons = ttk.Frame(frame)
        buttons.grid(row=12, column=0, columnspan=2, sticky='e', pady=(10, 0))
        ttk.Button(buttons, text='Cancel', command=self.destroy).pack(side='right', padx=(8, 0))
        self.save_button = ttk.Button(buttons, text='Save', command=self._save)
        self.save_button.pack(side='right')

        self.fen_var.trace_add('write', lambda *_: self._update_side_to_move())
        self.predecessor_var.trace_add('write', lambda *_: self._clear_error())
        self._update_side_to_move()

    def _clear_error(self) -> None:
        self.error_var.set('')

    def _update_side_to_move(self) -> None:
        self._clear_error()
        fen = self.fen_var.get().strip()
        if not fen:
            self.side_to_move_var.set('—')
            return
        try:
            board = chess.Board(fen)
        except ValueError:
            self.side_to_move_var.set('invalid FEN')
            return
        self.side_to_move_var.set('white' if board.turn == chess.WHITE else 'black')

    def _save(self) -> None:
        fen = self.fen_var.get().strip()
        predecessor = self.predecessor_var.get().strip() or None
        try:
            validate_manual_target(
                target_fen=fen,
                predecessor_line_uci=predecessor,
                presentation_mode=self.presentation_mode_var.get().strip(),
                auto_resolve_predecessor=True,
            )
        except ValueError as exc:
            self.error_var.set(str(exc))
            return
        self.on_save(
            target_fen=fen,
            predecessor_line_uci=predecessor,
            urgency_tier=self.urgency_var.get().strip(),
            allow_below_threshold_reach=self.allow_below_var.get(),
            manual_presentation_mode=self.presentation_mode_var.get().strip(),
            manual_forced_player_color=self.forced_color_var.get().strip(),
            operator_note=self.note_var.get().strip() or None,
        )
        self.destroy()
