from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import chess

from ..review.manual_target import validate_manual_target
from ..review.models import UrgencyTier


class ManualTargetDialog(tk.Toplevel):
    def __init__(self, master, on_save):
        super().__init__(master)
        self.title('Add Manual Target')
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()
        self.on_save = on_save

        self.fen_var = tk.StringVar()
        self.side_to_move_var = tk.StringVar(value='—')
        self.predecessor_var = tk.StringVar()
        self.urgency_var = tk.StringVar(value=UrgencyTier.ORDINARY.value)
        self.allow_below_var = tk.BooleanVar(value=False)
        self.note_var = tk.StringVar()
        self.error_var = tk.StringVar(value='')

        frame = ttk.Frame(self, padding=12)
        frame.grid(row=0, column=0, sticky='nsew')

        ttk.Label(frame, text='Target FEN').grid(row=0, column=0, sticky='w')
        ttk.Entry(frame, textvariable=self.fen_var, width=80).grid(row=1, column=0, columnspan=2, sticky='ew', pady=(2, 8))
        ttk.Label(frame, text='Side to move').grid(row=2, column=0, sticky='w')
        ttk.Label(frame, textvariable=self.side_to_move_var).grid(row=2, column=1, sticky='w')

        ttk.Label(frame, text='Predecessor line (UCI, optional)').grid(row=3, column=0, sticky='w', pady=(8, 0))
        ttk.Entry(frame, textvariable=self.predecessor_var, width=80).grid(row=4, column=0, columnspan=2, sticky='ew', pady=(2, 8))

        ttk.Label(frame, text='Initial urgency').grid(row=5, column=0, sticky='w')
        ttk.Combobox(
            frame,
            state='readonly',
            textvariable=self.urgency_var,
            values=[UrgencyTier.ORDINARY.value, UrgencyTier.BOOSTED.value, UrgencyTier.EXTREME.value],
            width=20,
        ).grid(row=5, column=1, sticky='w')

        ttk.Checkbutton(
            frame,
            text='Allow below-threshold setup moves required to reach target',
            variable=self.allow_below_var,
        ).grid(row=6, column=0, columnspan=2, sticky='w', pady=(8, 4))

        ttk.Label(frame, text='Note (optional)').grid(row=7, column=0, sticky='w')
        ttk.Entry(frame, textvariable=self.note_var, width=80).grid(row=8, column=0, columnspan=2, sticky='ew', pady=(2, 8))

        ttk.Label(frame, textvariable=self.error_var, foreground='red', wraplength=560).grid(row=9, column=0, columnspan=2, sticky='w')

        buttons = ttk.Frame(frame)
        buttons.grid(row=10, column=0, columnspan=2, sticky='e', pady=(10, 0))
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
            validate_manual_target(target_fen=fen, predecessor_line_uci=predecessor)
        except ValueError as exc:
            self.error_var.set(str(exc))
            return
        self.on_save(
            target_fen=fen,
            predecessor_line_uci=predecessor,
            urgency_tier=self.urgency_var.get().strip(),
            allow_below_threshold_reach=self.allow_below_var.get(),
            operator_note=self.note_var.get().strip() or None,
        )
        self.destroy()
