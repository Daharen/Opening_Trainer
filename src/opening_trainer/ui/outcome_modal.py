from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import chess

from ..session_contracts import OutcomeModalContract
from .board_view import BoardView


class OutcomeModal:
    def __init__(self, master: tk.Misc, contract: OutcomeModalContract, on_continue):
        self._on_continue = on_continue
        self._punishment_slides = contract.punishment_slides
        self._punishment_step = 0
        self._punishment_board: BoardView | None = None
        self._punishment_label_var = tk.StringVar(value='')
        self.window = tk.Toplevel(master)
        self.window.title(contract.headline)
        self.window.transient(master)
        self.window.resizable(False, False)
        self.window.protocol('WM_DELETE_WINDOW', self._close)
        self.window.configure(padx=20, pady=20)

        container = ttk.Frame(self.window, padding=8)
        container.pack(fill='both', expand=True)

        headline_color = '#1b5e20' if contract.headline == 'SUCCESS' else '#b71c1c'
        tk.Label(container, text=contract.headline, font=('TkDefaultFont', 24, 'bold'), fg=headline_color).pack(padx=16, pady=(8, 12))
        tk.Label(container, text=contract.summary, font=('TkDefaultFont', 12, 'bold'), anchor='w', justify='left', wraplength=560).pack(fill='x', padx=16, pady=(0, 10))

        if contract.review_boards or contract.punishment_slides:
            self._build_review_boards(container, contract)

        details = [
            f'Reason: {contract.reason}',
            f'Preferred move: {contract.preferred_move or "—"}',
            f'Routing reason: {contract.routing_reason}',
            f'Next routing reason: {contract.next_routing_reason}',
            f'{contract.impact_summary}',
        ]
        for line in details:
            tk.Label(container, text=line, anchor='w', justify='left', wraplength=560).pack(fill='x', padx=16, pady=2)

        ttk.Button(container, text='Continue', command=self._close).pack(pady=(18, 8))

        self.window.update_idletasks()
        self._center_over(master)
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self.window.grab_set()
        self.window.wait_visibility()
        self.window.wait_window()

    def _build_review_boards(self, container: ttk.Frame, contract: OutcomeModalContract) -> None:
        section = ttk.LabelFrame(container, text='What you should have played / How this is punished', padding=10)
        section.pack(fill='x', expand=True, padx=16, pady=(0, 12))
        boards_frame = ttk.Frame(section)
        boards_frame.pack(fill='both', expand=True)
        has_recommendation = bool(contract.review_boards)
        has_punishment = bool(contract.punishment_slides)
        if has_recommendation and has_punishment:
            boards_frame.columnconfigure(0, weight=1)
            boards_frame.columnconfigure(1, weight=1)
        if has_recommendation:
            board_contract = contract.review_boards[0]
            card = ttk.Frame(boards_frame, padding=6)
            card.grid(row=0, column=0, sticky='nsew', padx=6, pady=6)
            ttk.Label(card, text=board_contract.title).pack(anchor='w', pady=(0, 4))
            ttk.Label(card, text=f'{board_contract.arrow_label}: {board_contract.move_label or "—"}', wraplength=260, justify='left').pack(anchor='w', pady=(0, 6))
            board = BoardView(card, board_size=220, min_board_size=180)
            board.pack()
            board.set_arrows([(arrow.move_uci, arrow.color, arrow.width_scale) for arrow in board_contract.arrows])
            board.render(chess.Board(board_contract.board_fen), board_contract.player_color)
        if has_punishment:
            column = 1 if has_recommendation else 0
            card = ttk.Frame(boards_frame, padding=6)
            card.grid(row=0, column=column, sticky='nsew', padx=6, pady=6)
            ttk.Label(card, text='How this is punished (best play)').pack(anchor='w', pady=(0, 4))
            ttk.Label(card, textvariable=self._punishment_label_var).pack(anchor='w', pady=(0, 6))
            self._punishment_board = BoardView(card, board_size=220, min_board_size=180)
            self._punishment_board.pack()
            controls = ttk.Frame(card)
            controls.pack(anchor='w', pady=(8, 0))
            ttk.Button(controls, text='◀', command=lambda: self._step_punishment(-1)).pack(side='left', padx=(0, 6))
            ttk.Button(controls, text='▶', command=lambda: self._step_punishment(1)).pack(side='left')
            self._render_punishment_step(0)

    def _step_punishment(self, delta: int) -> None:
        if not self._punishment_slides:
            return
        self._render_punishment_step((self._punishment_step + delta) % len(self._punishment_slides))

    def _render_punishment_step(self, step: int) -> None:
        if not self._punishment_slides or self._punishment_board is None:
            return
        self._punishment_step = step
        slide = self._punishment_slides[step]
        self._punishment_label_var.set(f'Step {slide.step_index}/{slide.total_steps}: {slide.current_move_san}')
        self._punishment_board.set_arrows([(slide.current_move_uci, '#c62828', 0.95)])
        self._punishment_board.render(chess.Board(slide.board_fen), slide.player_color)

    def _center_over(self, master: tk.Misc) -> None:
        self.window.update_idletasks()
        width = self.window.winfo_width() or 640
        height = self.window.winfo_height() or 320
        try:
            master_x = master.winfo_rootx()
            master_y = master.winfo_rooty()
            master_width = master.winfo_width()
            master_height = master.winfo_height()
        except tk.TclError:
            master_x = master_y = 100
            master_width = 1024
            master_height = 768
        x = master_x + max((master_width - width) // 2, 0)
        y = master_y + max((master_height - height) // 2, 0)
        self.window.geometry(f'{width}x{height}+{x}+{y}')

    def _close(self):
        if self.window.winfo_exists():
            self.window.grab_release()
            self.window.destroy()
        self._on_continue()
