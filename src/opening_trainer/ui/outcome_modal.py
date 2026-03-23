from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import chess

from ..session_contracts import OutcomeModalContract
from .board_view import BoardView


class OutcomeModal:
    def __init__(self, master: tk.Misc, contract: OutcomeModalContract, on_continue):
        self._on_continue = on_continue
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

        if contract.review_boards:
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
        section = ttk.LabelFrame(container, text='What you should have played / What punishes this', padding=10)
        section.pack(fill='x', expand=True, padx=16, pady=(0, 12))
        boards_frame = ttk.Frame(section)
        boards_frame.pack(fill='both', expand=True)
        if len(contract.review_boards) >= 2:
            boards_frame.columnconfigure(0, weight=1)
            boards_frame.columnconfigure(1, weight=1)
        for index, board_contract in enumerate(contract.review_boards):
            row = 0 if len(contract.review_boards) >= 2 else index
            column = index if len(contract.review_boards) >= 2 else 0
            card = ttk.Frame(boards_frame, padding=6)
            card.grid(row=row, column=column, sticky='nsew', padx=6, pady=6)
            ttk.Label(card, text=board_contract.title).pack(anchor='w', pady=(0, 4))
            ttk.Label(card, text=f'{board_contract.arrow_label}: {board_contract.move_label or "—"}').pack(anchor='w', pady=(0, 6))
            board = BoardView(card, board_size=220, min_board_size=180)
            board.pack()
            board.set_arrow(board_contract.arrow_move_uci, board_contract.arrow_color)
            board.render(chess.Board(board_contract.board_fen), chess.Board(board_contract.board_fen).turn)

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
