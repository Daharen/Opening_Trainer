from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..models import MoveHistoryEntry


class MoveListPanel(ttk.LabelFrame):
    def __init__(self, master):
        super().__init__(master, text='Move list')
        self.text = tk.Text(self, height=10, width=28, state='disabled', wrap='none')
        self.text.pack(fill='both', expand=True)

    def update_moves(self, moves: tuple[MoveHistoryEntry, ...]) -> None:
        lines: list[str] = []
        move_number = 1
        pending_white: str | None = None
        for entry in moves:
            if entry.side_to_move == 'white':
                pending_white = f'{move_number}. {entry.display_notation}'
            else:
                line = f'{pending_white or f"{move_number}..."} {entry.display_notation}'
                lines.append(line)
                pending_white = None
                move_number += 1
        if pending_white is not None:
            lines.append(pending_white)
        content = '\n'.join(lines) if lines else 'No moves yet.'
        self.text.configure(state='normal')
        self.text.delete('1.0', 'end')
        self.text.insert('1.0', content)
        self.text.configure(state='disabled')
