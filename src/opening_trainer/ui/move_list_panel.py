from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..models import MoveHistoryEntry


class MoveListPanel(ttk.LabelFrame):
    def __init__(self, master):
        super().__init__(master, text='Move list')
        self.opening_name_var = tk.StringVar(value='')
        self.opening_name_label = ttk.Label(self, textvariable=self.opening_name_var)
        self.opening_name_label.pack(fill='x', padx=6, pady=(2, 4))
        self.text = tk.Text(self, height=10, width=28, state='disabled', wrap='none')
        self.text.pack(fill='both', expand=True)
        self._is_dark_theme = False

    def apply_theme(self, palette: dict[str, str], *, dark: bool) -> None:
        self._is_dark_theme = dark
        self.configure(style='MoveList.TLabelframe')
        self.opening_name_label.configure(style='MoveListHeader.TLabel')
        self.text.configure(
            bg=palette['field_bg'],
            fg=palette['text_fg'],
            insertbackground=palette['text_fg'],
            selectbackground=palette['selection_bg'],
            selectforeground=palette['text_fg'],
            relief='flat',
            highlightthickness=1,
            highlightbackground=palette['border_color'],
            highlightcolor=palette['accent_color'],
        )

    def update_opening_name(self, opening_name: str | None) -> None:
        if isinstance(opening_name, str) and opening_name.strip():
            self.opening_name_var.set(f'Opening: {opening_name.strip()}')
        else:
            self.opening_name_var.set('')

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
