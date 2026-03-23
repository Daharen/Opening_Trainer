from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..session_contracts import OutcomeModalContract


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
        tk.Label(container, text=contract.summary, font=('TkDefaultFont', 12, 'bold'), anchor='w', justify='left', wraplength=480).pack(fill='x', padx=16, pady=(0, 10))

        details = [
            f'Reason: {contract.reason}',
            f'Preferred move: {contract.preferred_move or "—"}',
            f'Routing reason: {contract.routing_reason}',
            f'Next routing reason: {contract.next_routing_reason}',
            f'{contract.impact_summary}',
        ]
        for line in details:
            tk.Label(container, text=line, anchor='w', justify='left', wraplength=480).pack(fill='x', padx=16, pady=2)

        ttk.Button(container, text='Continue', command=self._close).pack(pady=(18, 8))

        self.window.update_idletasks()
        self._center_over(master)
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self.window.grab_set()
        self.window.wait_visibility()
        self.window.wait_window()

    def _center_over(self, master: tk.Misc) -> None:
        self.window.update_idletasks()
        width = self.window.winfo_width() or 560
        height = self.window.winfo_height() or 280
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
