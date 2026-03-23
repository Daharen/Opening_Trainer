from __future__ import annotations

import tkinter as tk

from ..session_contracts import OutcomeModalContract


class OutcomeModal:
    def __init__(self, master: tk.Misc, contract: OutcomeModalContract, on_continue):
        self.window = tk.Toplevel(master)
        self.window.title(contract.headline)
        self.window.transient(master)
        self.window.grab_set()
        self.window.protocol('WM_DELETE_WINDOW', lambda: None)
        tk.Label(self.window, text=contract.headline, font=('TkDefaultFont', 16, 'bold')).pack(padx=16, pady=(16, 8))
        for line in [contract.summary, contract.reason, f'Preferred move: {contract.preferred_move or "—"}', f'Routing reason: {contract.routing_reason}', f'Next run: {contract.next_routing_reason}', f'Impact: {contract.impact_summary}']:
            tk.Label(self.window, text=line, anchor='w', justify='left', wraplength=420).pack(fill='x', padx=16, pady=2)
        tk.Button(self.window, text='Continue', command=lambda: self._close(on_continue)).pack(pady=16)

    def _close(self, on_continue):
        self.window.grab_release()
        self.window.destroy()
        on_continue()
