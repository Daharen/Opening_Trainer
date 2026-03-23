from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class StatusPanel(ttk.Frame):
    def __init__(self, master, compact: bool = False):
        super().__init__(master)
        self.compact = compact
        self.profile_var = tk.StringVar()
        self.bundle_var = tk.StringVar()
        self.routing_var = tk.StringVar()
        self.counts_var = tk.StringVar()
        style = ('TkDefaultFont', 10, 'bold') if compact else None
        for var in [self.profile_var, self.bundle_var, self.routing_var, self.counts_var]:
            ttk.Label(self, textvariable=var, anchor='w', font=style).pack(fill='x')

    def update_status(self, *, profile_name: str, bundle_summary: str, routing_summary: str, counts_summary: str):
        self.profile_var.set(f'Profile: {profile_name}')
        self.bundle_var.set(bundle_summary)
        self.routing_var.set(routing_summary)
        self.counts_var.set(counts_summary)
