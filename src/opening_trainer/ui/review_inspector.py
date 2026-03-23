from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk


class ReviewInspector(ttk.Frame):
    columns = ('position', 'side', 'urgency', 'due', 'fails', 'successes', 'last_seen', 'next_due', 'routing', 'preview')

    def __init__(self, master, session, refresh_callback):
        super().__init__(master)
        self.session = session
        self.refresh_callback = refresh_callback
        self.filter_var = tk.StringVar(value='all')
        self.tree = ttk.Treeview(self, columns=self.columns, show='headings', height=8)
        for column in self.columns:
            self.tree.heading(column, text=column.replace('_', ' ').title())
            self.tree.column(column, width=110, anchor='w')
        ttk.Combobox(self, textvariable=self.filter_var, values=['all', 'ordinary_review', 'boosted_review', 'extreme_urgency'], state='readonly').pack(anchor='e')
        self.filter_var.trace_add('write', lambda *_: self.refresh())
        self.tree.pack(fill='both', expand=True)
        ttk.Button(self, text='Delete item', command=self._delete_item).pack(side='left', padx=4, pady=4)
        ttk.Button(self, text='Reset item', command=self._reset_item).pack(side='left', padx=4, pady=4)

    def refresh(self):
        for row in self.tree.get_children():
            self.tree.delete(row)
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        tier = self.filter_var.get()
        if tier != 'all':
            items = [item for item in items if item.urgency_tier == tier]
        items.sort(key=lambda item: (item.urgency_tier, item.due_at_utc, -item.consecutive_failures, item.last_seen_at_utc))
        for item in items:
            self.tree.insert('', 'end', iid=item.review_item_id, values=(item.position_key[:24], item.side_to_move, item.urgency_tier, 'due' if item.due_at_utc <= item.updated_at_utc else 'scheduled', item.consecutive_failures, item.consecutive_successes, item.last_seen_at_utc or '—', item.due_at_utc, item.last_routing_reason, item.line_preview_san[:36]))

    def _delete_item(self):
        item_id = self.tree.focus()
        if not item_id:
            return
        if messagebox.askyesno('Confirm delete', 'Delete the selected review item?'):
            items = [item for item in self.session.review_storage.load_items(self.session.active_profile_id) if item.review_item_id != item_id]
            self.session.review_storage.save_items(self.session.active_profile_id, items)
            self.refresh_callback()

    def _reset_item(self):
        item_id = self.tree.focus()
        if not item_id:
            return
        if messagebox.askyesno('Confirm reset', 'Reset urgency and mastery for the selected review item?'):
            items = self.session.review_storage.load_items(self.session.active_profile_id)
            for item in items:
                if item.review_item_id == item_id:
                    item.consecutive_failures = 0
                    item.consecutive_successes = 0
                    item.mastery_score = 0.0
                    item.stability_score = 0.0
                    item.urgency_tier = 'ordinary_review'
                    item.last_routing_reason = 'manual_reset'
            self.session.review_storage.save_items(self.session.active_profile_id, items)
            self.refresh_callback()
