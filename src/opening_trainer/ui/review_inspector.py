from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk


class ReviewInspector(ttk.Frame):
    columns = (
        'position',
        'side',
        'urgency',
        'due',
        'fails',
        'success_streak',
        'freq_retired',
        'hijack_stage',
        'hijack_pass_ticker',
        'dormant',
        'avoidance_count',
        'anchor_count',
        'stubborn_state',
        'skipped_slots',
        'last_seen',
        'next_due',
        'routing',
    )

    def __init__(self, master, session, refresh_callback):
        super().__init__(master)
        self.session = session
        self.refresh_callback = refresh_callback
        self.filter_var = tk.StringVar(value='all')

        ttk.Combobox(self, textvariable=self.filter_var, values=['all', 'ordinary_review', 'boosted_review', 'extreme_urgency'], state='readonly').pack(anchor='e')
        self.filter_var.trace_add('write', lambda *_: self.refresh())

        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill='both', expand=True)
        self.tree = ttk.Treeview(tree_frame, columns=self.columns, show='headings', height=8)
        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        for column in self.columns:
            self.tree.heading(column, text=column.replace('_', ' ').title())
            self.tree.column(column, width=110, anchor='w')
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        button_row = ttk.Frame(self)
        button_row.pack(fill='x', pady=4)
        ttk.Button(button_row, text='Delete item', command=self._delete_item).pack(side='left', padx=4)
        ttk.Button(button_row, text='Reset item', command=self._reset_item).pack(side='left', padx=4)

    def refresh(self):
        for row in self.tree.get_children():
            self.tree.delete(row)
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        tier = self.filter_var.get()
        if tier != 'all':
            items = [item for item in items if item.urgency_tier == tier]
        tier_rank = {'extreme_urgency': 0, 'boosted_review': 1, 'ordinary_review': 2}
        items.sort(key=lambda item: (tier_rank.get(item.urgency_tier, 3), item.due_at_utc, -item.consecutive_failures, item.last_seen_at_utc, item.review_item_id))
        for item in items:
            self.tree.insert(
                '',
                'end',
                iid=item.review_item_id,
                values=(
                    item.position_key[:24],
                    item.side_to_move,
                    item.urgency_tier,
                    'due' if item.due_at_utc <= item.updated_at_utc else 'scheduled',
                    item.consecutive_failures,
                    item.success_streak,
                    item.frequency_retired_for_current_due_cycle,
                    item.hijack_stage,
                    item.hijack_pass_ticker,
                    item.dormant,
                    item.avoidance_count,
                    len(item.canonical_anchor_positions),
                    item.stubborn_extreme_state,
                    item.skipped_review_slots,
                    item.last_seen_at_utc or '—',
                    item.due_at_utc,
                    item.last_routing_reason,
                ),
            )

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
                    item.success_streak = 0
                    item.mastery_score = 0.0
                    item.stability_score = 0.0
                    item.urgency_tier = 'ordinary_review'
                    item.frequency_retired_for_current_due_cycle = False
                    item.stubborn_extreme_state = 'none'
                    item.stubborn_extra_repeat_consumed_until_success = False
                    item.skipped_review_slots = 0
                    item.hijack_stage = 'none'
                    item.hijack_pass_ticker = 0
                    item.dormant = False
                    item.avoidance_count = 0
                    item.last_hijack_routing_source = 'manual_reset'
                    item.last_routing_reason = 'manual_reset'
            self.session.review_storage.save_items(self.session.active_profile_id, items)
            self.refresh_callback()
