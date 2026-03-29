from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from .manual_target_dialog import ManualTargetDialog


class ReviewInspector(ttk.Frame):
    columns = (
        'position',
        'side',
        'origin',
        'manual_mode',
        'manual_color',
        'manual_reach',
        'urgency',
        'frequency_state',
        'due',
        'srs_stage',
        'srs_due',
        'srs_last_result',
        'srs_lapses',
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
    column_labels = {column: column.replace('_', ' ').title() for column in columns}

    def __init__(self, master, session, refresh_callback, *, visible_columns: tuple[str, ...] | None = None):
        super().__init__(master)
        self.session = session
        self.refresh_callback = refresh_callback
        self.filter_var = tk.StringVar(value='all')
        self.visible_columns = tuple(column for column in (visible_columns or self.columns) if column in self.columns) or self.columns

        ttk.Combobox(
            self,
            textvariable=self.filter_var,
            values=['all', 'ordinary_review', 'boosted_review', 'extreme_urgency', 'manual_target'],
            state='readonly',
        ).pack(anchor='e')
        self.filter_var.trace_add('write', lambda *_: self.refresh())

        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill='both', expand=True)
        self.tree = ttk.Treeview(tree_frame, columns=self.columns, show='headings', height=8, displaycolumns=self.visible_columns)
        scrollbar = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        for column in self.columns:
            self.tree.heading(column, text=self.column_labels[column])
            self.tree.column(column, width=110, anchor='w')
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')

        button_row = ttk.Frame(self)
        button_row.pack(fill='x', pady=4)
        ttk.Button(button_row, text='Add Manual Target', command=self._open_manual_target_dialog).pack(side='left', padx=4)
        ttk.Button(button_row, text='Edit item', command=self._edit_item).pack(side='left', padx=4)
        ttk.Button(button_row, text='Delete item', command=self._delete_item).pack(side='left', padx=4)
        ttk.Button(button_row, text='Reset item', command=self._reset_item).pack(side='left', padx=4)

    def set_visible_columns(self, columns: tuple[str, ...] | list[str]) -> None:
        normalized = tuple(column for column in columns if column in self.columns) or self.columns
        self.visible_columns = normalized
        self.tree.configure(displaycolumns=self.visible_columns)

    def refresh(self):
        for row in self.tree.get_children():
            self.tree.delete(row)
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        tier = self.filter_var.get()
        if tier == 'manual_target':
            items = [item for item in items if item.origin_kind == 'manual_target']
        elif tier != 'all':
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
                    item.origin_kind,
                    item.manual_presentation_mode if item.origin_kind == 'manual_target' else '—',
                    item.manual_forced_player_color if item.origin_kind == 'manual_target' else '—',
                    item.allow_below_threshold_reach if item.origin_kind == 'manual_target' else '—',
                    item.urgency_tier,
                    item.frequency_state,
                    'due' if item.due_at_utc <= item.updated_at_utc else 'scheduled',
                    item.srs_stage_index,
                    item.srs_next_due_at_utc,
                    item.srs_last_result,
                    item.srs_lapse_count,
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

    def _open_manual_target_dialog(self):
        def _save_manual_target(**payload):
            try:
                self.session.add_manual_target(**payload)
            except ValueError as exc:
                messagebox.showerror('Manual target validation', str(exc))
                return
            self.refresh_callback()
            messagebox.showinfo('Manual target', 'Manual target item saved.')

        ManualTargetDialog(self, _save_manual_target, title='Add Manual Target')

    def _edit_item(self):
        item_id = self.tree.focus()
        if not item_id:
            return
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        item = next((candidate for candidate in items if candidate.review_item_id == item_id), None)
        if item is None:
            messagebox.showerror('Edit item', 'Selected item no longer exists.')
            return
        if item.origin_kind == 'manual_target':
            initial = {
                'target_fen': item.manual_target_fen or item.position_fen_normalized,
                'predecessor_line_uci': item.predecessor_line_uci or '',
                'urgency_tier': item.urgency_tier,
                'allow_below_threshold_reach': item.allow_below_threshold_reach,
                'manual_presentation_mode': item.manual_presentation_mode,
                'manual_forced_player_color': item.manual_forced_player_color,
                'operator_note': item.operator_note or '',
            }

            def _save_edit(**payload):
                try:
                    self.session.edit_review_item(item.review_item_id, **payload)
                except ValueError as exc:
                    messagebox.showerror('Edit manual target', str(exc))
                    return
                self.refresh_callback()
                messagebox.showinfo('Edit manual target', 'Manual target item updated.')

            ManualTargetDialog(self, _save_edit, title='Edit Manual Target', initial=initial)
            return

        urgency = tk.StringVar(value=item.urgency_tier)
        dialog = tk.Toplevel(self)
        dialog.title('Edit Review Item')
        dialog.transient(self)
        dialog.grab_set()
        frame = ttk.Frame(dialog, padding=12)
        frame.grid(row=0, column=0)
        ttk.Label(frame, text='Urgency').grid(row=0, column=0, sticky='w')
        ttk.Combobox(
            frame,
            state='readonly',
            textvariable=urgency,
            values=['ordinary_review', 'boosted_review', 'extreme_urgency'],
            width=20,
        ).grid(row=0, column=1, sticky='w')

        def _save_ordinary():
            self.session.edit_review_item(item.review_item_id, urgency_tier=urgency.get().strip())
            dialog.destroy()
            self.refresh_callback()

        buttons = ttk.Frame(frame)
        buttons.grid(row=1, column=0, columnspan=2, sticky='e', pady=(12, 0))
        ttk.Button(buttons, text='Cancel', command=dialog.destroy).pack(side='right', padx=(8, 0))
        ttk.Button(buttons, text='Save', command=_save_ordinary).pack(side='right')
