from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from .board_setup_editor import BoardSetupEditorDialog
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
        'tier_membership',
        'tier_queue_position',
        'tier_capacity',
        'tier_active_size',
        'tier_waiting_size',
        'tier_round_seen',
        'tier_round_miss',
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
        self._focused_column_id: str | None = None

        self.filter_combo = ttk.Combobox(
            self,
            textvariable=self.filter_var,
            values=['all', 'ordinary_review', 'boosted_review', 'extreme_urgency', 'manual_target'],
            state='readonly',
        )
        self.filter_combo.pack(anchor='e')
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

        self.tree.bind('<ButtonRelease-1>', self._handle_cell_focus)
        self.tree.bind('<Button-3>', self._open_context_menu)
        self.tree.bind('<Control-c>', self._copy_with_shortcut)

        button_row = ttk.Frame(self)
        button_row.pack(fill='x', pady=4)
        ttk.Button(button_row, text='Add Manual Target', command=self._open_manual_target_dialog).pack(side='left', padx=4)
        ttk.Button(button_row, text='Edit Item', command=self._edit_item).pack(side='left', padx=4)
        ttk.Button(button_row, text='Board Edit', command=self._edit_item_in_board_setup).pack(side='left', padx=4)
        ttk.Button(button_row, text='Delete item', command=self._delete_item).pack(side='left', padx=4)
        ttk.Button(button_row, text='Reset item', command=self._reset_item).pack(side='left', padx=4)

    def _handle_cell_focus(self, event: tk.Event) -> None:
        column_token = self.tree.identify_column(event.x)
        if column_token.startswith('#'):
            try:
                idx = int(column_token[1:]) - 1
            except ValueError:
                self._focused_column_id = None
                return
            self._focused_column_id = self.visible_columns[idx] if 0 <= idx < len(self.visible_columns) else None

    def _open_context_menu(self, event: tk.Event) -> None:
        row = self.tree.identify_row(event.y)
        if row:
            self.tree.selection_set(row)
            self.tree.focus(row)
        self._handle_cell_focus(event)
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label='Copy Position', command=self._copy_position)
        menu.add_command(label='Copy Cell', command=self._copy_cell)
        menu.add_command(label='Copy Row', command=self._copy_row)
        menu.add_separator()
        menu.add_command(label='Edit Item', command=self._edit_item)
        menu.add_command(label='Edit in Board Setup', command=self._edit_item_in_board_setup)
        menu.tk_popup(event.x_root, event.y_root)

    def _copy_with_shortcut(self, _event=None):
        if self._focused_column_id:
            self._copy_cell()
        else:
            self._copy_position()
        return 'break'

    def _focused_item_and_values(self):
        item_id = self.tree.focus() or (self.tree.selection()[0] if self.tree.selection() else None)
        if not item_id:
            return None, None
        values = self.tree.item(item_id, 'values')
        return item_id, list(values)

    def _copy_text(self, text: str) -> None:
        if not text:
            return
        self.clipboard_clear()
        self.clipboard_append(text)
        self.update_idletasks()

    def _copy_position(self) -> None:
        item_id, values = self._focused_item_and_values()
        if not item_id or values is None:
            return
        position_idx = self.columns.index('position')
        self._copy_text(str(values[position_idx]))

    def _copy_cell(self) -> None:
        item_id, values = self._focused_item_and_values()
        if not item_id or values is None:
            return
        column = self._focused_column_id or 'position'
        if column not in self.columns:
            return
        self._copy_text(str(values[self.columns.index(column)]))

    def _copy_row(self) -> None:
        item_id, values = self._focused_item_and_values()
        if not item_id or values is None:
            return
        parts = []
        for column in self.visible_columns:
            value = values[self.columns.index(column)]
            parts.append(f'{column}={value}')
        self._copy_text('\t'.join(parts))

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
        controller_state = self.session.review_storage.load_router_state(self.session.active_profile_id) if hasattr(self.session.review_storage, 'load_router_state') else {}

        def _controller_for_item(item):
            category = 'D' if item.urgency_tier == 'ordinary_review' and item.hijack_stage == 'none' else ('B' if item.urgency_tier == 'boosted_review' else ('E' if item.urgency_tier == 'extreme_urgency' else None))
            if category is None:
                return None, None
            state = controller_state.get(category, {})
            active = list(state.get('active_deck', []))
            waiting = list(state.get('waiting_queue', []))
            if item.review_item_id in active:
                return category, ('active', active.index(item.review_item_id), state, active, waiting)
            if item.review_item_id in waiting:
                return category, ('waiting', waiting.index(item.review_item_id), state, active, waiting)
            return category, ('inactive', None, state, active, waiting)

        items.sort(key=lambda item: (tier_rank.get(item.urgency_tier, 3), item.due_at_utc, -item.consecutive_failures, item.last_seen_at_utc, item.review_item_id))
        for item in items:
            tier_category, controller = _controller_for_item(item)
            membership = controller[0] if controller else 'n/a'
            queue_position = controller[1] if controller else '—'
            state = controller[2] if controller else {}
            active = controller[3] if controller else []
            waiting = controller[4] if controller else []
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
                    membership,
                    queue_position,
                    state.get('capacity', '—'),
                    len(active),
                    len(waiting),
                    state.get('round_seen_count', '—'),
                    state.get('round_miss_count', '—'),
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

        ManualTargetDialog(
            self,
            _save_manual_target,
            title='Add Manual Target',
            predecessor_master_db_path=self.session.runtime_context.config.predecessor_master_db_path,
        )

    def open_board_setup_editor(self):
        def _save_manual_target(**payload):
            try:
                self.session.add_manual_target(**payload)
            except ValueError as exc:
                messagebox.showerror('Manual target validation', str(exc))
                return
            self.refresh_callback()
            messagebox.showinfo('Manual setup', 'Manual setup item saved.')

        BoardSetupEditorDialog(self, _save_manual_target, title='Create Manual Position')

    def _edit_item(self):
        item = self._selected_item_for_editing('Edit item')
        if item is None:
            return

        def _save_edit(**payload):
            try:
                updated = self.session.edit_review_item(item.review_item_id, **payload)
            except ValueError as exc:
                messagebox.showerror('Edit review item', str(exc))
                return
            self.refresh_callback()
            converted = item.origin_kind != 'manual_target' and updated.origin_kind == 'manual_target'
            message = 'Review item updated.'
            if converted:
                message = 'Review item updated and converted to manual-managed semantics.'
            messagebox.showinfo('Edit review item', message)

        ManualTargetDialog(
            self,
            _save_edit,
            title='Edit Review Item',
            initial=self._build_item_initial(item),
            predecessor_master_db_path=self.session.runtime_context.config.predecessor_master_db_path,
        )

    def _edit_item_in_board_setup(self):
        item = self._selected_item_for_editing('Board edit')
        if item is None:
            return

        def _save_edit(**payload):
            try:
                updated = self.session.edit_review_item(item.review_item_id, **payload)
            except ValueError as exc:
                messagebox.showerror('Edit review item', str(exc))
                return
            self.refresh_callback()
            converted = item.origin_kind != 'manual_target' and updated.origin_kind == 'manual_target'
            message = 'Review item updated.'
            if converted:
                message = 'Review item updated and converted to manual-managed semantics.'
            messagebox.showinfo('Edit review item', message)

        BoardSetupEditorDialog(self, _save_edit, title='Edit in Board Setup', initial=self._build_item_initial(item))

    def _selected_item_for_editing(self, context: str):
        item_id = self.tree.focus()
        if not item_id:
            return None
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        item = next((candidate for candidate in items if candidate.review_item_id == item_id), None)
        if item is None:
            messagebox.showerror(context, 'Selected item no longer exists.')
            return None
        return item

    @staticmethod
    def _build_item_initial(item):
        return {
            'target_fen': item.manual_target_fen or item.position_fen_normalized,
            'predecessor_line_uci': item.predecessor_line_uci or '',
            'urgency_tier': item.urgency_tier,
            'allow_below_threshold_reach': item.allow_below_threshold_reach,
            'manual_presentation_mode': item.manual_presentation_mode,
            'manual_forced_player_color': item.manual_forced_player_color,
            'operator_note': item.operator_note or '',
        }

    def apply_theme(self, *, palette: dict[str, str]) -> None:
        style = ttk.Style(self)
        style.configure('Inspector.TFrame', background=palette['panel_bg'])
        style.configure(
            'Inspector.Treeview',
            background=palette['surface_bg'],
            fieldbackground=palette['surface_bg'],
            foreground=palette['text_fg'],
            bordercolor=palette['border_color'],
        )
        style.configure(
            'Inspector.Treeview.Heading',
            background=palette['header_bg'],
            foreground=palette['text_fg'],
            bordercolor=palette['border_color'],
        )
        style.map('Inspector.Treeview', background=[('selected', palette['select_bg'])], foreground=[('selected', palette['text_fg'])])
        self.configure(style='Inspector.TFrame')
        self.tree.configure(style='Inspector.Treeview')
        self.filter_combo.configure(style='TCombobox')
