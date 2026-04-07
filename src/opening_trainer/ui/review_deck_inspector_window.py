from __future__ import annotations

import tkinter as tk
from tkinter import ttk


PALETTE = [
    ('red', '#d32f2f', 'white'),
    ('orange', '#f57c00', 'white'),
    ('yellow', '#ffeb3b', 'black'),
    ('green', '#43a047', 'white'),
    ('blue', '#1e88e5', 'white'),
    ('purple', '#8e24aa', 'white'),
    ('pink', '#f48fb1', 'black'),
    ('brown', '#6d4c41', 'white'),
    ('cyan', '#26c6da', 'black'),
    ('gray', '#9e9e9e', 'black'),
]


class ReviewDeckInspectorWindow:
    HISTORY_LIMIT = 400

    def __init__(self, master: tk.Misc, session, *, on_close=None) -> None:
        self.session = session
        self.on_close = on_close
        self.window = tk.Toplevel(master)
        self.window.title('Review Deck Inspector')
        self.window.geometry('1200x700')
        self.window.rowconfigure(1, weight=1)
        self.window.columnconfigure(0, weight=1)

        self._row_palette: dict[str, tuple[str, str, str]] = {}
        self._history_rows: list[tuple[str, str, str]] = []
        self._active_order: list[str] = []
        self._poll_handle = None

        self._build_layout()
        self.session.register_review_deck_observer(self._on_runtime_event)
        self.window.protocol('WM_DELETE_WINDOW', self.close)
        self.refresh_from_snapshot()
        self._schedule_poll()

    def focus(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def close(self) -> None:
        if self._poll_handle is not None:
            try:
                self.window.after_cancel(self._poll_handle)
            except Exception:
                pass
            self._poll_handle = None
        self.session.unregister_review_deck_observer(self._on_runtime_event)
        self.window.destroy()
        if callable(self.on_close):
            self.on_close()

    def _build_layout(self) -> None:
        top = ttk.LabelFrame(self.window, text='Active Review Stack', padding=8)
        top.grid(row=0, column=0, sticky='nsew', padx=8, pady=(8, 4))
        top.rowconfigure(0, weight=1)
        top.columnconfigure(0, weight=1)

        self.active_table = ttk.Treeview(
            top,
            show='headings',
            columns=('color', 'position', 'frequency', 'cards', 'fails', 'success_streak'),
            height=8,
        )
        for column, label, width in (
            ('color', 'Color', 70),
            ('position', 'Training Position in Stack Currently (Position)', 500),
            ('frequency', 'Frequency State', 170),
            ('cards', '# Cards in Deck', 100),
            ('fails', 'Fails', 70),
            ('success_streak', 'Success Streak', 110),
        ):
            self.active_table.heading(column, text=label)
            self.active_table.column(column, width=width, anchor='w')
        active_scroll = ttk.Scrollbar(top, orient='vertical', command=self.active_table.yview)
        self.active_table.configure(yscrollcommand=active_scroll.set)
        self.active_table.grid(row=0, column=0, sticky='nsew')
        active_scroll.grid(row=0, column=1, sticky='ns')

        lower = ttk.Frame(self.window)
        lower.grid(row=1, column=0, sticky='nsew', padx=8, pady=(4, 8))
        lower.columnconfigure(0, weight=4)
        lower.columnconfigure(1, weight=2)
        lower.rowconfigure(0, weight=1)

        history_frame = ttk.LabelFrame(lower, text='Running History (Corpus rows are black)', padding=8)
        history_frame.grid(row=0, column=0, sticky='nsew', padx=(0, 4))
        history_frame.rowconfigure(0, weight=1)
        history_frame.columnconfigure(0, weight=1)

        self.history_text = tk.Text(history_frame, wrap='none', height=20)
        history_scroll = ttk.Scrollbar(history_frame, orient='vertical', command=self.history_text.yview)
        self.history_text.configure(yscrollcommand=history_scroll.set)
        self.history_text.grid(row=0, column=0, sticky='nsew')
        history_scroll.grid(row=0, column=1, sticky='ns')
        self.history_text.configure(state='disabled')

        summary_frame = ttk.LabelFrame(lower, text='Live Summary', padding=8)
        summary_frame.grid(row=0, column=1, sticky='nsew')
        for idx in range(2):
            summary_frame.columnconfigure(idx, weight=1)
        self.summary_vars: dict[str, tk.StringVar] = {}
        labels = [
            ('due_streak', 'Due Streak'),
            ('boosted_streak', 'Boosted Streak'),
            ('urgent_streak', 'Urgent Streak'),
            ('due_misses', 'Due Misses'),
            ('boosted_misses', 'Boosted Misses'),
            ('urgent_misses', 'Urgent Misses'),
            ('due_active', 'Due Active'),
            ('due_capacity', 'Due Capacity'),
            ('boosted_active', 'Boosted Active'),
            ('urgent_active', 'Urgent Active'),
            ('waiting_due', 'Due Waiting'),
            ('waiting_boosted', 'Boosted Waiting'),
            ('waiting_urgent', 'Urgent Waiting'),
            ('deck_cursor', 'Deck Cursor'),
            ('last_mutation_reason', 'Last Mutation Reason'),
            ('last_routing_action', 'Last Routing Action'),
        ]
        for row, (key, label) in enumerate(labels):
            var = tk.StringVar(value='—')
            self.summary_vars[key] = var
            ttk.Label(summary_frame, text=f'{label}:').grid(row=row, column=0, sticky='w', pady=1)
            ttk.Label(summary_frame, textvariable=var).grid(row=row, column=1, sticky='w', pady=1)

    def _schedule_poll(self) -> None:
        self._poll_handle = self.window.after(1200, self._poll_refresh)

    def _poll_refresh(self) -> None:
        self._poll_handle = None
        self.refresh_from_snapshot()
        self._schedule_poll()

    def _on_runtime_event(self, event: dict[str, object]) -> None:
        event_type = str(event.get('event_type', ''))
        snapshot = event.get('snapshot') if isinstance(event.get('snapshot'), dict) else None
        if event_type == 'training_card_consumed':
            item_id = str(event.get('review_item_id') or '')
            label = self._label_for_item_id(item_id, snapshot)
            bg, fg = self._color_for_item_id(item_id)
            self._append_history(label, bg, fg)
        elif event_type == 'corpus_move_emitted':
            self._append_history('Corpus', '#000000', 'white')
        self.refresh_from_snapshot(snapshot=snapshot)

    def refresh_from_snapshot(self, snapshot: dict[str, object] | None = None) -> None:
        current = snapshot if isinstance(snapshot, dict) else self.session.review_deck_inspector_snapshot()
        active_rows = list(current.get('active_rows', []))
        self._refresh_active_table(active_rows)

        summary = dict(current.get('summary', {}))
        waiting = dict(current.get('waiting_sizes', {}))
        for key in ('due_streak', 'boosted_streak', 'urgent_streak', 'due_misses', 'boosted_misses', 'urgent_misses', 'due_active', 'due_capacity', 'boosted_active', 'urgent_active', 'deck_cursor', 'last_mutation_reason', 'last_routing_action'):
            self.summary_vars[key].set(str(summary.get(key, '—')))
        self.summary_vars['waiting_due'].set(str(waiting.get('due', '—')))
        self.summary_vars['waiting_boosted'].set(str(waiting.get('boosted', '—')))
        self.summary_vars['waiting_urgent'].set(str(waiting.get('urgent', '—')))

    def _refresh_active_table(self, active_rows: list[dict[str, object]]) -> None:
        for row_id in self.active_table.get_children(''):
            self.active_table.delete(row_id)
        ordered_ids = [str(row.get('review_item_id')) for row in active_rows]
        self._active_order = ordered_ids
        self._reconcile_palette(ordered_ids)
        for row in active_rows:
            item_id = str(row.get('review_item_id'))
            bg, fg = self._color_for_item_id(item_id)
            tag_name = f'active_{item_id}'
            self.active_table.tag_configure(tag_name, background=bg, foreground=fg)
            frequency_display = str(row.get('frequency_state', ''))
            if frequency_display == 'extreme_urgency':
                frequency_display = 'urgent_review'
            self.active_table.insert(
                '',
                'end',
                values=(
                    self._color_name_for_item_id(item_id),
                    row.get('position', '—'),
                    frequency_display,
                    row.get('deck_cards', '—'),
                    row.get('fails', '—'),
                    row.get('success_streak', '—'),
                ),
                tags=(tag_name,),
            )

    def _reconcile_palette(self, ordered_ids: list[str]) -> None:
        for item_id in list(self._row_palette.keys()):
            if item_id not in ordered_ids:
                self._row_palette.pop(item_id, None)
        used_colors = {name for name, _bg, _fg in self._row_palette.values()}
        next_palette_idx = 0
        for item_id in ordered_ids:
            if item_id in self._row_palette:
                continue
            while PALETTE[next_palette_idx % len(PALETTE)][0] in used_colors and len(used_colors) < len(PALETTE):
                next_palette_idx += 1
            color = PALETTE[next_palette_idx % len(PALETTE)]
            self._row_palette[item_id] = color
            used_colors.add(color[0])
            next_palette_idx += 1

    def _color_for_item_id(self, item_id: str) -> tuple[str, str]:
        color = self._row_palette.get(item_id)
        if color is None:
            self._reconcile_palette(self._active_order)
            color = self._row_palette.get(item_id, PALETTE[0])
        return color[1], color[2]

    def _color_name_for_item_id(self, item_id: str) -> str:
        return self._row_palette.get(item_id, PALETTE[0])[0]

    def _label_for_item_id(self, item_id: str, snapshot: dict[str, object] | None) -> str:
        rows = []
        if isinstance(snapshot, dict):
            rows = list(snapshot.get('active_rows', []))
        if not rows:
            rows = list(self.session.review_deck_inspector_snapshot().get('active_rows', []))
        for row in rows:
            if str(row.get('review_item_id')) == item_id:
                return str(row.get('position') or item_id[:12])
        return item_id[:12] if item_id else 'Training'

    def _append_history(self, text: str, bg: str, fg: str) -> None:
        self._history_rows.append((text, bg, fg))
        if len(self._history_rows) > self.HISTORY_LIMIT:
            self._history_rows = self._history_rows[-self.HISTORY_LIMIT :]
        self.history_text.configure(state='normal')
        self.history_text.delete('1.0', tk.END)
        for idx, (entry_text, row_bg, row_fg) in enumerate(self._history_rows):
            tag = f'history_{idx}'
            self.history_text.insert(tk.END, f'{entry_text}\n', tag)
            self.history_text.tag_configure(tag, background=row_bg, foreground=row_fg)
        self.history_text.configure(state='disabled')
        self.history_text.see(tk.END)
