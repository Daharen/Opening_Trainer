from __future__ import annotations

from dataclasses import dataclass, field
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

@dataclass
class _InspectorLiveState:
    active_order: list[str] = field(default_factory=list)
    color_by_item_id: dict[str, tuple[str, str, str]] = field(default_factory=dict)
    position_by_item_id: dict[str, str] = field(default_factory=dict)
    history_rows: list[dict[str, str]] = field(default_factory=list)
    pending_training_history_by_item_id: dict[str, list[str]] = field(default_factory=dict)
    history_row_serial: int = 0
    runtime_events: list[dict[str, object]] = field(default_factory=list)
    last_processed_event_index: int = 0
    last_seen_deck_cursor: object = None
    latest_summary_snapshot: dict[str, object] = field(default_factory=dict)
    placeholder_visible: bool = False


class _HoverTooltip:
    def __init__(self, widget: tk.Widget, text_provider) -> None:
        self.widget = widget
        self.text_provider = text_provider
        self.tooltip_window: tk.Toplevel | None = None
        self.widget.bind('<Enter>', self._on_enter, add='+')
        self.widget.bind('<Leave>', self._on_leave, add='+')

    def _on_enter(self, _event=None) -> None:
        text = str(self.text_provider() or '').strip()
        if not text:
            return
        x = self.widget.winfo_rootx() + 10
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 2
        self.tooltip_window = tk.Toplevel(self.widget)
        self.tooltip_window.wm_overrideredirect(True)
        self.tooltip_window.wm_geometry(f'+{x}+{y}')
        label = tk.Label(
            self.tooltip_window,
            text=text,
            justify='left',
            background='#ffffe0',
            relief='solid',
            borderwidth=1,
            padx=6,
            pady=4,
        )
        label.pack()

    def _on_leave(self, _event=None) -> None:
        if self.tooltip_window is not None:
            self.tooltip_window.destroy()
            self.tooltip_window = None


class ReviewDeckInspectorWindow:
    HISTORY_LIMIT = 400
    POLL_MS = 350

    def __init__(self, master: tk.Misc, session, *, on_close=None) -> None:
        self.session = session
        self.on_close = on_close
        self.window = tk.Toplevel(master)
        self.window.title('Review Deck Inspector')
        self.window.geometry('1200x700')
        self.window.rowconfigure(1, weight=1)
        self.window.columnconfigure(0, weight=1)

        self._live_state = _InspectorLiveState()
        self._poll_handle = None

        self._build_layout()
        self._show_empty_history_placeholder()
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

        self.history_table = ttk.Treeview(
            history_frame,
            show='headings',
            columns=('source', 'position', 'result'),
            height=20,
        )
        for column, label, width in (
            ('source', 'Source', 120),
            ('position', 'Position', 420),
            ('result', 'Result', 80),
        ):
            self.history_table.heading(column, text=label)
            self.history_table.column(column, width=width, anchor='w')
        history_scroll = ttk.Scrollbar(history_frame, orient='vertical', command=self.history_table.yview)
        self.history_table.configure(yscrollcommand=history_scroll.set)
        self.history_table.grid(row=0, column=0, sticky='nsew')
        history_scroll.grid(row=0, column=1, sticky='ns')
        self._history_placeholder_iid = 'history_placeholder'

        summary_frame = ttk.LabelFrame(lower, text='Live Summary', padding=8)
        summary_frame.grid(row=0, column=1, sticky='nsew')
        summary_frame.rowconfigure(0, weight=1)
        summary_frame.columnconfigure(0, weight=1)
        self.summary_canvas = tk.Canvas(summary_frame, highlightthickness=0)
        summary_scroll = ttk.Scrollbar(summary_frame, orient='vertical', command=self.summary_canvas.yview)
        self.summary_canvas.configure(yscrollcommand=summary_scroll.set)
        self.summary_canvas.grid(row=0, column=0, sticky='nsew')
        summary_scroll.grid(row=0, column=1, sticky='ns')
        self.summary_content = ttk.Frame(self.summary_canvas)
        self.summary_canvas_window = self.summary_canvas.create_window((0, 0), window=self.summary_content, anchor='nw')
        self.summary_content.bind('<Configure>', lambda _event: self.summary_canvas.configure(scrollregion=self.summary_canvas.bbox('all')))
        self.summary_canvas.bind(
            '<Configure>',
            lambda event: self.summary_canvas.itemconfigure(self.summary_canvas_window, width=event.width),
        )
        self.summary_vars: dict[str, tk.StringVar] = {}
        labels = [
            ('due_streak', 'Due Streak'),
            ('boosted_streak', 'Boosted Streak'),
            ('urgent_streak', 'Urgent Streak'),
            ('separator_1', '────────'),
            ('due_misses', 'Due Misses'),
            ('boosted_misses', 'Boosted Misses'),
            ('urgent_misses', 'Urgent Misses'),
            ('separator_2', '────────'),
            ('due_active', 'Due Active'),
            ('boosted_active', 'Boosted Active'),
            ('urgent_active', 'Urgent Active'),
            ('separator_3', '────────'),
            ('due_capacity', 'Due Capacity'),
            ('boosted_capacity', 'Boosted Capacity'),
            ('urgent_capacity', 'Urgent Capacity'),
            ('separator_4', '────────'),
            ('due_underfill', 'Due Underfill'),
            ('boosted_underfill', 'Boosted Underfill'),
            ('urgent_underfill', 'Urgent Underfill'),
            ('separator_5', '────────'),
            ('waiting_due', 'Due Waiting'),
            ('waiting_boosted', 'Boosted Waiting'),
            ('waiting_urgent', 'Urgent Waiting'),
            ('separator_6', '────────'),
            ('training_share', 'Training %'),
            ('corpus_share', 'Corpus %'),
            ('separator_7', '────────'),
            ('deck_cursor', 'Deck Cursor'),
            ('last_mutation_reason', 'Last Mutation Reason'),
            ('last_routing_action', 'Last Routing Action'),
        ]
        self._training_share_row_widget: ttk.Label | None = None
        self._latest_share_breakdown: dict[str, object] = {}
        for row, (key, label) in enumerate(labels):
            var = tk.StringVar(value='—')
            self.summary_vars[key] = var
            if key.startswith('separator_'):
                ttk.Label(self.summary_content, text=label).grid(row=row, column=0, columnspan=2, sticky='ew', pady=1)
            else:
                ttk.Label(self.summary_content, text=f'{label}:').grid(row=row, column=0, sticky='w', pady=1)
                value_label = ttk.Label(self.summary_content, textvariable=var)
                value_label.grid(row=row, column=1, sticky='w', pady=1)
                if key == 'training_share':
                    self._training_share_row_widget = value_label
        if self._training_share_row_widget is not None:
            self._training_share_tooltip = _HoverTooltip(self._training_share_row_widget, self._training_share_tooltip_text)

    def _schedule_poll(self) -> None:
        self._poll_handle = self.window.after(self.POLL_MS, self._poll_refresh)

    def _poll_refresh(self) -> None:
        self._poll_handle = None
        self.refresh_from_snapshot()
        self._schedule_poll()

    def _on_runtime_event(self, event: dict[str, object]) -> None:
        self._live_state.runtime_events.append(dict(event))
        self.refresh_from_snapshot(snapshot=event.get('snapshot') if isinstance(event.get('snapshot'), dict) else None)

    def refresh_from_snapshot(self, snapshot: dict[str, object] | None = None) -> None:
        current = snapshot if isinstance(snapshot, dict) else self.session.review_deck_inspector_snapshot()
        active_rows = list(current.get('active_rows', []))
        self._refresh_active_table(active_rows)
        self._append_new_history_events()

        summary = dict(current.get('summary', {}))
        self._live_state.latest_summary_snapshot = summary
        self._live_state.last_seen_deck_cursor = summary.get('deck_cursor')
        waiting = dict(current.get('waiting_sizes', {}))
        for key in (
            'due_streak',
            'boosted_streak',
            'urgent_streak',
            'due_misses',
            'boosted_misses',
            'urgent_misses',
            'due_active',
            'boosted_active',
            'urgent_active',
            'due_capacity',
            'boosted_capacity',
            'urgent_capacity',
            'due_underfill',
            'boosted_underfill',
            'urgent_underfill',
            'training_share',
            'corpus_share',
            'deck_cursor',
            'last_mutation_reason',
            'last_routing_action',
        ):
            if key in {'training_share', 'corpus_share'}:
                self.summary_vars[key].set(self._format_pct(current.get(key)))
            else:
                self.summary_vars[key].set(str(summary.get(key, '—')))
        self._latest_share_breakdown = dict(current.get('share_breakdown', {}))
        self.summary_vars['waiting_due'].set(str(waiting.get('due', '—')))
        self.summary_vars['waiting_boosted'].set(str(waiting.get('boosted', '—')))
        self.summary_vars['waiting_urgent'].set(str(waiting.get('urgent', '—')))

    def _refresh_active_table(self, active_rows: list[dict[str, object]]) -> None:
        for row_id in self.active_table.get_children(''):
            self.active_table.delete(row_id)
        ordered_ids = [str(row.get('review_item_id')) for row in active_rows]
        self._live_state.active_order = ordered_ids
        self._reconcile_palette(ordered_ids)
        for row in active_rows:
            item_id = str(row.get('review_item_id'))
            self._live_state.position_by_item_id[item_id] = str(row.get('position') or item_id[:12])
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
        for item_id in list(self._live_state.color_by_item_id.keys()):
            if item_id not in ordered_ids:
                self._live_state.color_by_item_id.pop(item_id, None)
        used_colors = {name for name, _bg, _fg in self._live_state.color_by_item_id.values()}
        next_palette_idx = 0
        for item_id in ordered_ids:
            if item_id in self._live_state.color_by_item_id:
                continue
            while PALETTE[next_palette_idx % len(PALETTE)][0] in used_colors and len(used_colors) < len(PALETTE):
                next_palette_idx += 1
            color = PALETTE[next_palette_idx % len(PALETTE)]
            self._live_state.color_by_item_id[item_id] = color
            used_colors.add(color[0])
            next_palette_idx += 1

    def _color_for_item_id(self, item_id: str) -> tuple[str, str]:
        color = self._live_state.color_by_item_id.get(item_id)
        if color is None:
            self._reconcile_palette(self._live_state.active_order)
            color = self._live_state.color_by_item_id.get(item_id, PALETTE[0])
        return color[1], color[2]

    def _color_name_for_item_id(self, item_id: str) -> str:
        return self._live_state.color_by_item_id.get(item_id, PALETTE[0])[0]

    def _label_for_item_id(self, item_id: str, snapshot: dict[str, object] | None) -> str:
        rows = []
        if isinstance(snapshot, dict):
            rows = list(snapshot.get('active_rows', []))
        if not rows:
            rows = list(self.session.review_deck_inspector_snapshot().get('active_rows', []))
        for row in rows:
            if str(row.get('review_item_id')) == item_id:
                return str(row.get('position') or item_id[:12])
        return self._live_state.position_by_item_id.get(item_id, item_id[:12] if item_id else 'Training')

    def _append_new_history_events(self) -> None:
        events = list(self._live_state.runtime_events)
        max_seen = self._live_state.last_processed_event_index
        for event in events:
            event_index = int(event.get('event_index', 0) or 0)
            if event_index <= self._live_state.last_processed_event_index:
                continue
            max_seen = max(max_seen, event_index)
            event_type = str(event.get('event_type', ''))
            snapshot = event.get('snapshot') if isinstance(event.get('snapshot'), dict) else None
            if event_type == 'training_card_consumed':
                item_id = str(event.get('review_item_id') or '')
                label = self._label_for_item_id(item_id, snapshot)
                bg, fg = self._color_for_item_id(item_id)
                self._append_history_row('Training', label, '—', bg, fg, review_item_id=item_id)
            elif event_type == 'corpus_move_emitted':
                self._append_history_row('Corpus', '—', '—', '#000000', 'white')
            elif event_type == 'training_outcome_recorded':
                self._apply_training_outcome(
                    review_item_id=str(event.get('review_item_id') or ''),
                    result=str(event.get('result') or ''),
                )
        self._live_state.last_processed_event_index = max_seen
        self._live_state.runtime_events = [
            event for event in events if int(event.get('event_index', 0) or 0) > self._live_state.last_processed_event_index
        ]

    def _append_history_row(
        self,
        source: str,
        position: str,
        result: str,
        bg: str,
        fg: str,
        *,
        review_item_id: str | None = None,
    ) -> None:
        if self._live_state.placeholder_visible:
            if self.history_table.exists(self._history_placeholder_iid):
                self.history_table.delete(self._history_placeholder_iid)
            self._live_state.placeholder_visible = False
        row_serial = self._live_state.history_row_serial
        self._live_state.history_row_serial += 1
        iid = f'history_{row_serial}'
        self._live_state.history_rows.append(
            {'iid': iid, 'source': source, 'position': position, 'result': result, 'bg': bg, 'fg': fg}
        )
        if len(self._live_state.history_rows) > self.HISTORY_LIMIT:
            self._live_state.history_rows = self._live_state.history_rows[-self.HISTORY_LIMIT :]
            self._rerender_history()
        else:
            self.history_table.insert('', 'end', iid=iid, values=(source, position, result), tags=(iid,))
            self.history_table.tag_configure(iid, background=bg, foreground=fg)
            self.history_table.see(iid)
        if review_item_id:
            self._live_state.pending_training_history_by_item_id.setdefault(review_item_id, []).append(iid)

    def _rerender_history(self) -> None:
        for row_id in self.history_table.get_children(''):
            self.history_table.delete(row_id)
        for row in self._live_state.history_rows:
            iid = row['iid']
            self.history_table.insert('', 'end', iid=iid, values=(row['source'], row['position'], row['result']), tags=(iid,))
            self.history_table.tag_configure(iid, background=row['bg'], foreground=row['fg'])
        visible = {row['iid'] for row in self._live_state.history_rows}
        self._live_state.pending_training_history_by_item_id = {
            item_id: [iid for iid in iids if iid in visible]
            for item_id, iids in self._live_state.pending_training_history_by_item_id.items()
            if any(iid in visible for iid in iids)
        }
        if self._live_state.history_rows:
            self.history_table.see(self._live_state.history_rows[-1]['iid'])

    def _show_empty_history_placeholder(self) -> None:
        self.history_table.insert(
            '',
            'end',
            iid=self._history_placeholder_iid,
            values=('—', 'No history yet. Waiting for corpus/training events.', '—'),
        )
        self._live_state.placeholder_visible = True

    def _apply_training_outcome(self, review_item_id: str, result: str) -> None:
        rows = self._live_state.pending_training_history_by_item_id.get(review_item_id, [])
        if not rows:
            return
        iid = rows.pop(0)
        rendered_result = 'PASS' if result.lower() == 'pass' else 'FAIL'
        if self.history_table.exists(iid):
            values = self.history_table.item(iid, 'values')
            self.history_table.item(iid, values=(values[0], values[1], rendered_result))
        for row in self._live_state.history_rows:
            if row['iid'] == iid:
                row['result'] = rendered_result
                break

    @staticmethod
    def _format_pct(value: object) -> str:
        if isinstance(value, (int, float)):
            return f'{value * 100:.1f}%'
        return '—'

    def _training_share_tooltip_text(self) -> str:
        data = self._latest_share_breakdown
        if not data:
            return ''
        return (
            'Active: '
            f"D={data.get('due_active', 0)} B={data.get('boosted_active', 0)} E={data.get('urgent_active', 0)}\n"
            'Equivalent: '
            f"D={data.get('due_equivalent', 0)} B={data.get('boosted_equivalent', 0)} E={data.get('urgent_equivalent', 0)}\n"
            'Contribution: '
            f"D={data.get('due_pct', 0)}% B={data.get('boosted_pct', 0)}% E={data.get('urgent_pct', 0)}%\n"
            f"Total training={data.get('training_pct', 0)}% | corpus remainder={data.get('corpus_pct', 0)}%"
        )
