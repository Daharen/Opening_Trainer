from __future__ import annotations

import json
import tkinter as tk
from pathlib import Path
from tkinter import simpledialog

import chess

from ..models import SessionState
from ..runtime import RuntimeContext
from ..session import TrainingSession
from ..session_contracts import OutcomeBoardContract, OutcomeModalContract
from .board_view import BoardView
from .outcome_modal import OutcomeModal
from .profile_dialog import ProfileDialog
from .review_inspector import ReviewInspector
from .status_panel import StatusPanel

PROMOTION_CHOICES = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}
GUI_STATE_FILENAME = 'gui_state.json'


class OpeningTrainerGUI:
    def __init__(self, session: TrainingSession | None = None, runtime_context: RuntimeContext | None = None):
        self.session = session or TrainingSession(runtime_context=runtime_context, mode='gui')
        self.root = tk.Tk()
        self.root.title('Opening Trainer')
        self.selected_square = None
        self.pending_restart = False
        self.panel_visible = self._load_panel_visibility_preference()
        self.side_panel_padx = (0, 12)
        self.side_panel_pady = (6, 12)

        self.root.columnconfigure(0, weight=8, minsize=420)
        self.root.columnconfigure(1, weight=0)
        self.root.rowconfigure(1, weight=1)

        toolbar = tk.Frame(self.root)
        toolbar.grid(row=0, column=0, columnspan=2, sticky='ew', padx=12, pady=(12, 4))
        tk.Button(toolbar, text='Start drill', command=self._start_game).pack(side='left')
        tk.Button(toolbar, text='Profiles', command=self._open_profiles).pack(side='left', padx=6)
        self.panel_toggle_button = tk.Button(toolbar, text='', command=self._toggle_side_panel)
        self.panel_toggle_button.pack(side='left', padx=(6, 0))

        self.main_region = tk.Frame(self.root)
        self.main_region.grid(row=1, column=0, sticky='nsew', padx=(12, 6), pady=(6, 12))
        self.main_region.columnconfigure(0, weight=1, minsize=420)
        self.main_region.rowconfigure(1, weight=1)

        self.compact_status_panel = StatusPanel(self.main_region, compact=True)
        self.compact_status_panel.grid(row=0, column=0, sticky='ew', pady=(0, 8))

        self.board_view = BoardView(self.main_region, board_size=560, min_board_size=360)
        self.board_view.grid(row=1, column=0, sticky='nsew')

        self.side_panel = tk.Frame(self.root, width=360)
        self.side_panel.grid(row=1, column=1, sticky='nsew', padx=self.side_panel_padx, pady=self.side_panel_pady)
        self.side_panel.rowconfigure(2, weight=1)
        self.side_panel.columnconfigure(0, weight=1)
        self.side_panel.grid_propagate(False)

        self.status_panel = StatusPanel(self.side_panel)
        self.status_panel.grid(row=0, column=0, sticky='ew')

        self.recent_var = tk.StringVar()
        tk.Label(self.side_panel, textvariable=self.recent_var, justify='left', anchor='w').grid(row=1, column=0, sticky='ew', pady=4)

        self.inspector = ReviewInspector(self.side_panel, self.session, self._refresh_supporting_surfaces)
        self.inspector.grid(row=2, column=0, sticky='nsew')

        self.board_view.bind('<Button-1>', self._on_board_click)
        self._apply_side_panel_layout(initializing=True)

    def run(self) -> None:
        self._start_game()
        self.root.mainloop()

    def _gui_state_path(self) -> Path:
        return self.session.review_storage.root / GUI_STATE_FILENAME

    def _load_panel_visibility_preference(self) -> bool:
        path = self._gui_state_path()
        if not path.exists():
            return True
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return True
        return bool(payload.get('side_panel_visible', True))

    def _save_panel_visibility_preference(self) -> None:
        path = self._gui_state_path()
        path.write_text(json.dumps({'side_panel_visible': self.panel_visible}, indent=2), encoding='utf-8')

    def _toggle_side_panel(self) -> None:
        self.panel_visible = not self.panel_visible
        self._apply_side_panel_layout()
        self._save_panel_visibility_preference()

    def _apply_side_panel_layout(self, initializing: bool = False) -> None:
        if self.panel_visible:
            if hasattr(self, 'side_panel'):
                self.side_panel.grid(row=1, column=1, sticky='nsew', padx=self.side_panel_padx, pady=self.side_panel_pady)
            if hasattr(self.root, 'grid_columnconfigure'):
                self.root.grid_columnconfigure(0, weight=8, minsize=420)
                self.root.grid_columnconfigure(1, weight=1, minsize=240, pad=0)
            if hasattr(self.side_panel, 'configure'):
                self.side_panel.configure(width=320)
            if hasattr(self, 'compact_status_panel'):
                self.compact_status_panel.grid_remove()
            self._set_panel_toggle_label('Hide Panel')
        else:
            if hasattr(self, 'side_panel'):
                self.side_panel.grid_remove()
            if hasattr(self.root, 'grid_columnconfigure'):
                self.root.grid_columnconfigure(0, weight=8, minsize=420)
                self.root.grid_columnconfigure(1, weight=0, minsize=0, pad=0)
            if hasattr(self, 'compact_status_panel'):
                self.compact_status_panel.grid()
            self._set_panel_toggle_label('Show Panel')
        if not initializing:
            self._refresh_supporting_surfaces()

    def _set_panel_toggle_label(self, label: str) -> None:
        if hasattr(self.panel_toggle_button, 'configure'):
            self.panel_toggle_button.configure(text=label)

    def _open_profiles(self):
        ProfileDialog(self.root, self.session, self._refresh_supporting_surfaces).open()

    def _start_game(self):
        self.session.start_new_game()
        self.selected_square = None
        self.pending_restart = False
        self._refresh_view()

    def _build_counts_summary(self, due: int, boosted: int, extreme: int) -> str:
        return f'Due: {due} | Boosted: {boosted} | Extreme: {extreme}'

    def _build_routing_summary(self, routing: str, explain: str) -> str:
        return f'Routing: {routing} | {explain}'

    def _build_compact_bundle_summary(self) -> str:
        return f'Opponent: {self.session.opponent.status_message}'

    def _refresh_supporting_surfaces(self):
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        profile_name = self.session.review_storage.load_profile_meta(self.session.active_profile_id).display_name
        due = sum(1 for item in items if item.due_at_utc <= item.updated_at_utc)
        boosted = sum(1 for item in items if item.urgency_tier == 'boosted_review')
        extreme = sum(1 for item in items if item.urgency_tier == 'extreme_urgency')
        routing = self.session.current_routing.routing_source if self.session.current_routing else 'not_started'
        explain = self.session.current_routing.selection_explanation if self.session.current_routing else 'No routing decision yet.'
        counts_summary = self._build_counts_summary(due, boosted, extreme)
        routing_summary = self._build_routing_summary(routing, explain)
        bundle_summary = f'Opponent source: {self.session.opponent.status_message}'
        self.status_panel.update_status(profile_name=profile_name, bundle_summary=bundle_summary, routing_summary=routing_summary, counts_summary=counts_summary)
        self.compact_status_panel.update_status(profile_name=profile_name, bundle_summary=self._build_compact_bundle_summary(), routing_summary=f'Route: {routing}', counts_summary=f'{due}/{boosted}/{extreme} due/boosted/extreme')
        history_path = self.session.review_storage.root / self.session.active_profile_id / 'session_history.jsonl'
        recent = history_path.read_text(encoding='utf-8').strip().splitlines()[-4:] if history_path.exists() else []
        self.recent_var.set('Recent events:\n' + ('\n'.join(recent) if recent else 'No recent events.'))
        self.inspector.refresh()

    def _refresh_view(self, transient_status: str | None = None) -> None:
        view = self.session.get_view()
        board = chess.Board(view.board_fen)
        legal_targets = []
        if self.selected_square is not None and view.awaiting_user_input:
            legal_targets = [move.to_square for move in self.session.legal_moves_from(self.selected_square)]
        self.board_view.set_selection(self.selected_square, legal_targets)
        self.board_view.render(board, view.player_color)
        self._refresh_supporting_surfaces()
        if transient_status:
            self.recent_var.set(transient_status + '\n\n' + self.recent_var.get())
        if view.state == SessionState.RESTART_PENDING and view.last_outcome is not None and not self.pending_restart:
            self.pending_restart = True
            self._show_outcome_modal(view)

    def _show_outcome_modal(self, view):
        outcome = view.last_outcome
        if outcome is None:
            return None
        review_boards: list[OutcomeBoardContract] = []
        if outcome.terminal_kind == 'fail' and outcome.pre_fail_fen and outcome.preferred_move_uci:
            review_boards.append(OutcomeBoardContract(
                title='What you should have played',
                board_fen=outcome.pre_fail_fen,
                arrow_move_uci=outcome.preferred_move_uci,
                arrow_color='#2e7d32',
                arrow_label='Correct move',
                move_label=outcome.preferred_move_san or outcome.preferred_move_uci,
            ))
        if outcome.terminal_kind == 'fail' and outcome.post_fail_fen and outcome.punishing_reply_uci:
            review_boards.append(OutcomeBoardContract(
                title='What punishes this',
                board_fen=outcome.post_fail_fen,
                arrow_move_uci=outcome.punishing_reply_uci,
                arrow_color='#c62828',
                arrow_label='Likely punishment',
                move_label=outcome.punishing_reply_san or outcome.punishing_reply_uci,
            ))
        contract = OutcomeModalContract(
            headline='SUCCESS' if outcome.passed else 'FAIL',
            summary=outcome.reason,
            reason=outcome.reason,
            preferred_move=outcome.preferred_move,
            routing_reason=outcome.routing_reason,
            next_routing_reason=outcome.next_routing_reason,
            impact_summary=f'Profile: {outcome.profile_name} | {outcome.impact_summary}',
            review_boards=tuple(review_boards),
        )
        return OutcomeModal(self.root, contract, self._acknowledge_outcome)

    def _acknowledge_outcome(self):
        self.pending_restart = False
        self.selected_square = None
        self.session.start_new_game()
        self._refresh_view()

    def _on_board_click(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if not view.awaiting_user_input:
            self._refresh_view('Wait for your turn.')
            return
        square = self.board_view.square_at_xy(event.x, event.y, view.player_color)
        if square is None:
            return
        board = self.session.current_board()
        piece = board.piece_at(square)
        if self.selected_square is None:
            if piece is None or piece.color != view.player_color:
                self._refresh_view('Select one of your own pieces.')
                return
            legal_moves = self.session.legal_moves_from(square)
            if not legal_moves:
                self._refresh_view('That piece has no legal moves.')
                return
            self.selected_square = square
            self._refresh_view()
            return
        if square == self.selected_square:
            self.selected_square = None
            self._refresh_view('Selection cleared.')
            return
        move = self._build_move(self.selected_square, square, board)
        self.selected_square = None
        if move is None:
            self._refresh_view('Illegal move selection.')
            return
        self.session.submit_user_move_uci(move.uci())
        self._refresh_view()

    def _build_move(self, from_square: chess.Square, to_square: chess.Square, board: chess.Board) -> chess.Move | None:
        candidate = chess.Move(from_square, to_square)
        if candidate in board.legal_moves:
            return candidate
        for promotion_piece in PROMOTION_CHOICES.values():
            promoted = chess.Move(from_square, to_square, promotion=promotion_piece)
            if promoted in board.legal_moves:
                choice = simpledialog.askstring('Promotion', 'Promote to (q, r, b, n). Default is q:', parent=self.root)
                code = (choice or 'q').strip().lower()
                return chess.Move(from_square, to_square, promotion=PROMOTION_CHOICES.get(code, chess.QUEEN))
        return None


def launch_gui(runtime_context: RuntimeContext | None = None) -> None:
    OpeningTrainerGUI(runtime_context=runtime_context).run()
