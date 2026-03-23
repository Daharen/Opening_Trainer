from __future__ import annotations

import tkinter as tk
from tkinter import simpledialog

import chess

from ..models import SessionState
from ..runtime import RuntimeContext
from ..session import TrainingSession
from ..session_contracts import OutcomeModalContract
from .board_view import BoardView
from .outcome_modal import OutcomeModal
from .profile_dialog import ProfileDialog
from .review_inspector import ReviewInspector
from .status_panel import StatusPanel

PROMOTION_CHOICES = {'q': chess.QUEEN, 'r': chess.ROOK, 'b': chess.BISHOP, 'n': chess.KNIGHT}


class OpeningTrainerGUI:
    def __init__(self, session: TrainingSession | None = None, runtime_context: RuntimeContext | None = None):
        self.session = session or TrainingSession(runtime_context=runtime_context, mode='gui')
        self.root = tk.Tk()
        self.root.title('Opening Trainer')
        toolbar = tk.Frame(self.root)
        toolbar.pack(fill='x', padx=12, pady=(12, 4))
        tk.Button(toolbar, text='Start drill', command=self._start_game).pack(side='left')
        tk.Button(toolbar, text='Profiles', command=self._open_profiles).pack(side='left', padx=6)
        self.board_view = BoardView(self.root)
        self.board_view.pack(side='left', padx=12, pady=(6, 12))
        right = tk.Frame(self.root)
        right.pack(side='left', fill='both', expand=True, padx=(0, 12), pady=(6, 12))
        self.status_panel = StatusPanel(right)
        self.status_panel.pack(fill='x')
        self.recent_var = tk.StringVar()
        tk.Label(right, textvariable=self.recent_var, justify='left', anchor='w').pack(fill='x', pady=4)
        self.inspector = ReviewInspector(right, self.session, self._refresh_supporting_surfaces)
        self.inspector.pack(fill='both', expand=True)
        self.board_view.bind('<Button-1>', self._on_board_click)
        self.selected_square = None
        self.pending_restart = False

    def run(self) -> None:
        self._start_game()
        self.root.mainloop()

    def _start_game(self):
        self.session.start_new_game()
        self._refresh_view()

    def _open_profiles(self):
        ProfileDialog(self.root, self.session, self._refresh_supporting_surfaces).open()

    def _refresh_supporting_surfaces(self):
        items = self.session.review_storage.load_items(self.session.active_profile_id)
        profile_name = self.session.review_storage.load_profile_meta(self.session.active_profile_id).display_name
        due = sum(1 for item in items if item.due_at_utc <= item.updated_at_utc)
        boosted = sum(1 for item in items if item.urgency_tier == 'boosted_review')
        extreme = sum(1 for item in items if item.urgency_tier == 'extreme_urgency')
        routing = self.session.current_routing.routing_source if self.session.current_routing else 'not_started'
        explain = self.session.current_routing.selection_explanation if self.session.current_routing else 'No routing decision yet.'
        self.status_panel.update_status(profile_name=profile_name, bundle_summary=f'Opponent source: {self.session.opponent.status_message}', routing_summary=f'Routing: {routing} | {explain}', counts_summary=f'Due: {due} | Boosted: {boosted} | Extreme: {extreme}')
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
        contract = OutcomeModalContract('SUCCESS' if outcome.passed else 'FAIL', outcome.reason, f'Profile: {outcome.profile_name}', outcome.reason, outcome.preferred_move, outcome.routing_reason, outcome.next_routing_reason, outcome.impact_summary, True)
        OutcomeModal(self.root, contract, self._acknowledge_outcome)

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
