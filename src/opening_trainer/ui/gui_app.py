from __future__ import annotations

import tkinter as tk
from tkinter import simpledialog

import chess

from ..models import SessionState
from ..runtime import RuntimeContext
from ..session import TrainingSession
from .board_view import BoardView

PROMOTION_CHOICES = {
    "q": chess.QUEEN,
    "r": chess.ROOK,
    "b": chess.BISHOP,
    "n": chess.KNIGHT,
}


class OpeningTrainerGUI:
    def __init__(self, session: TrainingSession | None = None, runtime_context: RuntimeContext | None = None):
        self.session = session or TrainingSession(runtime_context=runtime_context, mode="gui")
        self.root = tk.Tk()
        self.root.title("Opening Trainer")
        self.board_view = BoardView(self.root)
        self.board_view.pack(padx=12, pady=(12, 6))
        self.status_var = tk.StringVar()
        self.info_var = tk.StringVar()
        tk.Label(self.root, textvariable=self.status_var, anchor="w").pack(fill="x", padx=12)
        tk.Label(self.root, textvariable=self.info_var, anchor="w").pack(fill="x", padx=12, pady=(0, 12))
        self.board_view.bind("<Button-1>", self._on_board_click)
        self.selected_square: chess.Square | None = None
        self.pending_restart = False

    def run(self) -> None:
        self.session.start_new_game()
        self._refresh_view()
        self.root.mainloop()

    def _refresh_view(self, transient_status: str | None = None) -> None:
        view = self.session.get_view()
        board = chess.Board(view.board_fen)
        legal_targets = []
        if self.selected_square is not None and view.awaiting_user_input:
            legal_targets = [move.to_square for move in self.session.legal_moves_from(self.selected_square)]
        self.board_view.set_selection(self.selected_square, legal_targets)
        self.board_view.render(board, view.player_color)

        color_name = "White" if view.player_color == chess.WHITE else "Black"
        last_label = view.last_evaluation.overlay_label.value if view.last_evaluation else "—"
        self.info_var.set(
            f"Color: {color_name} | Envelope: {view.player_move_count}/{view.required_player_moves} | Last: {last_label}"
        )

        if transient_status:
            self.status_var.set(transient_status)
        elif view.last_outcome is not None:
            prefix = "Success" if view.last_outcome.passed else "Fail"
            hint = f" | Preferred: {view.last_outcome.preferred_move}" if view.last_outcome.preferred_move else ""
            self.status_var.set(f"{prefix}: {view.last_outcome.reason}{hint}")
        elif view.awaiting_user_input:
            self.status_var.set("Select a piece, then select its destination.")
        else:
            self.status_var.set("Processing opponent move...")

        if view.state == SessionState.RESTART_PENDING and not self.pending_restart:
            self.pending_restart = True
            self.root.after(self.session.restart_delay_ms, self._restart_game)

    def _restart_game(self) -> None:
        self.pending_restart = False
        self.selected_square = None
        self.session.start_new_game()
        self._refresh_view()

    def _on_board_click(self, event: tk.Event) -> None:
        view = self.session.get_view()
        if not view.awaiting_user_input:
            self._refresh_view("Wait for your turn.")
            return

        square = self.board_view.square_at_xy(event.x, event.y, view.player_color)
        if square is None:
            return

        board = self.session.current_board()
        piece = board.piece_at(square)

        if self.selected_square is None:
            if piece is None or piece.color != view.player_color:
                self._refresh_view("Select one of your own pieces.")
                return
            legal_moves = self.session.legal_moves_from(square)
            if not legal_moves:
                self._refresh_view("That piece has no legal moves.")
                return
            self.selected_square = square
            self._refresh_view()
            return

        if square == self.selected_square:
            self.selected_square = None
            self._refresh_view("Selection cleared.")
            return

        move = self._build_move(self.selected_square, square, board)
        self.selected_square = None
        if move is None:
            self._refresh_view("Illegal move selection.")
            return

        self.session.submit_user_move_uci(move.uci())
        self._refresh_view()

    def _build_move(self, from_square: chess.Square, to_square: chess.Square, board: chess.Board) -> chess.Move | None:
        candidate = chess.Move(from_square, to_square)
        if candidate in board.legal_moves:
            return candidate

        for promotion_code, promotion_piece in PROMOTION_CHOICES.items():
            promoted = chess.Move(from_square, to_square, promotion=promotion_piece)
            if promoted in board.legal_moves:
                choice = simpledialog.askstring(
                    "Promotion",
                    "Promote to (q, r, b, n). Default is q:",
                    parent=self.root,
                )
                selected_code = (choice or "q").strip().lower()
                if selected_code not in PROMOTION_CHOICES:
                    selected_code = "q"
                return chess.Move(from_square, to_square, promotion=PROMOTION_CHOICES[selected_code])
        return None


def launch_gui(runtime_context: RuntimeContext | None = None) -> None:
    gui = OpeningTrainerGUI(runtime_context=runtime_context)
    gui.run()
