from __future__ import annotations

import tkinter as tk
import chess

from .square_mapping import display_to_square, square_to_display

PIECE_GLYPHS = {
    "P": "♙",
    "N": "♘",
    "B": "♗",
    "R": "♖",
    "Q": "♕",
    "K": "♔",
    "p": "♟",
    "n": "♞",
    "b": "♝",
    "r": "♜",
    "q": "♛",
    "k": "♚",
}


class BoardView(tk.Canvas):
    def __init__(self, master: tk.Misc, board_size: int = 480):
        super().__init__(master, width=board_size, height=board_size, highlightthickness=0)
        self.board_size = board_size
        self.square_size = board_size // 8
        self.selected_square: chess.Square | None = None
        self.highlight_squares: set[chess.Square] = set()

    def set_selection(self, selected_square: chess.Square | None, legal_targets: list[chess.Square]) -> None:
        self.selected_square = selected_square
        self.highlight_squares = set(legal_targets)

    def square_at_xy(self, x: int, y: int, player_color: chess.Color) -> chess.Square | None:
        if x < 0 or y < 0 or x >= self.board_size or y >= self.board_size:
            return None
        row = y // self.square_size
        col = x // self.square_size
        return display_to_square(row, col, player_color)

    def render(self, board: chess.Board, player_color: chess.Color) -> None:
        self.delete("all")
        for square in chess.SQUARES:
            row, col = square_to_display(square, player_color)
            x0 = col * self.square_size
            y0 = row * self.square_size
            x1 = x0 + self.square_size
            y1 = y0 + self.square_size
            is_light = (chess.square_file(square) + chess.square_rank(square)) % 2 == 0
            fill = "#f0d9b5" if is_light else "#b58863"
            if square == self.selected_square:
                fill = "#f6f669"
            elif square in self.highlight_squares:
                fill = "#8fce72"
            self.create_rectangle(x0, y0, x1, y1, fill=fill, outline="#333333")

            piece = board.piece_at(square)
            if piece is not None:
                self.create_text(
                    x0 + self.square_size / 2,
                    y0 + self.square_size / 2,
                    text=PIECE_GLYPHS[piece.symbol()],
                    font=("Arial Unicode MS", int(self.square_size * 0.58)),
                )
