from __future__ import annotations

import math
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
    def __init__(self, master: tk.Misc, board_size: int = 480, min_board_size: int = 360):
        super().__init__(master, width=board_size, height=board_size, highlightthickness=0)
        self.board_size = board_size
        self.min_board_size = min_board_size
        self.square_size = board_size // 8
        self.selected_square: chess.Square | None = None
        self.highlight_squares: set[chess.Square] = set()
        self.arrow_move_uci: str | None = None
        self.arrow_color: str = '#2e7d32'
        self.bind('<Configure>', self._on_resize)
        self.configure(width=board_size, height=board_size)

    def set_selection(self, selected_square: chess.Square | None, legal_targets: list[chess.Square]) -> None:
        self.selected_square = selected_square
        self.highlight_squares = set(legal_targets)

    def set_arrow(self, move_uci: str | None, color: str = '#2e7d32') -> None:
        self.arrow_move_uci = move_uci
        self.arrow_color = color

    def square_at_xy(self, x: int, y: int, player_color: chess.Color) -> chess.Square | None:
        if x < 0 or y < 0 or x >= self.board_size or y >= self.board_size:
            return None
        row = y // self.square_size
        col = x // self.square_size
        return display_to_square(row, col, player_color)

    def render(self, board: chess.Board, player_color: chess.Color) -> None:
        self.delete('all')
        for square in chess.SQUARES:
            row, col = square_to_display(square, player_color)
            x0 = col * self.square_size
            y0 = row * self.square_size
            x1 = x0 + self.square_size
            y1 = y0 + self.square_size
            is_light = (chess.square_file(square) + chess.square_rank(square)) % 2 == 0
            fill = '#f0d9b5' if is_light else '#b58863'
            if square == self.selected_square:
                fill = '#f6f669'
            elif square in self.highlight_squares:
                fill = '#8fce72'
            self.create_rectangle(x0, y0, x1, y1, fill=fill, outline='#333333')

            piece = board.piece_at(square)
            if piece is not None:
                self.create_text(
                    x0 + self.square_size / 2,
                    y0 + self.square_size / 2,
                    text=PIECE_GLYPHS[piece.symbol()],
                    font=('Arial Unicode MS', int(self.square_size * 0.58)),
                )
        self._draw_arrow(player_color)

    def _on_resize(self, event: tk.Event) -> None:
        new_size = max(self.min_board_size, min(event.width, event.height))
        if new_size == self.board_size:
            return
        self.board_size = new_size
        self.square_size = max(1, new_size // 8)
        self.configure(width=new_size, height=new_size)

    def _draw_arrow(self, player_color: chess.Color) -> None:
        if not self.arrow_move_uci:
            return
        move = chess.Move.from_uci(self.arrow_move_uci)
        start_x, start_y = self._square_center(move.from_square, player_color)
        end_x, end_y = self._square_center(move.to_square, player_color)
        dx = end_x - start_x
        dy = end_y - start_y
        distance = math.hypot(dx, dy)
        if distance <= 0:
            return
        inset = self.square_size * 0.18
        ux = dx / distance
        uy = dy / distance
        line_start_x = start_x + ux * inset
        line_start_y = start_y + uy * inset
        line_end_x = end_x - ux * inset
        line_end_y = end_y - uy * inset
        self.create_line(
            line_start_x,
            line_start_y,
            line_end_x,
            line_end_y,
            fill=self.arrow_color,
            width=max(4, self.square_size * 0.12),
            arrow=tk.LAST,
            arrowshape=(max(12, self.square_size * 0.36), max(14, self.square_size * 0.42), max(6, self.square_size * 0.16)),
            capstyle=tk.ROUND,
        )

    def _square_center(self, square: chess.Square, player_color: chess.Color) -> tuple[float, float]:
        row, col = square_to_display(square, player_color)
        x = col * self.square_size + self.square_size / 2
        y = row * self.square_size + self.square_size / 2
        return x, y
