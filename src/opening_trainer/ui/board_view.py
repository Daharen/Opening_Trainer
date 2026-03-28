from __future__ import annotations

import math
import tkinter as tk
from dataclasses import dataclass
from time import monotonic

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
BOARD_PADDING = 28
DRAG_THRESHOLD = 10


@dataclass(frozen=True)
class DragState:
    source_square: chess.Square
    current_square: chess.Square | None
    piece_symbol: str
    cursor_x: int
    cursor_y: int
    moved: bool = False


@dataclass(frozen=True)
class SettleAnimationState:
    piece_symbol: str
    start_x: float
    start_y: float
    end_x: float
    end_y: float
    destination_square: chess.Square
    start_time: float
    duration_seconds: float


class BoardView(tk.Canvas):
    def __init__(self, master: tk.Misc, board_size: int = 480, min_board_size: int = 360):
        super().__init__(master, width=board_size, height=board_size, highlightthickness=0)
        self.board_size = board_size
        self.min_board_size = min_board_size
        self.square_size = max(1, (board_size - BOARD_PADDING * 2) // 8)
        self.selected_square: chess.Square | None = None
        self.highlight_squares: set[chess.Square] = set()
        self.arrow_move_uci: str | None = None
        self.arrow_color: str = '#2e7d32'
        self.drag_state: DragState | None = None
        self.settle_animation: SettleAnimationState | None = None
        self.bind('<Configure>', self._on_resize)
        self.configure(width=board_size, height=board_size)

    @property
    def board_pixels(self) -> int:
        return self.square_size * 8

    @property
    def board_origin(self) -> tuple[int, int]:
        offset_x = max(BOARD_PADDING, (self.board_size - self.board_pixels) // 2)
        offset_y = max(BOARD_PADDING, (self.board_size - self.board_pixels) // 2)
        return offset_x, offset_y

    def set_selection(self, selected_square: chess.Square | None, legal_targets: list[chess.Square]) -> None:
        self.selected_square = selected_square
        self.highlight_squares = set(legal_targets)

    def set_arrow(self, move_uci: str | None, color: str = '#2e7d32') -> None:
        self.arrow_move_uci = move_uci
        self.arrow_color = color

    def start_drag(self, source_square: chess.Square, piece_symbol: str, cursor_x: int, cursor_y: int) -> None:
        self.settle_animation = None
        self.drag_state = DragState(source_square, source_square, piece_symbol, cursor_x, cursor_y, moved=False)

    def update_drag(self, x: int, y: int, player_color: chess.Color) -> None:
        if self.drag_state is None:
            return
        origin_x, origin_y = self._square_center(self.drag_state.source_square, player_color)
        moved = self.drag_state.moved or math.hypot(x - origin_x, y - origin_y) >= DRAG_THRESHOLD
        self.drag_state = DragState(
            source_square=self.drag_state.source_square,
            current_square=self.square_at_xy(x, y, player_color),
            piece_symbol=self.drag_state.piece_symbol,
            cursor_x=x,
            cursor_y=y,
            moved=moved,
        )

    def cancel_drag(self) -> None:
        self.drag_state = None

    def clear_transient_state(self) -> None:
        self.drag_state = None
        self.settle_animation = None

    def release_drag(self, x: int, y: int, player_color: chess.Color) -> tuple[chess.Square, chess.Square | None, bool] | None:
        if self.drag_state is None:
            return None
        source_square = self.drag_state.source_square
        was_drag = self.drag_state.moved
        destination = self.square_at_xy(x, y, player_color)
        self.drag_state = None
        return source_square, destination, was_drag

    def start_settle_animation(
        self,
        *,
        piece_symbol: str,
        release_x: int,
        release_y: int,
        destination_square: chess.Square,
        player_color: chess.Color,
        duration_ms: int = 65,
    ) -> None:
        end_x, end_y = self._square_center(destination_square, player_color)
        self.settle_animation = SettleAnimationState(
            piece_symbol=piece_symbol,
            start_x=float(release_x),
            start_y=float(release_y),
            end_x=end_x,
            end_y=end_y,
            destination_square=destination_square,
            start_time=monotonic(),
            duration_seconds=max(0.01, duration_ms / 1000),
        )

    def animation_in_progress(self) -> bool:
        return self._settle_piece_position() is not None

    def square_at_xy(self, x: int, y: int, player_color: chess.Color) -> chess.Square | None:
        origin_x, origin_y = self.board_origin
        if x < origin_x or y < origin_y or x >= origin_x + self.board_pixels or y >= origin_y + self.board_pixels:
            return None
        row = (y - origin_y) // self.square_size
        col = (x - origin_x) // self.square_size
        return display_to_square(row, col, player_color)

    def coordinate_labels(self, player_color: chess.Color) -> tuple[list[str], list[str]]:
        if player_color == chess.WHITE:
            return list('abcdefgh'), [str(rank) for rank in range(8, 0, -1)]
        return list('hgfedcba'), [str(rank) for rank in range(1, 9)]

    def render(self, board: chess.Board, player_color: chess.Color) -> None:
        self.delete('all')
        origin_x, origin_y = self.board_origin
        for square in chess.SQUARES:
            row, col = square_to_display(square, player_color)
            x0 = origin_x + col * self.square_size
            y0 = origin_y + row * self.square_size
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
            if piece is None:
                continue
            if self.drag_state is not None and self.drag_state.moved and square == self.drag_state.source_square:
                continue
            if self.settle_animation is not None and square == self.settle_animation.destination_square:
                continue
            text_fill = '#111111'
            self.create_text(
                x0 + self.square_size / 2,
                y0 + self.square_size / 2,
                text=PIECE_GLYPHS[piece.symbol()],
                font=('Arial Unicode MS', int(self.square_size * 0.58)),
                fill=text_fill,
            )
        self._draw_coordinates(player_color)
        self._draw_arrow(player_color)
        self._draw_drag_piece()
        self._draw_settle_piece()

    def _draw_coordinates(self, player_color: chess.Color) -> None:
        origin_x, origin_y = self.board_origin
        files, ranks = self.coordinate_labels(player_color)
        font = ('TkDefaultFont', max(8, int(self.square_size * 0.16)), 'bold')
        for idx, label in enumerate(files):
            x = origin_x + idx * self.square_size + self.square_size / 2
            self.create_text(x, origin_y + self.board_pixels + BOARD_PADDING / 2, text=label, font=font, fill='#333333')
        for idx, label in enumerate(ranks):
            y = origin_y + idx * self.square_size + self.square_size / 2
            self.create_text(BOARD_PADDING / 2, y, text=label, font=font, fill='#333333')

    def _draw_drag_piece(self) -> None:
        if not self.drag_state or not self.drag_state.moved:
            return
        self.create_text(
            self.drag_state.cursor_x,
            self.drag_state.cursor_y,
            text=PIECE_GLYPHS[self.drag_state.piece_symbol],
            font=('Arial Unicode MS', int(self.square_size * 0.62)),
            fill='#111111',
        )

    def _draw_settle_piece(self) -> None:
        settle_position = self._settle_piece_position()
        if settle_position is None:
            return
        settle_x, settle_y = settle_position
        assert self.settle_animation is not None
        self.create_text(
            settle_x,
            settle_y,
            text=PIECE_GLYPHS[self.settle_animation.piece_symbol],
            font=('Arial Unicode MS', int(self.square_size * 0.58)),
            fill='#111111',
        )

    def _on_resize(self, event: tk.Event) -> None:
        new_size = max(self.min_board_size, min(event.width, event.height))
        if new_size == self.board_size:
            return
        self.board_size = new_size
        self.square_size = max(1, (new_size - BOARD_PADDING * 2) // 8)
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
        self.create_line(
            start_x + ux * inset,
            start_y + uy * inset,
            end_x - ux * inset,
            end_y - uy * inset,
            fill=self.arrow_color,
            width=max(4, self.square_size * 0.12),
            arrow=tk.LAST,
            arrowshape=(max(12, self.square_size * 0.36), max(14, self.square_size * 0.42), max(6, self.square_size * 0.16)),
            capstyle=tk.ROUND,
        )

    def _square_center(self, square: chess.Square, player_color: chess.Color) -> tuple[float, float]:
        row, col = square_to_display(square, player_color)
        origin_x, origin_y = self.board_origin
        return (
            origin_x + col * self.square_size + self.square_size / 2,
            origin_y + row * self.square_size + self.square_size / 2,
        )

    def _settle_piece_position(self) -> tuple[float, float] | None:
        if self.settle_animation is None:
            return None
        elapsed = monotonic() - self.settle_animation.start_time
        if elapsed >= self.settle_animation.duration_seconds:
            self.settle_animation = None
            return None
        progress = max(0.0, min(1.0, elapsed / self.settle_animation.duration_seconds))
        x = self.settle_animation.start_x + (self.settle_animation.end_x - self.settle_animation.start_x) * progress
        y = self.settle_animation.start_y + (self.settle_animation.end_y - self.settle_animation.start_y) * progress
        return x, y
