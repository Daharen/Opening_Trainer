from __future__ import annotations

import math
import tkinter as tk
from dataclasses import replace
from dataclasses import dataclass
from time import monotonic

import chess

from ..session_logging import log_line
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
ANIMATION_START_LEAD_SECONDS = 1 / 120
DEFAULT_COMMITTED_MOVE_DURATION_MS = 140
DEFAULT_IMMEDIATE_FRAME_MIN_PROGRESS = 0.08
DEFAULT_IMMEDIATE_FRAME_MAX_PROGRESS = 0.92


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
        self.premove_move_uci_queue: tuple[str, ...] = ()
        self.premove_highlight_squares: set[chess.Square] = set()
        self.drag_state: DragState | None = None
        self.settle_animation: SettleAnimationState | None = None
        self._last_board_fen: str | None = None
        self._last_player_color: chess.Color | None = None
        self._resize_refresh_after_handle: str | None = None
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

    def set_premove_queue(self, move_uci_queue: list[str]) -> None:
        self.premove_move_uci_queue = tuple(move_uci_queue)
        squares: set[chess.Square] = set()
        for move_uci in self.premove_move_uci_queue:
            try:
                move = chess.Move.from_uci(move_uci)
            except ValueError:
                continue
            squares.add(move.from_square)
            squares.add(move.to_square)
        self.premove_highlight_squares = squares

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
            start_time=monotonic() - ANIMATION_START_LEAD_SECONDS,
            duration_seconds=max(0.01, duration_ms / 1000),
        )
        log_line(
            'GUI_ANIM_HELPER_START_SETTLE: '
            f'piece={piece_symbol}; to_sq={chess.square_name(destination_square)}; duration_ms={duration_ms}',
            tag='timing',
        )

    def start_committed_move_animation(
        self,
        *,
        piece_symbol: str,
        source_square: chess.Square,
        destination_square: chess.Square,
        player_color: chess.Color,
        start_x: float | None = None,
        start_y: float | None = None,
        duration_ms: int = DEFAULT_COMMITTED_MOVE_DURATION_MS,
    ) -> None:
        if start_x is None or start_y is None:
            start_x, start_y = self._square_center(source_square, player_color)
        end_x, end_y = self._square_center(destination_square, player_color)
        self.settle_animation = SettleAnimationState(
            piece_symbol=piece_symbol,
            start_x=float(start_x),
            start_y=float(start_y),
            end_x=end_x,
            end_y=end_y,
            destination_square=destination_square,
            start_time=monotonic() - ANIMATION_START_LEAD_SECONDS,
            duration_seconds=max(0.01, duration_ms / 1000),
        )
        log_line(
            'GUI_ANIM_HELPER_START_COMMITTED: '
            f'piece={piece_symbol}; from_sq={chess.square_name(source_square)}; '
            f'to_sq={chess.square_name(destination_square)}; duration_ms={duration_ms}',
            tag='timing',
        )

    def force_immediate_visible_frame(
        self,
        *,
        min_progress: float = DEFAULT_IMMEDIATE_FRAME_MIN_PROGRESS,
        max_progress: float = DEFAULT_IMMEDIATE_FRAME_MAX_PROGRESS,
    ) -> float | None:
        animation = self.settle_animation
        if animation is None:
            return None
        clamped_max = max(0.01, min(1.0, max_progress))
        clamped_min = max(0.0, min(clamped_max, min_progress))
        now = monotonic()
        current_elapsed = max(0.0, now - animation.start_time)
        target_elapsed = max(current_elapsed, animation.duration_seconds * clamped_min)
        capped_elapsed = min(target_elapsed, animation.duration_seconds * clamped_max)
        if capped_elapsed > current_elapsed:
            self.settle_animation = replace(animation, start_time=now - capped_elapsed)
            animation = self.settle_animation
        progress = max(0.0, min(1.0, capped_elapsed / animation.duration_seconds))
        return progress

    def animation_in_progress(self) -> bool:
        return self.settle_animation is not None and not self.animation_complete()

    def animation_complete(self, now: float | None = None) -> bool:
        animation = self.settle_animation
        if animation is None:
            return False
        current_time = monotonic() if now is None else now
        return (current_time - animation.start_time) >= animation.duration_seconds

    def sample_animation_position(self, now: float | None = None) -> tuple[float, float] | None:
        animation = self.settle_animation
        if animation is None:
            return None
        current_time = monotonic() if now is None else now
        elapsed = max(0.0, current_time - animation.start_time)
        progress = max(0.0, min(1.0, elapsed / animation.duration_seconds))
        x = animation.start_x + (animation.end_x - animation.start_x) * progress
        y = animation.start_y + (animation.end_y - animation.start_y) * progress
        return x, y

    def finalize_animation(self) -> bool:
        if self.settle_animation is None:
            return False
        self.settle_animation = None
        return True

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
        self._last_board_fen = board.fen()
        self._last_player_color = player_color
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
            if square in getattr(self, 'premove_highlight_squares', set()):
                fill = '#efd6d6' if is_light else '#c69494'
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
        self._draw_premove_arrows(player_color)
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
        settle_position = self.sample_animation_position()
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
        if self._resize_refresh_after_handle is not None:
            try:
                self.after_cancel(self._resize_refresh_after_handle)
            except Exception:
                pass
        self._resize_refresh_after_handle = self.after_idle(self._refresh_after_resize)

    def _refresh_after_resize(self) -> None:
        self._resize_refresh_after_handle = None
        if self._last_board_fen is None or self._last_player_color is None:
            return
        self.render(chess.Board(self._last_board_fen), self._last_player_color)

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

    def _draw_premove_arrows(self, player_color: chess.Color) -> None:
        premove_queue = getattr(self, 'premove_move_uci_queue', ())
        if not premove_queue:
            return
        for move_uci in premove_queue:
            try:
                move = chess.Move.from_uci(move_uci)
            except ValueError:
                continue
            start_x, start_y = self._square_center(move.from_square, player_color)
            end_x, end_y = self._square_center(move.to_square, player_color)
            dx = end_x - start_x
            dy = end_y - start_y
            distance = math.hypot(dx, dy)
            if distance <= 0:
                continue
            inset = self.square_size * 0.24
            ux = dx / distance
            uy = dy / distance
            self.create_line(
                start_x + ux * inset,
                start_y + uy * inset,
                end_x - ux * inset,
                end_y - uy * inset,
                fill='#d66a6a',
                width=max(2, self.square_size * 0.07),
                arrow=tk.LAST,
                arrowshape=(max(8, self.square_size * 0.24), max(10, self.square_size * 0.28), max(4, self.square_size * 0.12)),
                capstyle=tk.ROUND,
            )

    def _square_center(self, square: chess.Square, player_color: chess.Color) -> tuple[float, float]:
        row, col = square_to_display(square, player_color)
        origin_x, origin_y = self.board_origin
        return (
            origin_x + col * self.square_size + self.square_size / 2,
            origin_y + row * self.square_size + self.square_size / 2,
        )
