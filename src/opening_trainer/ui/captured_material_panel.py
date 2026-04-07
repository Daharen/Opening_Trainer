from __future__ import annotations

import tkinter as tk
from tkinter import ttk

import chess

PIECE_VALUES = {
    chess.PAWN: 1,
    chess.KNIGHT: 3,
    chess.BISHOP: 3,
    chess.ROOK: 5,
    chess.QUEEN: 9,
}
PIECE_ORDER = (chess.QUEEN, chess.ROOK, chess.BISHOP, chess.KNIGHT, chess.PAWN)
PIECE_GLYPHS = {
    (chess.WHITE, chess.PAWN): '♙',
    (chess.WHITE, chess.KNIGHT): '♘',
    (chess.WHITE, chess.BISHOP): '♗',
    (chess.WHITE, chess.ROOK): '♖',
    (chess.WHITE, chess.QUEEN): '♕',
    (chess.BLACK, chess.PAWN): '♟',
    (chess.BLACK, chess.KNIGHT): '♞',
    (chess.BLACK, chess.BISHOP): '♝',
    (chess.BLACK, chess.ROOK): '♜',
    (chess.BLACK, chess.QUEEN): '♛',
}


def captured_pieces_and_material(board: chess.Board) -> tuple[list[str], list[str], int]:
    white_captured: list[str] = []
    black_captured: list[str] = []
    white_material = 0
    black_material = 0
    for piece_type in PIECE_ORDER:
        starting = 8 if piece_type == chess.PAWN else 2 if piece_type in (chess.KNIGHT, chess.BISHOP, chess.ROOK) else 1
        missing_white = max(0, starting - len(board.pieces(piece_type, chess.WHITE)))
        missing_black = max(0, starting - len(board.pieces(piece_type, chess.BLACK)))
        white_captured.extend([PIECE_GLYPHS[(chess.BLACK, piece_type)]] * missing_black)
        black_captured.extend([PIECE_GLYPHS[(chess.WHITE, piece_type)]] * missing_white)
        white_material += PIECE_VALUES.get(piece_type, 0) * missing_black
        black_material += PIECE_VALUES.get(piece_type, 0) * missing_white
    return white_captured, black_captured, white_material - black_material


def _format_clock_seconds(seconds: float | None) -> str:
    if seconds is None:
        return '--:--'
    total = max(0, int(round(seconds)))
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f'{hours:d}:{minutes:02d}:{secs:02d}'
    return f'{minutes:02d}:{secs:02d}'


class CapturedMaterialPanel(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.pieces_var = tk.StringVar(value='—')
        self.clock_var = tk.StringVar(value='--:--')
        self.delta_var = tk.StringVar(value='')
        self.pieces_label = tk.Label(self, textvariable=self.pieces_var, anchor='w')
        self.delta_label = tk.Label(self, textvariable=self.delta_var, anchor='e', width=4)
        self.clock_label = tk.Label(self, textvariable=self.clock_var, anchor='e', width=8)
        self.pieces_label.pack(side='left', fill='x', expand=True)
        self.delta_label.pack(side='right', padx=(6, 0))
        self.clock_label.pack(side='right')

    def apply_theme(self, palette: dict[str, str], *, dark: bool) -> None:
        self.configure(style='CapturedMaterial.TFrame')
        for label in (self.pieces_label, self.delta_label, self.clock_label):
            label.configure(bg=palette['surface_bg'], fg=palette['text_fg'])

    def update_board(self, board: chess.Board, *, player_color: chess.Color, near_side: bool, clock_seconds: float | None = None) -> None:
        white_captured, black_captured, delta = captured_pieces_and_material(board)
        if near_side:
            owner_is_white = player_color == chess.WHITE
        else:
            owner_is_white = player_color == chess.BLACK
        owner_captured = white_captured if owner_is_white else black_captured
        self.pieces_var.set(' '.join(owner_captured) if owner_captured else '—')
        self.clock_var.set(_format_clock_seconds(clock_seconds))
        owner_advantage = delta if owner_is_white else -delta
        self.delta_var.set(f'+{owner_advantage}' if owner_advantage > 0 else '')
