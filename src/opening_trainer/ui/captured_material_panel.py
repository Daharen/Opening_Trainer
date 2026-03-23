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


class CapturedMaterialPanel(ttk.LabelFrame):
    def __init__(self, master, title: str = 'Captured material'):
        super().__init__(master, text=title)
        self.primary_var = tk.StringVar(value='Near side captured: —')
        self.secondary_var = tk.StringVar(value='Far side captured: —')
        self.delta_var = tk.StringVar(value='Material: Even')
        ttk.Label(self, textvariable=self.primary_var, anchor='w').pack(fill='x')
        ttk.Label(self, textvariable=self.secondary_var, anchor='w').pack(fill='x')
        ttk.Label(self, textvariable=self.delta_var, anchor='w').pack(fill='x')

    def update_board(self, board: chess.Board, *, player_color: chess.Color, near_side: bool) -> None:
        white_captured, black_captured, delta = captured_pieces_and_material(board)
        if near_side:
            owner_is_white = player_color == chess.WHITE
        else:
            owner_is_white = player_color == chess.BLACK
        owner_label = 'White' if owner_is_white else 'Black'
        owner_captured = white_captured if owner_is_white else black_captured
        other_label = 'Black' if owner_is_white else 'White'
        other_captured = black_captured if owner_is_white else white_captured
        self.primary_var.set(f"{owner_label} captured: {' '.join(owner_captured) if owner_captured else '—'}")
        self.secondary_var.set(f"{other_label} captured: {' '.join(other_captured) if other_captured else '—'}")
        if delta > 0:
            self.delta_var.set(f'Material: White +{delta}')
        elif delta < 0:
            self.delta_var.set(f'Material: Black +{-delta}')
        else:
            self.delta_var.set('Material: Even')
