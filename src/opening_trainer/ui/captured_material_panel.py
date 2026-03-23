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


class CapturedMaterialPanel(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.white_var = tk.StringVar(value='White captured: —')
        self.black_var = tk.StringVar(value='Black captured: —')
        self.delta_var = tk.StringVar(value='Material: Even')
        ttk.Label(self, textvariable=self.white_var, anchor='w').pack(fill='x')
        ttk.Label(self, textvariable=self.black_var, anchor='w').pack(fill='x')
        ttk.Label(self, textvariable=self.delta_var, anchor='w').pack(fill='x')

    def update_board(self, board: chess.Board) -> None:
        white_captured, black_captured, delta = captured_pieces_and_material(board)
        self.white_var.set(f"White captured: {' '.join(white_captured) if white_captured else '—'}")
        self.black_var.set(f"Black captured: {' '.join(black_captured) if black_captured else '—'}")
        if delta > 0:
            self.delta_var.set(f'Material: White +{delta}')
        elif delta < 0:
            self.delta_var.set(f'Material: Black +{-delta}')
        else:
            self.delta_var.set('Material: Even')
