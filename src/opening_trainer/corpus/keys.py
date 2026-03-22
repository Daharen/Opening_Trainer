from __future__ import annotations

import chess


def normalize_position_key(board: chess.Board) -> str:
    fen_parts = board.fen().split(" ")
    return " ".join([*fen_parts[:4], "0", "1"])


def fallback_position_key(position_key: str) -> str | None:
    return None
