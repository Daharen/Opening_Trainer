from __future__ import annotations

import chess

from ..bundle_corpus import normalize_builder_position_key


def normalize_position_key(board: chess.Board) -> str:
    return normalize_builder_position_key(board)


def fallback_position_key(position_key: str) -> str | None:
    return None
