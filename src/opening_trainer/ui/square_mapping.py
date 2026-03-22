from __future__ import annotations

import chess


def square_to_display(square: chess.Square, player_color: chess.Color) -> tuple[int, int]:
    file_index = chess.square_file(square)
    rank_index = chess.square_rank(square)

    if player_color == chess.WHITE:
        return 7 - rank_index, file_index
    return rank_index, 7 - file_index


def display_to_square(row: int, col: int, player_color: chess.Color) -> chess.Square:
    if player_color == chess.WHITE:
        file_index = col
        rank_index = 7 - row
    else:
        file_index = 7 - col
        rank_index = row
    return chess.square(file_index, rank_index)
