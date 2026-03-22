from __future__ import annotations

import chess


class GameBoard:
    def __init__(self):
        self.board = chess.Board()

    def reset(self) -> None:
        self.board.reset()

    def turn(self) -> chess.Color:
        return self.board.turn

    def parse_move(self, move_str: str) -> chess.Move:
        move_str = move_str.strip()
        if not move_str:
            raise ValueError("Move input cannot be empty.")

        try:
            if 4 <= len(move_str) <= 5:
                move = chess.Move.from_uci(move_str)
                if move in self.board.legal_moves:
                    return move
        except ValueError:
            pass

        return self.board.parse_san(move_str)

    def is_legal(self, move_str: str) -> bool:
        try:
            move = self.parse_move(move_str)
        except ValueError:
            return False
        return move in self.board.legal_moves

    def push(self, move_str: str) -> chess.Move:
        move = self.parse_move(move_str)
        self.board.push(move)
        return move

    def legal_moves_from(self, square: chess.Square) -> list[chess.Move]:
        return [move for move in self.board.legal_moves if move.from_square == square]

    def __str__(self) -> str:
        return str(self.board)
