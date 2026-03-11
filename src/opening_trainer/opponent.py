import random


class OpponentProvider:

    def choose_move(self, board):

        legal_moves = list(board.board.legal_moves)

        return random.choice(legal_moves)
