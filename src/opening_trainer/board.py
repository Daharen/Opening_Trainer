import chess


class GameBoard:

    def __init__(self):
        self.board = chess.Board()

    def reset(self):
        self.board.reset()

    def turn(self):
        return self.board.turn

    def is_legal(self, move_str):

        try:
            move = self.board.parse_san(move_str)
        except:
            return False

        return move in self.board.legal_moves

    def push(self, move_str):

        move = self.board.parse_san(move_str)
        self.board.push(move)
        return move

    def __str__(self):
        return str(self.board)
