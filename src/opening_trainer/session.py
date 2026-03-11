import random
import chess

from .board import GameBoard
from .opponent import OpponentProvider
from .evaluator import MoveEvaluator


class TrainingSession:

    def __init__(self):
        self.board = GameBoard()
        self.opponent = OpponentProvider()
        self.evaluator = MoveEvaluator()

        self.player_color = chess.WHITE
        self.player_move_count = 0

    def start_new_game(self):
        self.board.reset()

        self.player_color = random.choice([chess.WHITE, chess.BLACK])
        self.player_move_count = 0

        print("\n--- New Training Game ---")

        if self.player_color == chess.WHITE:
            print("You are WHITE")
        else:
            print("You are BLACK")

    def run_session(self):

        while True:

            if self.board.turn() == self.player_color:
                if not self.player_move():
                    return

            else:
                self.opponent_move()

    def player_move(self):

        print(self.board)

        move_str = input("Your move: ")

        if not self.board.is_legal(move_str):
            print("Illegal move.")
            return True

        move = self.board.push(move_str)

        self.player_move_count += 1

        result = self.evaluator.evaluate(self.board, move, self.player_move_count)

        if result == "FAIL":
            print("FAIL — move not Book or Better")
            print("Restarting training game")
            return False

        if self.player_move_count >= 5:
            print("SUCCESS — Opening passed")
            return False

        return True

    def opponent_move(self):

        move = self.opponent.choose_move(self.board)

        print(f"Opponent plays {move}")

        self.board.push(move)
