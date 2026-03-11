import random
import chess


class OpponentProvider:
    """
    Provisional opponent move source.

    Current behavior samples uniformly from legal moves.
    Future lanes should replace this with weighted move selection derived
    from low-ELO human opening corpus data.
    """

    def choose_move(self, board: chess.Board) -> chess.Move:
        legal_moves = list(board.legal_moves)

        if not legal_moves:
            raise ValueError("OpponentProvider received a position with no legal moves.")

        return random.choice(legal_moves)
