from __future__ import annotations

import chess

from .types import BookAuthorityResult, ReasonCode


class OpeningBookAuthority:
    """Explicit seam for opening-book validation.

    The current repository does not ship a concrete book asset, so the default
    implementation reports that book authority is unavailable rather than
    silently pretending a move is book-approved.
    """

    def evaluate(self, board_before_move: chess.Board, played_move: chess.Move) -> BookAuthorityResult:
        return BookAuthorityResult(
            accepted=False,
            available=False,
            reason_code=ReasonCode.BOOK_UNAVAILABLE,
            reason_text="Book authority unavailable for this position.",
            metadata={"book_available": False, "position_fen": board_before_move.fen()},
        )
