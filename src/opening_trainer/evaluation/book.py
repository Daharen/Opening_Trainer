from __future__ import annotations

from pathlib import Path

import chess
import chess.polyglot

from .types import BookAuthorityResult, ReasonCode


class OpeningBookAuthority:
    """Explicit seam for opening-book validation."""

    def __init__(self, book_path: str | Path | None = None):
        self.book_path = Path(book_path) if book_path is not None else None

    def evaluate(self, board_before_move: chess.Board, played_move: chess.Move) -> BookAuthorityResult:
        if self.book_path is None:
            return BookAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.BOOK_UNAVAILABLE,
                reason_text="Book authority unavailable for this position (no book file configured).",
                metadata={"book_available": False, "position_fen": board_before_move.fen()},
            )

        try:
            with chess.polyglot.open_reader(str(self.book_path)) as reader:
                entries = list(reader.find_all(board_before_move))
        except FileNotFoundError:
            return BookAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.BOOK_UNAVAILABLE,
                reason_text=f"Book authority unavailable: book file not found at {self.book_path}.",
                metadata={"book_available": False, "book_path": str(self.book_path), "position_fen": board_before_move.fen()},
            )
        except OSError as exc:
            return BookAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.BOOK_UNAVAILABLE,
                reason_text=f"Book authority unavailable: could not read {self.book_path} ({exc}).",
                metadata={"book_available": False, "book_path": str(self.book_path), "position_fen": board_before_move.fen()},
            )

        matched = next((entry for entry in entries if entry.move == played_move), None)
        candidate_moves = [entry.move.uci() for entry in entries]
        if matched is not None:
            return BookAuthorityResult(
                accepted=True,
                available=True,
                reason_code=ReasonCode.BOOK_HIT,
                reason_text="Accepted via opening-book membership.",
                candidate_move_uci=played_move.uci(),
                metadata={
                    "book_available": True,
                    "book_path": str(self.book_path),
                    "position_fen": board_before_move.fen(),
                    "candidate_moves": candidate_moves,
                    "entry_count": len(entries),
                    "weight": matched.weight,
                    "learn": matched.learn,
                },
            )

        return BookAuthorityResult(
            accepted=False,
            available=True,
            reason_code=ReasonCode.BOOK_UNAVAILABLE,
            reason_text="Move not present in opening-book membership for this position.",
            metadata={
                "book_available": True,
                "book_path": str(self.book_path),
                "position_fen": board_before_move.fen(),
                "candidate_moves": candidate_moves,
                "entry_count": len(entries),
            },
        )
