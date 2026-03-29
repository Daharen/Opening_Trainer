from __future__ import annotations

import json
from pathlib import Path

import chess
import chess.polyglot

from .types import BookAuthorityResult, ReasonCode


class OpeningBookAuthority:
    """Explicit seam for opening-book validation."""

    def __init__(self, book_path: str | Path | None = None, opening_name_index_path: str | Path | None = None):
        self.book_path = Path(book_path) if book_path is not None else None
        self.opening_name_index_path = Path(opening_name_index_path) if opening_name_index_path is not None else None
        self._opening_names_by_position = self._load_opening_name_index()

    @staticmethod
    def normalized_position_key(board: chess.Board) -> str:
        fen_parts = board.fen().split(" ")
        en_passant = fen_parts[3]
        if en_passant != "-":
            maybe_ep = chess.parse_square(en_passant)
            if not board.has_legal_en_passant():
                en_passant = "-"
            elif board.ep_square != maybe_ep:
                en_passant = chess.square_name(board.ep_square) if board.ep_square is not None else "-"
        return " ".join([fen_parts[0], fen_parts[1], fen_parts[2], en_passant])

    def opening_name_for_position(self, board: chess.Board) -> str | None:
        if not self._opening_names_by_position:
            return None
        normalized = self.normalized_position_key(board)
        direct = self._opening_names_by_position.get(normalized)
        if direct:
            return direct
        return self._opening_names_by_position.get(board.fen())

    def _load_opening_name_index(self) -> dict[str, str]:
        if self.book_path is None:
            return {}
        candidates: list[Path] = []
        if self.opening_name_index_path is not None:
            candidates.append(self.opening_name_index_path)
        candidates.extend(
            [
                self.book_path.with_suffix(".openings.json"),
                Path(str(self.book_path) + ".openings.json"),
            ]
        )
        selected = next((path for path in candidates if path.exists() and path.is_file()), None)
        if selected is None:
            return {}
        try:
            payload = json.loads(selected.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return self._parse_opening_name_payload(payload)

    @staticmethod
    def _parse_opening_name_payload(payload: object) -> dict[str, str]:
        opening_names: dict[str, str] = {}
        if isinstance(payload, dict):
            for key, value in payload.items():
                if isinstance(key, str) and isinstance(value, str) and value.strip():
                    opening_names[key] = value.strip()
            return opening_names
        if isinstance(payload, list):
            for row in payload:
                if not isinstance(row, dict):
                    continue
                key = row.get("position_key") or row.get("fen")
                value = row.get("opening_name") or row.get("name")
                if isinstance(key, str) and isinstance(value, str) and value.strip():
                    opening_names[key] = value.strip()
        return opening_names

    def evaluate(self, board_before_move: chess.Board, played_move: chess.Move) -> BookAuthorityResult:
        opening_name = self.opening_name_for_position(board_before_move)
        if self.book_path is None:
            return BookAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.BOOK_UNAVAILABLE,
                reason_text="Book authority unavailable for this position (no book file configured).",
                metadata={
                    "book_available": False,
                    "position_fen": board_before_move.fen(),
                    "opening_name": opening_name,
                },
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
                metadata={
                    "book_available": False,
                    "book_path": str(self.book_path),
                    "position_fen": board_before_move.fen(),
                    "opening_name": opening_name,
                },
            )
        except OSError as exc:
            return BookAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.BOOK_UNAVAILABLE,
                reason_text=f"Book authority unavailable: could not read {self.book_path} ({exc}).",
                metadata={
                    "book_available": False,
                    "book_path": str(self.book_path),
                    "position_fen": board_before_move.fen(),
                    "opening_name": opening_name,
                },
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
                    "opening_name": opening_name,
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
                "opening_name": opening_name,
            },
        )
