from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path

import chess

DEFAULT_DATASET_PATHS: tuple[Path, ...] = (
    Path(r"C:/Users/just_/Opening-Trainer/data/opening_book_names_extracted/chess-openings-master"),
    Path("data/opening_book_names_extracted/chess-openings-master"),
)
ENV_OPENING_NAME_DATASET_DIR = "OPENING_TRAINER_OPENING_NAMES_DIR"
_TSV_FILENAMES: tuple[str, ...] = ("a.tsv", "b.tsv", "c.tsv", "d.tsv", "e.tsv")
_RESULT_TOKENS = {"1-0", "0-1", "1/2-1/2", "*"}
_MOVE_NUMBER_TOKEN = re.compile(r"^\d+\.(\.\.)?$")


@dataclass(frozen=True)
class OpeningNameDatasetStatus:
    loaded: bool
    source_path: str | None
    entry_count: int
    detail: str


class OpeningNameDataset:
    """External naming authority backed by Lichess openings TSV files."""

    def __init__(self, names_by_prefix: dict[tuple[str, ...], str], source_path: Path | None = None):
        self._names_by_prefix = names_by_prefix
        self.source_path = source_path

    @property
    def entry_count(self) -> int:
        return len(self._names_by_prefix)

    @property
    def loaded(self) -> bool:
        return bool(self._names_by_prefix)

    @classmethod
    def load(cls, dataset_dir: str | Path | None = None) -> "OpeningNameDataset":
        resolved_dir = cls._resolve_dataset_dir(dataset_dir)
        if resolved_dir is None:
            return cls({}, None)
        names_by_prefix: dict[tuple[str, ...], str] = {}
        for filename in _TSV_FILENAMES:
            path = resolved_dir / filename
            if not path.is_file():
                continue
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                for row in reader:
                    opening_name = (row.get("name") or "").strip()
                    pgn_moves = (row.get("pgn") or "").strip()
                    if not opening_name or not pgn_moves:
                        continue
                    uci_sequence = _parse_pgn_to_uci_sequence(pgn_moves)
                    if not uci_sequence:
                        continue
                    key = tuple(uci_sequence)
                    existing = names_by_prefix.get(key)
                    if existing is None or len(opening_name) > len(existing):
                        names_by_prefix[key] = opening_name
        return cls(names_by_prefix, resolved_dir)

    @staticmethod
    def _resolve_dataset_dir(dataset_dir: str | Path | None) -> Path | None:
        candidates: list[Path] = []
        env_path = os.getenv(ENV_OPENING_NAME_DATASET_DIR)
        if env_path:
            candidates.append(Path(env_path))
        if dataset_dir is not None:
            candidates.append(Path(dataset_dir))
        candidates.extend(DEFAULT_DATASET_PATHS)
        return next((path for path in candidates if path.is_dir()), None)

    def status(self) -> OpeningNameDatasetStatus:
        if self.loaded and self.source_path is not None:
            detail = f"loaded=yes; source={self.source_path}; entries={self.entry_count}"
            return OpeningNameDatasetStatus(True, str(self.source_path), self.entry_count, detail)
        if self.source_path is None:
            return OpeningNameDatasetStatus(False, None, 0, "loaded=no; source=unresolved")
        return OpeningNameDatasetStatus(False, str(self.source_path), 0, f"loaded=no; source={self.source_path}; entries=0")

    def opening_name_for_board(self, board: chess.Board) -> str | None:
        if not self.loaded:
            return None
        sequence = tuple(move.uci() for move in board.move_stack)
        return self._names_by_prefix.get(sequence)


def _parse_pgn_to_uci_sequence(pgn_moves: str) -> tuple[str, ...]:
    board = chess.Board()
    uci_moves: list[str] = []
    for token in pgn_moves.split():
        if _MOVE_NUMBER_TOKEN.match(token):
            continue
        if token in _RESULT_TOKENS:
            continue
        try:
            move = board.parse_san(token)
        except ValueError:
            return ()
        uci_moves.append(move.uci())
        board.push(move)
    return tuple(uci_moves)
