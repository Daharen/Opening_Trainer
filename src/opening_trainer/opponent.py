from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import chess

from .corpus import DEFAULT_ARTIFACT_PATH, PositionRecord, load_artifact, normalize_position_key
from .runtime import corpus_status_detail


@dataclass(frozen=True)
class OpponentMoveChoice:
    move: chess.Move
    position_key: str
    selected_via: str
    raw_count: int
    effective_weight: float
    total_observed_count: int
    sparse: bool
    sparse_reason: str | None
    fallback_applied: bool
    candidate_summaries: tuple[dict[str, object], ...]


class OpponentMoveProvider(Protocol):
    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        ...


class RandomOpponentProvider:
    def __init__(self, rng=None):
        self.rng = rng or random

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        legal_moves = list(board.legal_moves)
        if not legal_moves:
            raise ValueError("RandomOpponentProvider received a position with no legal moves.")
        move = self.rng.choice(legal_moves)
        return OpponentMoveChoice(
            move=move,
            position_key=normalize_position_key(board),
            selected_via="random_legal_move",
            raw_count=0,
            effective_weight=1.0,
            total_observed_count=0,
            sparse=False,
            sparse_reason=None,
            fallback_applied=False,
            candidate_summaries=tuple({"uci": legal.uci(), "raw_count": 0, "effective_weight": 1.0} for legal in legal_moves),
        )


class CorpusBackedOpponentProvider:
    def __init__(self, artifact_path: str | Path = DEFAULT_ARTIFACT_PATH, rng=None):
        self.artifact_path = Path(artifact_path)
        self.rng = rng or random
        self.artifact = load_artifact(self.artifact_path)
        self.position_index = {position.position_key: position for position in self.artifact.positions}

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        lookup_chain = self._lookup_chain(board)
        for selected_via, position_key in lookup_chain:
            position = self.position_index.get(position_key)
            if position is None:
                continue
            legal_candidates = [candidate for candidate in position.candidate_moves if chess.Move.from_uci(candidate.uci) in board.legal_moves]
            if not legal_candidates:
                continue
            weights = [candidate.effective_weight for candidate in legal_candidates]
            selected = self.rng.choices(legal_candidates, weights=weights, k=1)[0]
            return OpponentMoveChoice(
                move=chess.Move.from_uci(selected.uci),
                position_key=position.position_key,
                selected_via=selected_via,
                raw_count=selected.raw_count,
                effective_weight=selected.effective_weight,
                total_observed_count=position.total_observed_count,
                sparse=position.sparse,
                sparse_reason=position.sparse_reason,
                fallback_applied=selected_via != "exact_position",
                candidate_summaries=tuple(
                    {
                        "uci": candidate.uci,
                        "raw_count": candidate.raw_count,
                        "effective_weight": candidate.effective_weight,
                    }
                    for candidate in legal_candidates
                ),
            )
        raise LookupError(
            f"No corpus-backed move available for position {normalize_position_key(board)} using artifact {self.artifact_path}."
        )

    def _lookup_chain(self, board: chess.Board) -> list[tuple[str, str]]:
        positions: list[tuple[str, str]] = [("exact_position", normalize_position_key(board))]
        rewind_board = board.copy(stack=True)
        fallback_index = 0
        while rewind_board.move_stack:
            rewind_board.pop()
            fallback_index += 1
            positions.append((f"prefix_backoff_{fallback_index}", normalize_position_key(rewind_board)))
        return positions


class OpponentProvider:
    def __init__(self, artifact_path: str | Path = DEFAULT_ARTIFACT_PATH, rng=None):
        self.rng = rng or random
        self.artifact_path = Path(artifact_path)
        self.random_provider = RandomOpponentProvider(rng=self.rng)
        self.corpus_provider: CorpusBackedOpponentProvider | None = None
        self.mode = "random_fallback"
        self.status_message = "Corpus artifact not loaded; opponent provider is using explicit provisional random fallback."
        self.last_choice: OpponentMoveChoice | None = None
        if self.artifact_path.exists():
            self.corpus_provider = CorpusBackedOpponentProvider(self.artifact_path, rng=self.rng)
            self.mode = "corpus"
            self.status_message = corpus_status_detail(self.artifact_path)
        else:
            self.status_message = corpus_status_detail(None)

    def choose_move(self, board: chess.Board) -> chess.Move:
        self.last_choice = self.choose_move_with_context(board)
        return self.last_choice.move

    def choose_move_with_context(self, board: chess.Board) -> OpponentMoveChoice:
        if self.corpus_provider is not None:
            return self.corpus_provider.choose_move(board)
        return self.random_provider.choose_move(board)
