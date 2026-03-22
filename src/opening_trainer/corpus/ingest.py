from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO

import chess
import chess.pgn

from opening_trainer.zstd_compat import open_text_reader

from .constants import ARTIFACT_SCHEMA_VERSION
from .keys import fallback_position_key, normalize_position_key
from .models import CandidateMoveRecord, CorpusArtifact, PositionRecord
from .policy import RatingBandPolicy, SparseWeightPolicy, retained_ply_depth


@contextmanager
def open_pgn_text(path: str | Path) -> Iterator[TextIO]:
    source_path = Path(path)
    if source_path.suffix == ".zst" and source_path.name.endswith(".pgn.zst"):
        with source_path.open("rb") as compressed_handle:
            with open_text_reader(compressed_handle) as text_handle:
                yield text_handle
        return

    with source_path.open("r", encoding="utf-8") as text_handle:
        yield text_handle


class CorpusIngestor:
    def __init__(
        self,
        rating_policy: RatingBandPolicy | None = None,
        sparse_policy: SparseWeightPolicy | None = None,
        max_ply: int | None = None,
    ):
        self.rating_policy = rating_policy or RatingBandPolicy()
        self.sparse_policy = sparse_policy or SparseWeightPolicy()
        self.max_ply = max_ply or retained_ply_depth()

    def build_artifact(self, pgn_paths: list[str] | tuple[str, ...]) -> CorpusArtifact:
        position_counters: dict[str, Counter[str]] = defaultdict(Counter)
        side_to_move_by_position: dict[str, str] = {}

        for path in [Path(item) for item in pgn_paths]:
            with open_pgn_text(path) as handle:
                while True:
                    game = chess.pgn.read_game(handle)
                    if game is None:
                        break
                    if not self.rating_policy.accepts(game.headers):
                        continue
                    self._consume_game(game, position_counters, side_to_move_by_position)

        positions = self._build_positions(position_counters, side_to_move_by_position)
        return CorpusArtifact(
            schema_version=ARTIFACT_SCHEMA_VERSION,
            source_files=tuple(sorted(str(Path(path)) for path in pgn_paths)),
            target_rating_band={
                "minimum": self.rating_policy.minimum,
                "maximum": self.rating_policy.maximum,
            },
            rating_policy=self.rating_policy.policy_name,
            retained_ply_depth=self.max_ply,
            sparse_policy=self.sparse_policy.describe(),
            weighting_policy={
                "mode": "raw_count_with_tail_suppression",
                "tail_weight_power": self.sparse_policy.tail_weight_power,
                "preserves_raw_counts": True,
            },
            positions=positions,
        )

    def _consume_game(
        self,
        game: chess.pgn.Game,
        position_counters: dict[str, Counter[str]],
        side_to_move_by_position: dict[str, str],
    ) -> None:
        board = game.board()
        for ply_index, move in enumerate(game.mainline_moves(), start=1):
            if ply_index > self.max_ply:
                break
            position_key = normalize_position_key(board)
            position_counters[position_key][move.uci()] += 1
            side_to_move_by_position[position_key] = "white" if board.turn == chess.WHITE else "black"
            board.push(move)

    def _build_positions(
        self,
        position_counters: dict[str, Counter[str]],
        side_to_move_by_position: dict[str, str],
    ) -> tuple[PositionRecord, ...]:
        records: list[PositionRecord] = []
        for position_key in sorted(position_counters):
            counter = position_counters[position_key]
            total_count = sum(counter.values())
            sparse, sparse_reason = self.sparse_policy.describe_sparse(total_count)
            candidate_moves = tuple(
                CandidateMoveRecord(
                    uci=move_uci,
                    raw_count=counter[move_uci],
                    effective_weight=self.sparse_policy.effective_weight(counter[move_uci]),
                )
                for move_uci in sorted(counter)
            )
            records.append(
                PositionRecord(
                    position_key=position_key,
                    side_to_move=side_to_move_by_position[position_key],
                    total_observed_count=total_count,
                    sparse=sparse,
                    sparse_reason=sparse_reason,
                    fallback_position_key=fallback_position_key(position_key) if sparse else None,
                    candidate_moves=candidate_moves,
                )
            )
        return tuple(records)
