from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CandidateMoveRecord:
    uci: str
    raw_count: int
    effective_weight: float

    def to_dict(self) -> dict[str, object]:
        return {
            "uci": self.uci,
            "raw_count": self.raw_count,
            "effective_weight": self.effective_weight,
        }


@dataclass(frozen=True)
class PositionRecord:
    position_key: str
    side_to_move: str
    total_observed_count: int
    sparse: bool
    sparse_reason: str | None
    fallback_position_key: str | None
    candidate_moves: tuple[CandidateMoveRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return {
            "position_key": self.position_key,
            "side_to_move": self.side_to_move,
            "total_observed_count": self.total_observed_count,
            "sparse": self.sparse,
            "sparse_reason": self.sparse_reason,
            "fallback_position_key": self.fallback_position_key,
            "candidate_moves": [move.to_dict() for move in self.candidate_moves],
        }


@dataclass(frozen=True)
class CorpusArtifact:
    schema_version: int
    source_files: tuple[str, ...]
    target_rating_band: dict[str, int]
    rating_policy: str
    retained_ply_depth: int
    sparse_policy: dict[str, object]
    weighting_policy: dict[str, object]
    positions: tuple[PositionRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_files": list(self.source_files),
            "target_rating_band": self.target_rating_band,
            "rating_policy": self.rating_policy,
            "retained_ply_depth": self.retained_ply_depth,
            "sparse_policy": self.sparse_policy,
            "weighting_policy": self.weighting_policy,
            "positions": [position.to_dict() for position in self.positions],
        }
