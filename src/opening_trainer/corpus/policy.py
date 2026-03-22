from __future__ import annotations

import chess.pgn

from .constants import (
    DEFAULT_RATING_MAX,
    DEFAULT_RATING_MIN,
    DEFAULT_RATING_POLICY,
    DEFAULT_RETAINED_PLY_DEPTH,
    DEFAULT_SPARSE_MIN_POSITION_COUNT,
    DEFAULT_TAIL_WEIGHT_POWER,
)


class RatingBandPolicy:
    def __init__(
        self,
        minimum: int = DEFAULT_RATING_MIN,
        maximum: int = DEFAULT_RATING_MAX,
        policy_name: str = DEFAULT_RATING_POLICY,
    ):
        self.minimum = minimum
        self.maximum = maximum
        self.policy_name = policy_name

    def accepts(self, headers: chess.pgn.Headers) -> bool:
        white_elo = self._parse_rating(headers.get("WhiteElo"))
        black_elo = self._parse_rating(headers.get("BlackElo"))
        if white_elo is None or black_elo is None:
            return False
        return self.minimum <= white_elo <= self.maximum and self.minimum <= black_elo <= self.maximum

    def describe(self) -> dict[str, object]:
        return {
            "policy": self.policy_name,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "requires_both_players": True,
        }

    def _parse_rating(self, value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None


class SparseWeightPolicy:
    def __init__(
        self,
        min_position_count: int = DEFAULT_SPARSE_MIN_POSITION_COUNT,
        tail_weight_power: float = DEFAULT_TAIL_WEIGHT_POWER,
    ):
        self.min_position_count = min_position_count
        self.tail_weight_power = tail_weight_power

    def effective_weight(self, raw_count: int) -> float:
        return float(raw_count) ** self.tail_weight_power

    def is_sparse(self, total_observed_count: int) -> bool:
        return total_observed_count < self.min_position_count

    def describe_sparse(self, total_observed_count: int) -> tuple[bool, str | None]:
        sparse = self.is_sparse(total_observed_count)
        if not sparse:
            return False, None
        return True, f"total_observed_count_below_{self.min_position_count}"

    def describe(self) -> dict[str, object]:
        return {
            "min_position_count": self.min_position_count,
            "tail_weight_power": self.tail_weight_power,
            "sparse_fallback": "prefix_backoff",
        }


def retained_ply_depth() -> int:
    return DEFAULT_RETAINED_PLY_DEPTH
