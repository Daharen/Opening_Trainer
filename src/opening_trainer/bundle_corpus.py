from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path

import chess

from .bundle_contract import (
    BUNDLE_MANIFEST_NAME,
    SUPPORTED_BUNDLE_MOVE_KEY_FORMAT,
    SUPPORTED_BUNDLE_POSITION_KEY_FORMAT,
    is_supported_builder_aggregate_bundle,
)


@dataclass(frozen=True)
class BuilderAggregateCandidate:
    uci: str
    raw_count: int


@dataclass(frozen=True)
class BuilderAggregatePosition:
    position_key: str
    total_observed_count: int
    candidates: tuple[BuilderAggregateCandidate, ...]


@dataclass(frozen=True)
class BuilderAggregateBundleMetadata:
    bundle_dir: Path
    manifest_path: Path
    aggregate_path: Path
    manifest: dict[str, object]
    position_key_format: str
    move_key_format: str
    payload_status: str | None
    provider_label: str = "corpus_aggregate_bundle"


def normalize_builder_position_key(board: chess.Board) -> str:
    fen_parts = board.fen().split(" ")
    en_passant = fen_parts[3]
    if en_passant != "-":
        maybe_ep = chess.parse_square(en_passant)
        if not board.has_legal_en_passant():
            en_passant = "-"
        elif board.ep_square != maybe_ep:
            en_passant = chess.square_name(board.ep_square) if board.ep_square is not None else "-"
    return " ".join([fen_parts[0], fen_parts[1], fen_parts[2], en_passant, "0", "1"])


class BuilderAggregateCorpusProvider:
    def __init__(self, bundle_dir: str | Path, rng=None):
        self.bundle_dir = Path(bundle_dir)
        self.rng = rng or random
        self.metadata, self.position_index = self._load_bundle()

    def _load_bundle(self) -> tuple[BuilderAggregateBundleMetadata, dict[str, BuilderAggregatePosition]]:
        manifest_path = self.bundle_dir / BUNDLE_MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        supported, aggregate_path, failure_reason = is_supported_builder_aggregate_bundle(manifest, self.bundle_dir)
        if not supported or aggregate_path is None:
            raise ValueError(failure_reason or f"Unsupported builder aggregate bundle at {self.bundle_dir}")
        position_key_format = manifest.get("position_key_format")
        if position_key_format != SUPPORTED_BUNDLE_POSITION_KEY_FORMAT:
            raise ValueError(f"Unsupported position_key_format: {position_key_format!r}")
        move_key_format = manifest.get("move_key_format")
        if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
            raise ValueError(f"Unsupported move_key_format: {move_key_format!r}")
        payload_status = manifest.get("payload_status")

        index: dict[str, BuilderAggregatePosition] = {}
        with aggregate_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                row = json.loads(line)
                position_key = row["position_key"]
                candidates = tuple(
                    BuilderAggregateCandidate(
                        uci=str(candidate["uci"]),
                        raw_count=int(candidate["raw_count"]),
                    )
                    for candidate in row.get("candidate_moves", [])
                )
                total_observed_count = int(row.get("total_observed_count", sum(candidate.raw_count for candidate in candidates)))
                index[position_key] = BuilderAggregatePosition(
                    position_key=position_key,
                    total_observed_count=total_observed_count,
                    candidates=candidates,
                )
        metadata = BuilderAggregateBundleMetadata(
            bundle_dir=self.bundle_dir.resolve(),
            manifest_path=manifest_path.resolve(),
            aggregate_path=aggregate_path.resolve(),
            manifest=manifest,
            position_key_format=position_key_format,
            move_key_format=move_key_format,
            payload_status=payload_status,
        )
        return metadata, index
