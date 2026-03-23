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
    candidate_row_count: int
    unsupported_candidate_row_count: int


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


@dataclass(frozen=True)
class BuilderAggregateParseIssue:
    reason_code: str
    detail: str
    line_number: int | None = None


class BuilderAggregateParseError(ValueError):
    def __init__(self, reason_code: str, detail: str, *, line_number: int | None = None):
        self.reason_code = reason_code
        self.detail = detail
        self.line_number = line_number
        suffix = f" line={line_number}" if line_number is not None else ""
        super().__init__(f"{reason_code}: {detail}{suffix}")



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
        self.last_parse_issue: BuilderAggregateParseIssue | None = None
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
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.last_parse_issue = BuilderAggregateParseIssue(
                        reason_code="bundle_provider_parse_error",
                        detail=f"invalid json row: {exc.msg}",
                        line_number=line_number,
                    )
                    raise BuilderAggregateParseError(
                        "bundle_provider_parse_error",
                        f"invalid json row: {exc.msg}",
                        line_number=line_number,
                    ) from exc
                position = self._parse_position_row(row, line_number=line_number)
                index[position.position_key] = position
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

    def _parse_position_row(self, row: object, *, line_number: int) -> BuilderAggregatePosition:
        if not isinstance(row, dict):
            raise BuilderAggregateParseError("bundle_provider_parse_error", "aggregate row must be a JSON object", line_number=line_number)
        position_key = row.get("position_key")
        if not isinstance(position_key, str) or not position_key.strip():
            raise BuilderAggregateParseError("bundle_provider_parse_error", "aggregate row is missing a valid position_key", line_number=line_number)
        candidate_rows = row.get("candidate_moves", [])
        if not isinstance(candidate_rows, list):
            raise BuilderAggregateParseError("bundle_provider_parse_error", "aggregate row candidate_moves must be a list", line_number=line_number)
        candidates, unsupported_count = self._parse_candidate_rows(candidate_rows)
        total_observed_value = row.get("total_observations", row.get("total_observed_count"))
        if total_observed_value is None:
            total_observed_count = sum(candidate.raw_count for candidate in candidates)
        else:
            try:
                total_observed_count = int(total_observed_value)
            except (TypeError, ValueError) as exc:
                raise BuilderAggregateParseError(
                    "bundle_provider_parse_error",
                    f"aggregate row has invalid total observations value {total_observed_value!r}",
                    line_number=line_number,
                ) from exc
        return BuilderAggregatePosition(
            position_key=position_key,
            total_observed_count=total_observed_count,
            candidates=candidates,
            candidate_row_count=len(candidate_rows),
            unsupported_candidate_row_count=unsupported_count,
        )

    def _parse_candidate_rows(self, candidate_rows: list[object]) -> tuple[tuple[BuilderAggregateCandidate, ...], int]:
        parsed: list[BuilderAggregateCandidate] = []
        unsupported_count = 0
        for candidate in candidate_rows:
            parsed_candidate = self._parse_candidate_row(candidate)
            if parsed_candidate is None:
                unsupported_count += 1
                continue
            parsed.append(parsed_candidate)
        return tuple(parsed), unsupported_count

    def _parse_candidate_row(self, candidate: object) -> BuilderAggregateCandidate | None:
        if not isinstance(candidate, dict):
            return None
        move_key_format = candidate.get("move_key_format")
        move_key = candidate.get("move_key")
        if isinstance(move_key, str):
            if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
                return None
            uci = move_key
        else:
            uci = candidate.get("uci")
            if not isinstance(uci, str):
                return None
        raw_count = candidate.get("raw_count")
        try:
            parsed_raw_count = int(raw_count)
        except (TypeError, ValueError):
            return None
        return BuilderAggregateCandidate(uci=uci, raw_count=parsed_raw_count)
