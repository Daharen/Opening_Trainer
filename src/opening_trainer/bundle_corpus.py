from __future__ import annotations

import json
import random
import sqlite3
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import chess

from .bundle_contract import (
    BUNDLE_MANIFEST_NAME,
    SUPPORTED_BUNDLE_MOVE_KEY_FORMAT,
    SUPPORTED_BUNDLE_POSITION_KEY_FORMAT,
    is_supported_builder_aggregate_bundle,
    read_corpus_metadata_contract,
    resolve_bundle_payload,
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
    payload_format: str
    provider_label: str


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
    return " ".join([fen_parts[0], fen_parts[1], fen_parts[2], en_passant])


class _BuilderAggregateDataProvider(Protocol):
    metadata: BuilderAggregateBundleMetadata

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        ...


class JsonlAggregateCorpusProvider:
    def __init__(self, bundle_dir: Path, manifest: dict[str, object], payload_path: Path, rng=None):
        self.bundle_dir = bundle_dir
        self.rng = rng or random
        self.last_parse_issue: BuilderAggregateParseIssue | None = None
        self.metadata, self.position_index = self._load_bundle(manifest, payload_path)

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        return self.position_index.get(position_key)

    def _load_bundle(self, manifest: dict[str, object], payload_path: Path) -> tuple[BuilderAggregateBundleMetadata, dict[str, BuilderAggregatePosition]]:
        position_key_format = manifest.get("position_key_format")
        if position_key_format != SUPPORTED_BUNDLE_POSITION_KEY_FORMAT:
            raise ValueError(f"Unsupported position_key_format: {position_key_format!r}")
        move_key_format = manifest.get("move_key_format")
        if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
            raise ValueError(f"Unsupported move_key_format: {move_key_format!r}")
        payload_status = manifest.get("payload_status")

        index: dict[str, BuilderAggregatePosition] = {}
        with payload_path.open("r", encoding="utf-8") as handle:
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
            manifest_path=(self.bundle_dir / BUNDLE_MANIFEST_NAME).resolve(),
            aggregate_path=payload_path.resolve(),
            manifest=manifest,
            position_key_format=position_key_format,
            move_key_format=move_key_format,
            payload_status=payload_status,
            payload_format="jsonl",
            provider_label="corpus_aggregate_bundle_jsonl",
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


class SQLiteAggregateCorpusProvider:
    def __init__(self, bundle_dir: Path, manifest: dict[str, object], payload_path: Path, cache_size: int = 512):
        self.bundle_dir = bundle_dir
        self.payload_path = payload_path
        self.cache_size = max(0, cache_size)
        self._position_cache: OrderedDict[str, BuilderAggregatePosition | None] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._thread_local = threading.local()
        self.metadata = self._build_metadata(manifest)

    def _build_metadata(self, manifest: dict[str, object]) -> BuilderAggregateBundleMetadata:
        position_key_format = manifest.get("position_key_format")
        move_key_format = manifest.get("move_key_format")
        if position_key_format != SUPPORTED_BUNDLE_POSITION_KEY_FORMAT:
            raise ValueError(f"Unsupported position_key_format: {position_key_format!r}")
        if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
            raise ValueError(f"Unsupported move_key_format: {move_key_format!r}")
        return BuilderAggregateBundleMetadata(
            bundle_dir=self.bundle_dir.resolve(),
            manifest_path=(self.bundle_dir / BUNDLE_MANIFEST_NAME).resolve(),
            aggregate_path=self.payload_path.resolve(),
            manifest=manifest,
            position_key_format=position_key_format,
            move_key_format=move_key_format,
            payload_status=manifest.get("payload_status"),
            payload_format="sqlite",
            provider_label="corpus_aggregate_bundle_sqlite",
        )

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        with self._cache_lock:
            cached = self._position_cache.get(position_key)
            if cached is not None or position_key in self._position_cache:
                return cached

        connection = self._get_connection_for_current_thread()
        position_cursor = connection.cursor()
        move_cursor = connection.cursor()
        try:
            position_row = position_cursor.execute(
                "SELECT position_id, position_key, position_key_format, side_to_move, candidate_move_count, total_observations FROM positions WHERE position_key = ? LIMIT 1",
                (position_key,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise ValueError(f"SQLite corpus schema mismatch or unsupported bundle schema: {exc}") from exc
        if position_row is None:
            return self._remember(position_key, None)

        try:
            move_rows = move_cursor.execute(
                "SELECT move_key, move_key_format, raw_count, example_san FROM moves WHERE position_id = ? ORDER BY raw_count DESC, move_key ASC",
                (position_row["position_id"],),
            ).fetchall()
        except sqlite3.OperationalError as exc:
            raise ValueError(f"SQLite corpus schema mismatch or unsupported bundle schema: {exc}") from exc
        candidates = tuple(
            BuilderAggregateCandidate(uci=str(move_row["move_key"]), raw_count=int(move_row["raw_count"]))
            for move_row in move_rows
            if move_row["move_key"] is not None and move_row["move_key_format"] == SUPPORTED_BUNDLE_MOVE_KEY_FORMAT
        )
        total_observed_count = int(position_row["total_observations"] or 0)
        if total_observed_count <= 0:
            total_observed_count = sum(candidate.raw_count for candidate in candidates)
        candidate_row_count = int(position_row["candidate_move_count"] or 0)
        if candidate_row_count <= 0:
            candidate_row_count = len(move_rows)

        position = BuilderAggregatePosition(
            position_key=str(position_row["position_key"]),
            total_observed_count=total_observed_count,
            candidates=candidates,
            candidate_row_count=candidate_row_count,
            unsupported_candidate_row_count=max(0, len(move_rows) - len(candidates)),
        )
        return self._remember(position_key, position)

    def _get_connection_for_current_thread(self) -> sqlite3.Connection:
        connection = getattr(self._thread_local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(f"file:{self.payload_path}?mode=ro", uri=True)
            connection.row_factory = sqlite3.Row
            self._thread_local.connection = connection
        return connection

    def _remember(self, position_key: str, position: BuilderAggregatePosition | None) -> BuilderAggregatePosition | None:
        if self.cache_size <= 0:
            return position
        with self._cache_lock:
            self._position_cache[position_key] = position
            self._position_cache.move_to_end(position_key)
            while len(self._position_cache) > self.cache_size:
                self._position_cache.popitem(last=False)
        return position


class CompactSQLiteAggregateCorpusProvider:
    """Reader for compact exact payload v2 with position/move dictionaries."""

    def __init__(self, bundle_dir: Path, manifest: dict[str, object], payload_path: Path, cache_size: int = 512):
        self.bundle_dir = bundle_dir
        self.payload_path = payload_path
        self.cache_size = max(0, cache_size)
        self._position_cache: OrderedDict[str, BuilderAggregatePosition | None] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._thread_local = threading.local()
        self.metadata = self._build_metadata(manifest)

    def _build_metadata(self, manifest: dict[str, object]) -> BuilderAggregateBundleMetadata:
        return BuilderAggregateBundleMetadata(
            bundle_dir=self.bundle_dir.resolve(),
            manifest_path=(self.bundle_dir / BUNDLE_MANIFEST_NAME).resolve(),
            aggregate_path=self.payload_path.resolve(),
            manifest=manifest,
            position_key_format=SUPPORTED_BUNDLE_POSITION_KEY_FORMAT,
            move_key_format=SUPPORTED_BUNDLE_MOVE_KEY_FORMAT,
            payload_status=manifest.get("payload_status"),
            payload_format="sqlite",
            provider_label="corpus_aggregate_bundle_sqlite_compact_v2",
        )

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        with self._cache_lock:
            cached = self._position_cache.get(position_key)
            if cached is not None or position_key in self._position_cache:
                return cached
        connection = self._get_connection_for_current_thread()
        position_cursor = connection.cursor()
        move_cursor = connection.cursor()
        try:
            position_row = position_cursor.execute(
                """
                SELECT p.position_id, k.position_key, p.candidate_move_count, p.total_observations
                FROM positions p
                JOIN position_keys k ON k.position_key_id = p.position_key_id
                WHERE k.position_key = ?
                LIMIT 1
                """,
                (position_key,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise ValueError(f"Compact SQLite corpus schema mismatch or unsupported compact schema: {exc}") from exc
        if position_row is None:
            return self._remember(position_key, None)
        try:
            move_rows = move_cursor.execute(
                """
                SELECT d.uci AS move_uci, m.raw_count
                FROM position_moves m
                JOIN move_dictionary d ON d.move_id = m.move_id
                WHERE m.position_id = ?
                ORDER BY m.raw_count DESC, d.uci ASC
                """,
                (position_row["position_id"],),
            ).fetchall()
        except sqlite3.OperationalError as exc:
            raise ValueError(f"Compact SQLite corpus schema mismatch or unsupported compact schema: {exc}") from exc
        candidates = tuple(
            BuilderAggregateCandidate(uci=str(row["move_uci"]), raw_count=int(row["raw_count"]))
            for row in move_rows
            if row["move_uci"] is not None
        )
        total_observed_count = int(position_row["total_observations"] or 0)
        if total_observed_count <= 0:
            total_observed_count = sum(candidate.raw_count for candidate in candidates)
        candidate_row_count = int(position_row["candidate_move_count"] or 0)
        if candidate_row_count <= 0:
            candidate_row_count = len(move_rows)
        position = BuilderAggregatePosition(
            position_key=str(position_row["position_key"]),
            total_observed_count=total_observed_count,
            candidates=candidates,
            candidate_row_count=candidate_row_count,
            unsupported_candidate_row_count=max(0, len(move_rows) - len(candidates)),
        )
        return self._remember(position_key, position)

    def _get_connection_for_current_thread(self) -> sqlite3.Connection:
        connection = getattr(self._thread_local, "connection", None)
        if connection is None:
            connection = sqlite3.connect(f"file:{self.payload_path}?mode=ro", uri=True)
            connection.row_factory = sqlite3.Row
            self._thread_local.connection = connection
        return connection

    def _remember(self, position_key: str, position: BuilderAggregatePosition | None) -> BuilderAggregatePosition | None:
        if self.cache_size <= 0:
            return position
        with self._cache_lock:
            self._position_cache[position_key] = position
            self._position_cache.move_to_end(position_key)
            while len(self._position_cache) > self.cache_size:
                self._position_cache.popitem(last=False)
        return position


class BuilderAggregateCorpusProvider:
    def __init__(self, bundle_dir: str | Path, rng=None):
        self.bundle_dir = Path(bundle_dir)
        self.rng = rng or random
        self.last_parse_issue: BuilderAggregateParseIssue | None = None
        self._provider = self._load_provider()
        self.metadata = self._provider.metadata
        self.position_index = getattr(self._provider, "position_index", {})

    def _load_provider(self) -> _BuilderAggregateDataProvider:
        manifest_path = self.bundle_dir / BUNDLE_MANIFEST_NAME
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        supported, _payload_path, failure_reason = is_supported_builder_aggregate_bundle(manifest, self.bundle_dir)
        if not supported:
            raise ValueError(failure_reason or f"Unsupported builder aggregate bundle at {self.bundle_dir}")

        payload_resolution, resolution_error = resolve_bundle_payload(manifest, self.bundle_dir)
        if payload_resolution is None:
            raise ValueError(resolution_error or f"Unsupported bundle payload at {self.bundle_dir}")

        contract = read_corpus_metadata_contract(manifest)
        if payload_resolution.payload_format == "sqlite":
            if contract.payload_version == "exact_compact_v2":
                return CompactSQLiteAggregateCorpusProvider(self.bundle_dir, manifest, payload_resolution.payload_path)
            return SQLiteAggregateCorpusProvider(self.bundle_dir, manifest, payload_resolution.payload_path)

        provider = JsonlAggregateCorpusProvider(self.bundle_dir, manifest, payload_resolution.payload_path, rng=self.rng)
        self.last_parse_issue = provider.last_parse_issue
        return provider

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        return self._provider.lookup_position(position_key)
