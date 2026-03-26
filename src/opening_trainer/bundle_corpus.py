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
    def __init__(self, bundle_dir: Path, manifest: dict[str, object], payload_path: Path, cache_size: int = 512):
        self.bundle_dir = bundle_dir
        self.payload_path = payload_path
        self.cache_size = max(0, cache_size)
        self._position_cache: OrderedDict[str, BuilderAggregatePosition | None] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._thread_local = threading.local()
        self._schema = self._inspect_schema()
        self.metadata = self._build_metadata(manifest)

    def _build_metadata(self, manifest: dict[str, object]) -> BuilderAggregateBundleMetadata:
        return BuilderAggregateBundleMetadata(
            bundle_dir=self.bundle_dir.resolve(),
            manifest_path=(self.bundle_dir / BUNDLE_MANIFEST_NAME).resolve(),
            aggregate_path=self.payload_path.resolve(),
            manifest=manifest,
            position_key_format=str(manifest.get("position_key_format", SUPPORTED_BUNDLE_POSITION_KEY_FORMAT)),
            move_key_format=str(manifest.get("move_key_format", SUPPORTED_BUNDLE_MOVE_KEY_FORMAT)),
            payload_status=manifest.get("payload_status"),
            payload_format="sqlite_compact_v2",
            provider_label="corpus_exact_bundle_sqlite_compact_v2",
        )

    def _inspect_schema(self) -> dict[str, str]:
        connection = sqlite3.connect(f"file:{self.payload_path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            table_rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = {str(row["name"]) for row in table_rows}
            positions_table = "positions" if "positions" in table_names else None
            moves_table = "moves" if "moves" in table_names else None
            if positions_table is None or moves_table is None:
                raise ValueError("compact SQLite payload requires positions and moves tables")
            position_columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({positions_table})").fetchall()}
            move_columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({moves_table})").fetchall()}
            position_key_col = _pick_column(position_columns, ("position_key", "fen_normalized", "fen", "board_fen"))
            position_id_col = _pick_column(position_columns, ("position_id", "id"))
            move_key_col = _pick_column(move_columns, ("uci", "move_uci", "move_key"))
            if not all((position_key_col, position_id_col, move_key_col)):
                raise ValueError("compact SQLite payload is missing one or more required columns")
            move_position_id_col = _pick_column(move_columns, ("position_id", "parent_position_id", "pos_id"))
            raw_count_col = _pick_column(move_columns, ("raw_count", "observed_count", "count"))
            move_id_col = _pick_column(move_columns, ("move_id", "id"))

            assoc_table = None
            assoc_columns: set[str] = set()
            if move_position_id_col is None or raw_count_col is None:
                for table_name in table_names:
                    if table_name in {positions_table, moves_table}:
                        continue
                    candidate_columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
                    assoc_position_id_col = _pick_column(candidate_columns, ("position_id", "parent_position_id", "pos_id"))
                    assoc_move_id_col = _pick_column(candidate_columns, ("move_id", "parent_move_id", "mv_id"))
                    assoc_raw_count_col = _pick_column(candidate_columns, ("raw_count", "observed_count", "count"))
                    if assoc_position_id_col and assoc_move_id_col and assoc_raw_count_col:
                        assoc_table = table_name
                        assoc_columns = candidate_columns
                        break
                if assoc_table is None:
                    raise ValueError("compact SQLite payload is missing one or more required columns")
                if move_id_col is None:
                    raise ValueError("compact SQLite payload normalized schema requires move identifier column in moves table")

            return {
                "positions_table": positions_table,
                "moves_table": moves_table,
                "query_mode": "simplified" if move_position_id_col and raw_count_col else "normalized",
                "position_key_col": position_key_col,
                "position_id_col": position_id_col,
                "position_total_col": _pick_column(position_columns, ("total_observed_count", "total_observations", "observed_total", "total_count")),
                "position_candidate_count_col": _pick_column(position_columns, ("candidate_row_count", "candidate_move_count", "candidate_count", "move_count")),
                "move_position_id_col": move_position_id_col,
                "move_id_col": move_id_col,
                "move_key_col": move_key_col,
                "move_format_col": _pick_column(move_columns, ("move_key_format", "uci_format")),
                "move_raw_count_col": raw_count_col,
                "assoc_table": assoc_table,
                "assoc_position_id_col": _pick_column(assoc_columns, ("position_id", "parent_position_id", "pos_id")) if assoc_table else None,
                "assoc_move_id_col": _pick_column(assoc_columns, ("move_id", "parent_move_id", "mv_id")) if assoc_table else None,
                "assoc_raw_count_col": _pick_column(assoc_columns, ("raw_count", "observed_count", "count")) if assoc_table else None,
            }
        finally:
            connection.close()

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        with self._cache_lock:
            cached = self._position_cache.get(position_key)
            if cached is not None or position_key in self._position_cache:
                return cached

        connection = self._get_connection_for_current_thread()
        schema = self._schema
        position_select_parts = [
            f"{schema['position_id_col']} AS position_id",
            f"{schema['position_key_col']} AS position_key",
        ]
        if schema["position_total_col"]:
            position_select_parts.append(f"{schema['position_total_col']} AS total_observed_count")
        if schema["position_candidate_count_col"]:
            position_select_parts.append(f"{schema['position_candidate_count_col']} AS candidate_row_count")
        position_cursor = connection.cursor()
        row = position_cursor.execute(
            f"SELECT {', '.join(position_select_parts)} FROM {schema['positions_table']} WHERE {schema['position_key_col']} = ? LIMIT 1",
            (position_key,),
        ).fetchone()
        if row is None:
            return self._remember(position_key, None)

        if schema["query_mode"] == "simplified":
            move_select_parts = [
                f"{schema['move_key_col']} AS move_key",
                f"{schema['move_raw_count_col']} AS raw_count",
            ]
            if schema["move_format_col"]:
                move_select_parts.append(f"{schema['move_format_col']} AS move_key_format")
            move_rows = connection.cursor().execute(
                f"SELECT {', '.join(move_select_parts)} FROM {schema['moves_table']} WHERE {schema['move_position_id_col']} = ? ORDER BY {schema['move_raw_count_col']} DESC, {schema['move_key_col']} ASC",
                (row["position_id"],),
            ).fetchall()
        else:
            move_select_parts = [
                f"m.{schema['move_key_col']} AS move_key",
                f"pm.{schema['assoc_raw_count_col']} AS raw_count",
            ]
            if schema["move_format_col"]:
                move_select_parts.append(f"m.{schema['move_format_col']} AS move_key_format")
            move_rows = connection.cursor().execute(
                (
                    f"SELECT {', '.join(move_select_parts)} "
                    f"FROM {schema['assoc_table']} AS pm "
                    f"JOIN {schema['moves_table']} AS m ON m.{schema['move_id_col']} = pm.{schema['assoc_move_id_col']} "
                    f"WHERE pm.{schema['assoc_position_id_col']} = ? "
                    f"ORDER BY pm.{schema['assoc_raw_count_col']} DESC, m.{schema['move_key_col']} ASC"
                ),
                (row["position_id"],),
            ).fetchall()

        candidates: list[BuilderAggregateCandidate] = []
        unsupported_count = 0
        for move_row in move_rows:
            if schema["move_format_col"] and move_row["move_key_format"] not in (None, SUPPORTED_BUNDLE_MOVE_KEY_FORMAT):
                unsupported_count += 1
                continue
            move_key = move_row["move_key"]
            if move_key is None:
                unsupported_count += 1
                continue
            candidates.append(BuilderAggregateCandidate(uci=str(move_key), raw_count=int(move_row["raw_count"] or 0)))

        total_observed_count = int(row["total_observed_count"] or 0) if "total_observed_count" in row.keys() else 0
        if total_observed_count <= 0:
            total_observed_count = sum(candidate.raw_count for candidate in candidates)
        candidate_row_count = int(row["candidate_row_count"] or 0) if "candidate_row_count" in row.keys() else 0
        if candidate_row_count <= 0:
            candidate_row_count = len(move_rows)
        position = BuilderAggregatePosition(
            position_key=str(row["position_key"]),
            total_observed_count=total_observed_count,
            candidates=tuple(candidates),
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


def _pick_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


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

        payload_format_hint = str(manifest.get("payload_format", "")).strip().lower()
        payload_version_hint = str(manifest.get("payload_version", "")).strip().lower()
        if payload_resolution.payload_format == "sqlite" and (
            payload_format_hint in {"sqlite_compact_v2", "compact_exact_payload_v2", "compact_sqlite_v2"}
            or payload_version_hint in {"2", "v2"}
        ):
            return CompactSQLiteAggregateCorpusProvider(self.bundle_dir, manifest, payload_resolution.payload_path)

        if payload_resolution.payload_format == "sqlite":
            return SQLiteAggregateCorpusProvider(self.bundle_dir, manifest, payload_resolution.payload_path)

        provider = JsonlAggregateCorpusProvider(self.bundle_dir, manifest, payload_resolution.payload_path, rng=self.rng)
        self.last_parse_issue = provider.last_parse_issue
        return provider

    def lookup_position(self, position_key: str) -> BuilderAggregatePosition | None:
        return self._provider.lookup_position(position_key)
