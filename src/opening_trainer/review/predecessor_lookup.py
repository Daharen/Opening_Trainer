from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import chess

from ..bundle_corpus import normalize_builder_position_key
from ..sqlite_mounts import MountedSQLiteLease, SQLitePayloadResolutionError, get_mounted_sqlite_manager


@dataclass(frozen=True)
class RouteLookupResult:
    success: bool
    normalized_position_key: str | None
    predecessor_line_uci: str | None
    ply_count: int
    failure_reason: str | None = None
    failure_detail: str | None = None


@dataclass(frozen=True)
class _ResolvedSchema:
    table_name: str
    position_key_col: str
    parent_position_key_col: str
    incoming_move_uci_col: str


class PredecessorMasterLookupService:
    def __init__(self, db_path: str | None, *, max_depth: int = 256):
        self.db_path = db_path
        self.max_depth = max_depth
        self._mount_lease: MountedSQLiteLease | None = None

    def find_predecessor_route_for_fen(self, fen: str) -> RouteLookupResult:
        try:
            board = chess.Board(fen)
        except ValueError as exc:
            return RouteLookupResult(
                success=False,
                normalized_position_key=None,
                predecessor_line_uci=None,
                ply_count=0,
                failure_reason="invalid_fen",
                failure_detail=str(exc),
            )
        if not board.is_valid():
            return RouteLookupResult(
                success=False,
                normalized_position_key=None,
                predecessor_line_uci=None,
                ply_count=0,
                failure_reason="invalid_fen",
                failure_detail="Target FEN does not describe a valid chess position.",
            )
        normalized_position_key = normalize_builder_position_key(board)
        if not self.db_path:
            return RouteLookupResult(
                success=False,
                normalized_position_key=normalized_position_key,
                predecessor_line_uci=None,
                ply_count=0,
                failure_reason="db_unavailable",
                failure_detail="No predecessor database path is configured.",
            )

        db_file = Path(self.db_path)
        try:
            resolution, lease = get_mounted_sqlite_manager().resolve(db_file)
        except SQLitePayloadResolutionError as exc:
            return RouteLookupResult(
                success=False,
                normalized_position_key=normalized_position_key,
                predecessor_line_uci=None,
                ply_count=0,
                failure_reason="db_unavailable",
                failure_detail=f"Unable to resolve predecessor database: {exc}",
            )
        self._mount_lease = lease

        try:
            connection = sqlite3.connect(f"file:{resolution.active_path}?mode=ro", uri=True)
        except sqlite3.Error as exc:
            return RouteLookupResult(
                success=False,
                normalized_position_key=normalized_position_key,
                predecessor_line_uci=None,
                ply_count=0,
                failure_reason="db_unavailable",
                failure_detail=f"Unable to open predecessor database: {exc}",
            )

        try:
            schema = self._resolve_schema(connection)
            if schema is None:
                return RouteLookupResult(
                    success=False,
                    normalized_position_key=normalized_position_key,
                    predecessor_line_uci=None,
                    ply_count=0,
                    failure_reason="schema_error",
                    failure_detail="No table with position_key, parent_position_key, and incoming_move_uci columns was found.",
                )
            return self._reconstruct_route(connection, schema, normalized_position_key)
        except sqlite3.Error as exc:
            return RouteLookupResult(
                success=False,
                normalized_position_key=normalized_position_key,
                predecessor_line_uci=None,
                ply_count=0,
                failure_reason="db_error",
                failure_detail=f"Database query failed: {exc}",
            )
        finally:
            connection.close()
            if self._mount_lease is not None:
                self._mount_lease.release()
                self._mount_lease = None

    def _resolve_schema(self, connection: sqlite3.Connection) -> _ResolvedSchema | None:
        table_rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        for (table_name,) in table_rows:
            if not isinstance(table_name, str) or table_name.startswith("sqlite_"):
                continue
            columns = [
                row[1]
                for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
                if len(row) > 1 and isinstance(row[1], str)
            ]
            normalized = {column.lower(): column for column in columns}
            position_key_col = normalized.get("position_key")
            parent_position_key_col = normalized.get("parent_position_key")
            incoming_move_uci_col = normalized.get("incoming_move_uci")
            if position_key_col and parent_position_key_col and incoming_move_uci_col:
                return _ResolvedSchema(
                    table_name=table_name,
                    position_key_col=position_key_col,
                    parent_position_key_col=parent_position_key_col,
                    incoming_move_uci_col=incoming_move_uci_col,
                )
        return None

    def _reconstruct_route(
        self,
        connection: sqlite3.Connection,
        schema: _ResolvedSchema,
        normalized_position_key: str,
    ) -> RouteLookupResult:
        cursor = connection.cursor()
        seen: set[str] = set()
        current_key = normalized_position_key
        reverse_moves: list[str] = []
        for depth in range(self.max_depth):
            row = cursor.execute(
                (
                    f"SELECT {schema.parent_position_key_col}, {schema.incoming_move_uci_col} "
                    f"FROM {schema.table_name} WHERE {schema.position_key_col} = ? LIMIT 1"
                ),
                (current_key,),
            ).fetchone()
            if row is None:
                if depth == 0:
                    return RouteLookupResult(
                        success=False,
                        normalized_position_key=normalized_position_key,
                        predecessor_line_uci=None,
                        ply_count=0,
                        failure_reason="target_not_found",
                        failure_detail="Target position was not found in the predecessor database.",
                    )
                return RouteLookupResult(
                    success=False,
                    normalized_position_key=normalized_position_key,
                    predecessor_line_uci=None,
                    ply_count=0,
                    failure_reason="chain_reconstruction_failed",
                    failure_detail=f"Missing parent row for position key: {current_key}",
                )

            parent_key = row[0]
            incoming_move = row[1]
            if incoming_move is not None:
                if not isinstance(incoming_move, str) or not incoming_move.strip():
                    return RouteLookupResult(
                        success=False,
                        normalized_position_key=normalized_position_key,
                        predecessor_line_uci=None,
                        ply_count=0,
                        failure_reason="chain_reconstruction_failed",
                        failure_detail=f"Missing incoming move for position key: {current_key}",
                    )
                try:
                    chess.Move.from_uci(incoming_move)
                except ValueError:
                    return RouteLookupResult(
                        success=False,
                        normalized_position_key=normalized_position_key,
                        predecessor_line_uci=None,
                        ply_count=0,
                        failure_reason="chain_reconstruction_failed",
                        failure_detail=f"Invalid incoming UCI move '{incoming_move}' for position key: {current_key}",
                    )
                reverse_moves.append(incoming_move)

            if parent_key in (None, ""):
                forward_moves = list(reversed(reverse_moves))
                return RouteLookupResult(
                    success=True,
                    normalized_position_key=normalized_position_key,
                    predecessor_line_uci=" ".join(forward_moves) if forward_moves else None,
                    ply_count=len(forward_moves),
                )
            if not isinstance(parent_key, str):
                return RouteLookupResult(
                    success=False,
                    normalized_position_key=normalized_position_key,
                    predecessor_line_uci=None,
                    ply_count=0,
                    failure_reason="chain_reconstruction_failed",
                    failure_detail=f"Invalid parent key type for position key: {current_key}",
                )
            if current_key in seen:
                return RouteLookupResult(
                    success=False,
                    normalized_position_key=normalized_position_key,
                    predecessor_line_uci=None,
                    ply_count=0,
                    failure_reason="chain_reconstruction_failed",
                    failure_detail=f"Cycle detected while resolving predecessor chain at: {current_key}",
                )
            seen.add(current_key)
            current_key = parent_key

        return RouteLookupResult(
            success=False,
            normalized_position_key=normalized_position_key,
            predecessor_line_uci=None,
            ply_count=0,
            failure_reason="chain_reconstruction_failed",
            failure_detail=f"Predecessor traversal exceeded max depth of {self.max_depth}.",
        )


def find_predecessor_route_for_fen(fen: str, *, predecessor_master_db_path: str | None, max_depth: int = 256) -> RouteLookupResult:
    service = PredecessorMasterLookupService(predecessor_master_db_path, max_depth=max_depth)
    return service.find_predecessor_route_for_fen(fen)
