from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class OpeningTransitionClassification(str, Enum):
    SELECTED_OPENING_PRESERVED = "selected_opening_preserved"
    LEFT_TO_OTHER_NAMED_OPENING = "left_to_other_named_opening"
    LEFT_TO_UNNAMED = "left_to_unnamed"
    UNKNOWN = "unknown"


class OpeningLockedModeState(str, Enum):
    OPENING_LOCKED = "OpeningLocked"
    RELEASED_BY_OPPONENT = "ReleasedByOpponent"
    COMPLETED_OR_RESOLVED = "CompletedOrResolved"


@dataclass(frozen=True)
class OpeningLockedArtifactStatus:
    loaded: bool
    manifest_path: Path | None
    sqlite_path: Path | None
    opening_count: int
    detail: str


@dataclass
class OpeningLockedSessionState:
    enabled: bool = False
    selected_opening_name: str | None = None
    lock_released_by_opponent: bool = False
    current_transition_state: OpeningLockedModeState = OpeningLockedModeState.OPENING_LOCKED


@dataclass(frozen=True)
class OpeningTransitionResult:
    classification: OpeningTransitionClassification
    successor_opening_names: tuple[str, ...]


@dataclass(frozen=True)
class CanonicalContinuation:
    next_move_uci: str | None
    line: tuple[str, ...]


class OpeningLockedProvider:
    def __init__(self, sqlite_path: Path):
        self.sqlite_path = sqlite_path

    def available(self) -> bool:
        return self.sqlite_path.exists()

    def _connect(self):
        return sqlite3.connect(self.sqlite_path)

    def list_exact_opening_names(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT node_name
                FROM opening_nodes
                WHERE node_kind = 'exact_opening'
                ORDER BY node_name ASC
                """
            ).fetchall()
        return [str(row[0]) for row in rows if row and str(row[0]).strip()]

    def opening_names_for_position(self, position_key: str) -> tuple[str, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT opening_nodes.node_name
                FROM positions
                JOIN path_memberships ON path_memberships.position_id = positions.position_id
                JOIN opening_nodes ON opening_nodes.node_id = path_memberships.node_id
                WHERE positions.position_key = ?
                ORDER BY opening_nodes.node_name ASC
                """,
                (position_key,),
            ).fetchall()
        names = [str(row[0]) for row in rows if row and str(row[0]).strip()]
        return tuple(names)

    def position_preserves_selected_opening(self, position_key: str, selected_opening_name: str) -> bool:
        names = self.opening_names_for_position(position_key)
        return selected_opening_name in names

    def classify_transition(self, successor_position_key: str, selected_opening_name: str) -> OpeningTransitionResult:
        names = self.opening_names_for_position(successor_position_key)
        if not names:
            return OpeningTransitionResult(OpeningTransitionClassification.LEFT_TO_UNNAMED, names)
        if selected_opening_name in names:
            return OpeningTransitionResult(OpeningTransitionClassification.SELECTED_OPENING_PRESERVED, names)
        return OpeningTransitionResult(OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING, names)

    def canonical_continuation(
        self,
        *,
        position_key: str,
        selected_opening_name: str,
        max_plies: int = 8,
    ) -> CanonicalContinuation:
        if max_plies <= 0:
            return CanonicalContinuation(next_move_uci=None, line=())
        line: list[str] = []
        with self._connect() as conn:
            row = conn.execute(
                "SELECT node_id FROM opening_nodes WHERE node_name = ? AND node_kind = 'exact_opening' LIMIT 1",
                (selected_opening_name,),
            ).fetchone()
            if not row:
                return CanonicalContinuation(next_move_uci=None, line=())
            node_id = int(row[0])
            row = conn.execute(
                "SELECT position_id FROM positions WHERE position_key = ? LIMIT 1",
                (position_key,),
            ).fetchone()
            if not row:
                return CanonicalContinuation(next_move_uci=None, line=())
            current_position_id = int(row[0])
            for _ in range(int(max_plies)):
                move_row = conn.execute(
                    """
                    SELECT move_uci, to_position_id
                    FROM node_moves
                    WHERE node_id = ?
                      AND from_position_id = ?
                      AND COALESCE(is_canonical, 0) = 1
                    ORDER BY COALESCE(support_count, 0) DESC, move_uci ASC
                    LIMIT 1
                    """,
                    (node_id, current_position_id),
                ).fetchone()
                if not move_row:
                    break
                move_uci = str(move_row[0]).strip() if move_row[0] is not None else ""
                if not move_uci:
                    break
                line.append(move_uci)
                if move_row[1] is None:
                    break
                current_position_id = int(move_row[1])
        line_tuple = tuple(line)
        return CanonicalContinuation(next_move_uci=(line_tuple[0] if line_tuple else None), line=line_tuple)


def discover_opening_locked_artifact(content_root: Path, *, artifact_root_override: Path | None = None) -> OpeningLockedArtifactStatus:
    artifact_root = artifact_root_override if artifact_root_override is not None else (content_root / "opening_locked_mode")
    manifest_path = artifact_root / "manifest.json"
    sqlite_path = artifact_root / "opening_locked_openings.sqlite"
    if not manifest_path.exists() or not sqlite_path.exists():
        missing: list[str] = []
        if not manifest_path.exists():
            missing.append(str(manifest_path))
        if not sqlite_path.exists():
            missing.append(str(sqlite_path))
        return OpeningLockedArtifactStatus(
            loaded=False,
            manifest_path=manifest_path,
            sqlite_path=sqlite_path,
            opening_count=0,
            detail=f"opening-locked artifact unavailable (missing: {', '.join(missing)})",
        )
    opening_count = 0
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            opening_count = int(payload.get("opening_count") or payload.get("exact_opening_count") or 0)
    except Exception:
        opening_count = 0
    return OpeningLockedArtifactStatus(
        loaded=True,
        manifest_path=manifest_path,
        sqlite_path=sqlite_path,
        opening_count=max(0, opening_count),
        detail=f"opening-locked artifact loaded from {artifact_root}",
    )
