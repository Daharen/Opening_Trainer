from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


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
    selected_family_name: str | None = None
    selected_variation_name: str | None = None
    effective_opening_lock_node: str | None = None
    allowed_opening_space: tuple[str, ...] = ()
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

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
            (table_name,),
        ).fetchone()
        return bool(row)

    def _table_columns(self, conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row[1]) for row in rows if row and row[1]}

    def supports_family_ui(self) -> bool:
        with self._connect() as conn:
            required = {"ui_tree", "family_memberships", "transposition_edges"}
            supported = all(self._table_exists(conn, table_name) for table_name in required)
        logger.debug("Opening locked supports_family_ui=%s sqlite=%s", supported, self.sqlite_path)
        return supported

    def supports_family_aware(self) -> bool:
        return self.supports_family_ui()

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

    def _node_name_for_id(self, conn: sqlite3.Connection, node_id: int | None) -> str | None:
        if node_id is None:
            return None
        row = conn.execute("SELECT node_name FROM opening_nodes WHERE node_id = ? LIMIT 1", (int(node_id),)).fetchone()
        if not row:
            return None
        value = str(row[0]).strip() if row[0] is not None else ""
        return value or None

    def _ui_tree_edges(self, conn: sqlite3.Connection) -> list[tuple[str, str]]:
        columns = self._table_columns(conn, "ui_tree")
        parent_name_col = next((name for name in ("ui_parent_node_name", "parent_node_name", "parent_name", "ancestor_node_name", "ancestor_name", "parent") if name in columns), None)
        child_name_col = next((name for name in ("child_node_name", "child_name", "descendant_node_name", "descendant_name", "child") if name in columns), None)
        parent_id_col = next((name for name in ("ui_parent_node_id", "parent_node_id", "ancestor_node_id", "parent_id") if name in columns), None)
        child_id_col = next((name for name in ("child_node_id", "descendant_node_id", "child_id") if name in columns), None)
        if parent_name_col and child_name_col:
            rows = conn.execute(f"SELECT {parent_name_col}, {child_name_col} FROM ui_tree").fetchall()
            return [
                (str(row[0]).strip(), str(row[1]).strip())
                for row in rows
                if row and row[0] is not None and row[1] is not None and str(row[0]).strip() and str(row[1]).strip()
            ]
        if parent_id_col and child_id_col:
            rows = conn.execute(f"SELECT {parent_id_col}, {child_id_col} FROM ui_tree").fetchall()
            edges: list[tuple[str, str]] = []
            for row in rows:
                parent_name = self._node_name_for_id(conn, int(row[0])) if row and row[0] is not None else None
                child_name = self._node_name_for_id(conn, int(row[1])) if row and row[1] is not None else None
                if parent_name and child_name:
                    edges.append((parent_name, child_name))
            return edges
        logger.debug("Opening locked ui_tree schema unsupported columns=%s sqlite=%s", sorted(columns), self.sqlite_path)
        return []

    def list_family_root_names(self) -> list[str]:
        if not self.supports_family_ui():
            return self.list_exact_opening_names()
        with self._connect() as conn:
            edges = self._ui_tree_edges(conn)
        logger.debug("Opening locked ui_tree edge_count=%d sqlite=%s", len(edges), self.sqlite_path)
        parents = {parent for parent, _child in edges}
        children = {child for _parent, child in edges}
        roots = sorted((parents - children), key=lambda value: value.lower())
        logger.debug("Opening locked family_root_count=%d sqlite=%s", len(roots), self.sqlite_path)
        return roots

    def list_root_openings(self) -> list[str]:
        return self.list_family_root_names()

    def list_variation_names_for_family(self, family_name: str) -> list[str]:
        return self.list_descendant_openings(family_name)

    def list_descendant_openings(self, root_name: str) -> list[str]:
        root = str(root_name or "").strip()
        if not root:
            return []
        if not self.supports_family_ui():
            return []
        with self._connect() as conn:
            edges = self._ui_tree_edges(conn)
        children_by_parent: dict[str, set[str]] = {}
        for parent, child in edges:
            children_by_parent.setdefault(parent, set()).add(child)
        pending = list(children_by_parent.get(root, set()))
        visited: set[str] = set()
        descendants: set[str] = set()
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            descendants.add(current)
            pending.extend(children_by_parent.get(current, set()))
        results = sorted(descendants, key=lambda value: value.lower())
        logger.debug(
            "Opening locked selected_family=%s descendant_count=%d sqlite=%s",
            root,
            len(results),
            self.sqlite_path,
        )
        return results

    def resolve_effective_selected_opening(self, family_name: str | None, variation_name: str | None) -> str | None:
        variation = str(variation_name or "").strip()
        if variation:
            return variation
        family = str(family_name or "").strip()
        return family or None

    def resolve_effective_opening_node(self, family_name: str | None, variation_name: str | None) -> str | None:
        return self.resolve_effective_selected_opening(family_name, variation_name)

    def resolve_allowed_opening_space(self, effective_node: str | None) -> tuple[str, ...]:
        node_name = str(effective_node or "").strip()
        if not node_name:
            return ()
        if not self.supports_family_aware():
            return (node_name,)
        with self._connect() as conn:
            columns = self._table_columns(conn, "family_memberships")
            family_name_col = next((name for name in ("family_node_name", "family_name", "ancestor_node_name", "node_name") if name in columns), None)
            member_name_col = next((name for name in ("member_node_name", "member_name", "descendant_node_name", "exact_opening_name") if name in columns), None)
            family_id_col = next((name for name in ("family_node_id", "ancestor_node_id", "node_id") if name in columns), None)
            member_id_col = next((name for name in ("member_node_id", "descendant_node_id", "exact_opening_node_id") if name in columns), None)
            members: set[str] = set()
            if family_name_col and member_name_col:
                rows = conn.execute(
                    f"SELECT {member_name_col} FROM family_memberships WHERE {family_name_col} = ?",
                    (node_name,),
                ).fetchall()
                members = {str(row[0]).strip() for row in rows if row and row[0] is not None and str(row[0]).strip()}
            elif family_id_col and member_id_col:
                row = conn.execute("SELECT node_id FROM opening_nodes WHERE node_name = ? LIMIT 1", (node_name,)).fetchone()
                if row:
                    family_id = int(row[0])
                    rows = conn.execute(
                        f"SELECT {member_id_col} FROM family_memberships WHERE {family_id_col} = ?",
                        (family_id,),
                    ).fetchall()
                    for member_row in rows:
                        member_name = self._node_name_for_id(conn, int(member_row[0])) if member_row and member_row[0] is not None else None
                        if member_name:
                            members.add(member_name)
            if members:
                return tuple(sorted(members, key=lambda value: value.lower()))
        descendants = self.list_descendant_openings(node_name)
        if not descendants:
            return (node_name,)
        descendants_set = set(descendants) | {node_name}
        return tuple(sorted(descendants_set, key=lambda value: value.lower()))

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

    def classify_transition(
        self,
        successor_position_key: str,
        selected_opening_name: str,
        *,
        allowed_opening_space: set[str] | None = None,
    ) -> OpeningTransitionResult:
        names = self.opening_names_for_position(successor_position_key)
        if not names:
            return OpeningTransitionResult(OpeningTransitionClassification.LEFT_TO_UNNAMED, names)
        if allowed_opening_space:
            if any(name in allowed_opening_space for name in names):
                return OpeningTransitionResult(OpeningTransitionClassification.SELECTED_OPENING_PRESERVED, names)
            return OpeningTransitionResult(OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING, names)
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
