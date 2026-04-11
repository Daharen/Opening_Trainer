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
    source: str | None = None
    candidate_roots: tuple[Path, ...] = ()


@dataclass
class OpeningLockedSessionState:
    enabled: bool = False
    requested: bool = False
    artifact_available: bool = False
    selected_opening_name: str | None = None
    lock_released_by_opponent: bool = False
    current_transition_state: OpeningLockedModeState = OpeningLockedModeState.OPENING_LOCKED
    ineffective_reason: str | None = None


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
                "SELECT DISTINCT opening_name FROM opening_membership WHERE COALESCE(is_exact, 1) = 1 ORDER BY opening_name"
            ).fetchall()
        return [str(row[0]) for row in rows if row and str(row[0]).strip()]

    def opening_names_for_position(self, position_key: str) -> tuple[str, ...]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT opening_name FROM opening_membership WHERE position_key = ?",
                (position_key,),
            ).fetchall()
        names = [str(row[0]) for row in rows if row and str(row[0]).strip()]
        return tuple(sorted(set(names)))

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
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT move_uci
                FROM canonical_continuation
                WHERE opening_name = ? AND position_key = ?
                ORDER BY COALESCE(ply_index, 0) ASC
                LIMIT ?
                """,
                (selected_opening_name, position_key, int(max_plies)),
            ).fetchall()
        line = tuple(str(row[0]) for row in rows if row and str(row[0]).strip())
        return CanonicalContinuation(next_move_uci=(line[0] if line else None), line=line)


def discover_opening_locked_artifact(
    content_root: Path,
    *,
    artifact_root: Path | None = None,
    candidate_roots: tuple[Path, ...] = (),
    source: str | None = None,
) -> OpeningLockedArtifactStatus:
    unique_candidates: list[Path] = []

    def _append_candidate(path: Path | None) -> None:
        if path is None:
            return
        normalized = path.expanduser()
        if normalized not in unique_candidates:
            unique_candidates.append(normalized)

    _append_candidate(artifact_root)
    for candidate in candidate_roots:
        _append_candidate(candidate)
    if not unique_candidates:
        _append_candidate(content_root / "opening_locked_mode")

    missing_by_candidate: list[str] = []
    for candidate_root in unique_candidates:
        manifest_path = candidate_root / "manifest.json"
        sqlite_path = candidate_root / "opening_locked_openings.sqlite"
        if not manifest_path.exists() or not sqlite_path.exists():
            missing: list[str] = []
            if not manifest_path.exists():
                missing.append(str(manifest_path))
            if not sqlite_path.exists():
                missing.append(str(sqlite_path))
            missing_by_candidate.append(f"{candidate_root} -> missing: {', '.join(missing)}")
            continue
        opening_count = 0
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                opening_count = int(payload.get("opening_count") or payload.get("exact_opening_count") or 0)
        except Exception:
            opening_count = 0
        resolved_source = source or "discovery"
        return OpeningLockedArtifactStatus(
            loaded=True,
            manifest_path=manifest_path,
            sqlite_path=sqlite_path,
            opening_count=max(0, opening_count),
            detail=f"opening-locked artifact loaded from {candidate_root} ({resolved_source})",
            source=resolved_source,
            candidate_roots=tuple(unique_candidates),
        )
    return OpeningLockedArtifactStatus(
        loaded=False,
        manifest_path=None,
        sqlite_path=None,
        opening_count=0,
        detail=f"opening-locked artifact unavailable (tried: {'; '.join(missing_by_candidate)})",
        source=source or "discovery",
        candidate_roots=tuple(unique_candidates),
    )
