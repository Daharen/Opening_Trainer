from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ENV_PRACTICAL_RISK_RECONCILED_PATH = "OPENING_TRAINER_PRACTICAL_RISK_RECONCILED_PATH"
DEFAULT_RECONCILED_RELATIVE_PATH = Path("practical_risk") / "reconciled" / "default" / "practical_risk_reconciled.sqlite"


@dataclass(frozen=True)
class BandResolution:
    requested_band_id: str | None
    resolved_band_id: str | None
    provenance: str


@dataclass(frozen=True)
class ReconciledRuntimeStatus:
    active: bool
    path: str | None
    family_id: str | None
    artifact_time_control_id: str | None
    requested_band_id: str | None
    resolved_band_id: str | None
    mode_id: str | None
    sharp_toggle_enabled: bool
    detail: str


@dataclass
class PracticalRiskReconciledService:
    path: Path | None = None
    compatible_time_control_id: str | None = None
    active: bool = False
    family_id: str | None = None
    artifact_role: str | None = None
    artifact_time_control_id: str | None = None
    included_bands: tuple[str, ...] = field(default_factory=tuple)
    band_order: tuple[str, ...] = field(default_factory=tuple)
    validation_error: str | None = None

    @classmethod
    def from_runtime(cls, *, runtime_config: Any, runtime_paths: Any, time_control_id: str | None) -> "PracticalRiskReconciledService":
        explicit = getattr(runtime_config, "practical_risk_reconciled_path", None)
        if isinstance(explicit, str) and explicit.strip():
            selected = Path(explicit.strip())
        else:
            env_path = os.getenv(ENV_PRACTICAL_RISK_RECONCILED_PATH)
            if env_path and env_path.strip():
                selected = Path(env_path.strip())
            else:
                selected = Path(runtime_paths.content_root) / DEFAULT_RECONCILED_RELATIVE_PATH
        service = cls(path=selected, compatible_time_control_id=time_control_id)
        service._load()
        return service

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            self.validation_error = "reconciled artifact path is not configured or file is missing"
            self.active = False
            return
        try:
            conn = sqlite3.connect(self.path)
        except sqlite3.Error as exc:
            self.validation_error = f"sqlite open failed: {exc}"
            self.active = False
            return
        with conn:
            if not self._required_tables_present(conn):
                self.validation_error = "artifact missing one or more required tables"
                self.active = False
                return
            metadata = self._load_metadata(conn)
            self.artifact_role = str(metadata.get("artifact_role") or "").strip() or None
            self.family_id = str(metadata.get("artifact_family_id") or metadata.get("family_id") or "").strip() or None
            self.artifact_time_control_id = str(metadata.get("time_control_id") or "").strip() or None
            if self.artifact_role not in {"stage_d_reconciled", "practical_risk_reconciled"}:
                self.validation_error = f"unsupported artifact_role={self.artifact_role!r}"
                self.active = False
                return
            if self.compatible_time_control_id and self.artifact_time_control_id and self.compatible_time_control_id != self.artifact_time_control_id:
                self.validation_error = (
                    f"time control mismatch: trainer={self.compatible_time_control_id!r} artifact={self.artifact_time_control_id!r}"
                )
                self.active = False
                return
            included_bands = self._load_band_list(metadata, conn)
            self.included_bands = tuple(included_bands)
            self.band_order = tuple(included_bands)
            self.active = True
            self.validation_error = None

    def _required_tables_present(self, conn: sqlite3.Connection) -> bool:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        tables = {str(row[0]) for row in rows}
        required = {"artifact_metadata", "reconciled_move_admissions", "failure_explanations", "reconciled_root_summaries"}
        return required.issubset(tables)

    def _load_metadata(self, conn: sqlite3.Connection) -> dict[str, Any]:
        rows = conn.execute("SELECT key, value FROM artifact_metadata").fetchall()
        metadata: dict[str, Any] = {}
        for key, value in rows:
            key_text = str(key)
            if isinstance(value, str):
                trimmed = value.strip()
                if (trimmed.startswith("{") and trimmed.endswith("}")) or (trimmed.startswith("[") and trimmed.endswith("]")):
                    try:
                        metadata[key_text] = json.loads(trimmed)
                        continue
                    except json.JSONDecodeError:
                        pass
            metadata[key_text] = value
        return metadata

    def _load_band_list(self, metadata: dict[str, Any], conn: sqlite3.Connection) -> list[str]:
        for key in ("included_band_ids", "included_bands", "band_order"):
            value = metadata.get(key)
            if isinstance(value, list):
                return [str(item) for item in value if str(item).strip()]
            if isinstance(value, str) and value.strip():
                parts = [part.strip() for part in value.split(",") if part.strip()]
                if parts:
                    return parts
        rows = conn.execute("SELECT DISTINCT band_id FROM reconciled_move_admissions ORDER BY band_id ASC").fetchall()
        return [str(row[0]) for row in rows if str(row[0]).strip()]

    def resolve_band_id(self, requested_band_id: str | None) -> BandResolution:
        if not self.active:
            return BandResolution(requested_band_id, None, "reconciled_inactive")
        if not requested_band_id:
            if self.band_order:
                return BandResolution(None, self.band_order[0], "requested_band_missing_default_first_included")
            return BandResolution(None, None, "requested_band_missing_and_no_included_bands")
        if requested_band_id in self.band_order:
            return BandResolution(requested_band_id, requested_band_id, "exact_band_match")
        resolved = self._nearest_higher_then_lower(requested_band_id)
        if resolved is None:
            return BandResolution(requested_band_id, None, "requested_band_not_available_no_fallback")
        return BandResolution(requested_band_id, resolved, "nearest_stricter_higher_then_lower")

    def _nearest_higher_then_lower(self, requested_band_id: str) -> str | None:
        ranked = list(self.band_order)
        if not ranked:
            return None
        try:
            requested_numeric = int("".join(ch for ch in requested_band_id if ch.isdigit()))
            parsed = [(band, int("".join(ch for ch in band if ch.isdigit()))) for band in ranked]
        except ValueError:
            return ranked[0]
        higher = sorted((item for item in parsed if item[1] > requested_numeric), key=lambda item: item[1])
        if higher:
            return higher[0][0]
        lower = sorted((item for item in parsed if item[1] < requested_numeric), key=lambda item: abs(item[1] - requested_numeric))
        return lower[0][0] if lower else None

    def get_move_admission(self, position_key: str, band_id: str | None, move_uci: str) -> dict[str, Any] | None:
        if not self.active or band_id is None:
            return None
        conn = sqlite3.connect(self.path)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT * FROM reconciled_move_admissions
                WHERE position_key = ? AND band_id = ? AND move_uci = ?
                """,
                (position_key, band_id, move_uci),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_failure_explanation(self, position_key: str, band_id: str | None, move_uci: str, mode_id: str) -> dict[str, Any] | None:
        if not self.active or band_id is None:
            return None
        conn = sqlite3.connect(self.path)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT * FROM failure_explanations
                WHERE position_key = ? AND band_id = ? AND move_uci = ? AND mode_id = ?
                """,
                (position_key, band_id, move_uci, mode_id),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_root_summary(self, position_key: str, band_id: str | None) -> dict[str, Any] | None:
        if not self.active or band_id is None:
            return None
        conn = sqlite3.connect(self.path)
        try:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM reconciled_root_summaries WHERE position_key = ? AND band_id = ?",
                (position_key, band_id),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


def render_failure_explanation(explanation: dict[str, Any], *, requested_band_id: str | None, resolved_band_id: str | None) -> str:
    reason_code = str(explanation.get("reason_code") or "").strip()
    family_label = str(explanation.get("family_label") or "line").strip() or "line"
    max_practical_band = explanation.get("max_practical_band_id")
    first_failure_band = explanation.get("first_failure_band_id")
    if reason_code == "would_pass_if_sharp_toggle_enabled":
        text = f"This {family_label} is disabled in your current mode. Enable sharp/gambit lines to allow it up to {max_practical_band}."
    elif reason_code == "outgrown_above_band":
        text = f"This {family_label} is practical through {max_practical_band}, but is outgrown by {first_failure_band}."
    elif reason_code == "failed_below_threshold":
        text = "This move falls below the minimum accepted threshold for this band."
    elif reason_code == "strict_mode_rejects_good":
        text = "This move is only accepted when Good moves are enabled."
    elif reason_code == "no_threshold_available":
        text = "This move is not accepted here because no qualifying threshold was available for this position."
    else:
        preview = str(explanation.get("rendered_preview") or "").strip()
        text = preview or "This move is not accepted for this line in the current mode."
    if requested_band_id and resolved_band_id and requested_band_id != resolved_band_id:
        text = f"{text} (Current band: {requested_band_id}; resolved band: {resolved_band_id}.)"
    return text
