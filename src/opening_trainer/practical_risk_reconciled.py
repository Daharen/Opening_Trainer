from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EXPECTED_ARTIFACT_ROLE = "practical_risk_reconciled"
EXPECTED_RECONCILED_MOVE_ADMISSIONS_COLUMNS = {
    "position_key",
    "band_id",
    "move_uci",
    "local_admitted_if_good_accepted",
    "local_admitted_if_good_rejected",
    "reconciled_admitted_if_good_accepted",
    "reconciled_admitted_if_good_rejected",
}
EXPECTED_FAILURE_EXPLANATIONS_COLUMNS = {
    "position_key",
    "band_id",
    "move_uci",
    "mode_id",
    "reason_code",
    "template_id",
    "family_label",
    "max_practical_band_id",
    "first_failure_band_id",
    "toggle_state_required",
    "rendered_preview",
}
OPTIONAL_RECONCILED_MOVE_ADMISSIONS_COLUMNS = (
    "local_admission_origin_if_good_accepted",
    "local_admission_origin_if_good_rejected",
    "reconciled_admission_origin_if_good_accepted",
    "reconciled_admission_origin_if_good_rejected",
    "admission_origin",
    "engine_quality_class",
    "local_reason",
    "local_reason_code",
    "practical_ceiling_band_id",
)


@dataclass(frozen=True)
class ReconciledBandResolution:
    requested_band_id: str | None
    resolved_band_id: str | None
    provenance: str


class PracticalRiskReconciledService:
    def __init__(
        self,
        artifact_path: str | Path | None,
        *,
        expected_time_control_id: str | None,
    ):
        self.path = Path(artifact_path).expanduser() if artifact_path else None
        self.expected_time_control_id = str(expected_time_control_id).strip() if expected_time_control_id else None
        self.active = False
        self.activation_error: str | None = None
        self.family_id: str | None = None
        self.time_control_id: str | None = None
        self.artifact_role: str | None = None
        self.band_order: tuple[str, ...] = ()
        self._admissions: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._failure_explanations: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self._root_summaries: dict[tuple[str, str], dict[str, Any]] = {}
        self._admissions_columns: set[str] = set()
        self._connect_and_load()

    def _connect_and_load(self) -> None:
        if self.path is None:
            self.activation_error = "no artifact path configured"
            return
        if not self.path.exists():
            self.activation_error = f"artifact path missing: {self.path}"
            return
        try:
            conn = sqlite3.connect(str(self.path))
        except sqlite3.Error as exc:
            self.activation_error = f"unable to open sqlite artifact: {exc}"
            return
        try:
            required = {
                "artifact_metadata",
                "reconciled_move_admissions",
                "failure_explanations",
                "reconciled_root_summaries",
            }
            tables = {
                row[0]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            missing = sorted(required - tables)
            if missing:
                self.activation_error = f"missing required tables: {', '.join(missing)}"
                return
            admissions_columns = _table_columns(conn, "reconciled_move_admissions")
            missing_admissions = sorted(EXPECTED_RECONCILED_MOVE_ADMISSIONS_COLUMNS - admissions_columns)
            if missing_admissions:
                self.activation_error = (
                    "schema contract mismatch for reconciled_move_admissions: missing columns: "
                    f"{', '.join(missing_admissions)}"
                )
                return
            self._admissions_columns = admissions_columns
            failure_columns = _table_columns(conn, "failure_explanations")
            missing_failure = sorted(EXPECTED_FAILURE_EXPLANATIONS_COLUMNS - failure_columns)
            if missing_failure:
                self.activation_error = (
                    "schema contract mismatch for failure_explanations: missing columns: "
                    f"{', '.join(missing_failure)}"
                )
                return
            self._load_metadata(conn)
            if self.artifact_role and self.artifact_role != EXPECTED_ARTIFACT_ROLE:
                self.activation_error = f"artifact_role mismatch: expected={EXPECTED_ARTIFACT_ROLE} got={self.artifact_role}"
                return
            if self.expected_time_control_id and self.time_control_id and self.time_control_id != self.expected_time_control_id:
                self.activation_error = (
                    f"time_control_id mismatch: expected={self.expected_time_control_id} got={self.time_control_id}"
                )
                return
            self._load_admissions(conn)
            self._load_failure_explanations(conn)
            self._load_root_summaries(conn)
            self.active = True
        except sqlite3.Error as exc:
            self.activation_error = f"sqlite read error: {exc}"
        finally:
            conn.close()

    def _load_metadata(self, conn: sqlite3.Connection) -> None:
        rows = list(conn.execute("SELECT key, value FROM artifact_metadata"))
        metadata = {str(key): value for key, value in rows}
        self.artifact_role = _as_text(metadata.get("artifact_role"))
        self.time_control_id = _as_text(metadata.get("time_control_id"))
        self.family_id = _as_text(metadata.get("artifact_family_id") or metadata.get("family_id"))
        parsed_order = _parse_band_order(metadata.get("included_band_order") or metadata.get("included_band_ids"))
        if parsed_order:
            self.band_order = tuple(parsed_order)

    def _load_admissions(self, conn: sqlite3.Connection) -> None:
        selected_columns = [
            "position_key",
            "band_id",
            "move_uci",
            "local_admitted_if_good_accepted",
            "local_admitted_if_good_rejected",
            "reconciled_admitted_if_good_accepted",
            "reconciled_admitted_if_good_rejected",
        ]
        selected_columns.extend(
            [column for column in OPTIONAL_RECONCILED_MOVE_ADMISSIONS_COLUMNS if column in self._admissions_columns]
        )
        cursor = conn.execute(
            """
            SELECT {columns}
            FROM reconciled_move_admissions
            """.format(columns=", ".join(selected_columns))
        )
        for row in cursor:
            row_data = dict(zip(selected_columns, row))
            position_key, band_id, move_uci = _as_text(row[0]), _as_text(row[1]), _as_text(row[2])
            if not position_key or not band_id or not move_uci:
                continue
            local_admitted_if_good_accepted = bool(row_data.get("local_admitted_if_good_accepted"))
            local_admitted_if_good_rejected = bool(row_data.get("local_admitted_if_good_rejected"))
            reconciled_admitted_if_good_accepted = bool(row_data.get("reconciled_admitted_if_good_accepted"))
            reconciled_admitted_if_good_rejected = bool(row_data.get("reconciled_admitted_if_good_rejected"))
            self._admissions[(position_key, band_id, move_uci)] = {
                "position_key": position_key,
                "band_id": band_id,
                "move_uci": move_uci,
                "local_admitted_if_good_accepted": local_admitted_if_good_accepted,
                "local_admitted_if_good_rejected": local_admitted_if_good_rejected,
                "reconciled_admitted_if_good_accepted": reconciled_admitted_if_good_accepted,
                "reconciled_admitted_if_good_rejected": reconciled_admitted_if_good_rejected,
                "admitted_good_inclusive": reconciled_admitted_if_good_accepted,
                "admitted_good_exclusive": reconciled_admitted_if_good_rejected,
                "local_admission_origin_if_good_accepted": _as_text(row_data.get("local_admission_origin_if_good_accepted")),
                "local_admission_origin_if_good_rejected": _as_text(row_data.get("local_admission_origin_if_good_rejected")),
                "reconciled_admission_origin_if_good_accepted": _as_text(
                    row_data.get("reconciled_admission_origin_if_good_accepted")
                ),
                "reconciled_admission_origin_if_good_rejected": _as_text(
                    row_data.get("reconciled_admission_origin_if_good_rejected")
                ),
                "admission_origin": _as_text(row_data.get("admission_origin")),
                "engine_quality_class": _as_text(row_data.get("engine_quality_class")),
                "local_reason": _as_text(row_data.get("local_reason")),
                "local_reason_code": _as_text(row_data.get("local_reason_code")),
                "practical_ceiling_band_id": _as_text(row_data.get("practical_ceiling_band_id")),
            }
            if band_id not in self.band_order:
                self.band_order = (*self.band_order, band_id)

    def _load_failure_explanations(self, conn: sqlite3.Connection) -> None:
        cursor = conn.execute(
            """
            SELECT position_key, band_id, move_uci, mode_id,
                   reason_code, template_id, family_label,
                   max_practical_band_id, first_failure_band_id,
                   toggle_state_required, rendered_preview
            FROM failure_explanations
            """
        )
        for row in cursor:
            key = tuple(_as_text(value) for value in row[:4])
            if not all(key):
                continue
            self._failure_explanations[key] = {
                "position_key": key[0],
                "band_id": key[1],
                "move_uci": key[2],
                "mode_id": key[3],
                "reason_code": _as_text(row[4]),
                "template_id": _as_text(row[5]),
                "family_label": _as_text(row[6]),
                "max_practical_band_id": _as_text(row[7]),
                "first_failure_band_id": _as_text(row[8]),
                "toggle_state_required": _as_text(row[9]),
                "rendered_preview": _as_text(row[10]),
            }

    def _load_root_summaries(self, conn: sqlite3.Connection) -> None:
        cursor = conn.execute(
            """
            SELECT position_key, band_id, summary_json
            FROM reconciled_root_summaries
            """
        )
        for row in cursor:
            position_key, band_id = _as_text(row[0]), _as_text(row[1])
            if not position_key or not band_id:
                continue
            summary_payload = row[2]
            parsed = summary_payload
            if isinstance(summary_payload, str):
                try:
                    parsed = json.loads(summary_payload)
                except json.JSONDecodeError:
                    parsed = summary_payload
            self._root_summaries[(position_key, band_id)] = {
                "position_key": position_key,
                "band_id": band_id,
                "summary": parsed,
            }

    def resolve_band_id(self, requested_band_id: str | None) -> ReconciledBandResolution:
        if not requested_band_id:
            return ReconciledBandResolution(requested_band_id=None, resolved_band_id=None, provenance="requested_band_missing")
        requested = str(requested_band_id)
        if requested in self.band_order:
            return ReconciledBandResolution(requested, requested, "exact")
        if not self.band_order:
            return ReconciledBandResolution(requested, None, "artifact_band_list_missing")
        order = list(self.band_order)
        try:
            requested_value = int(requested.split("-")[0])
            order_values = [int(item.split("-")[0]) for item in order]
            higher = [item for item, value in zip(order, order_values) if value > requested_value]
            if higher:
                return ReconciledBandResolution(requested, min(higher, key=lambda item: int(item.split("-")[0])), "fallback_nearest_higher")
            lower = [item for item, value in zip(order, order_values) if value < requested_value]
            if lower:
                return ReconciledBandResolution(requested, max(lower, key=lambda item: int(item.split("-")[0])), "fallback_nearest_lower")
        except ValueError:
            pass
        return ReconciledBandResolution(requested, order[0], "fallback_first_available")

    def get_move_admission(self, position_key: str, band_id: str, move_uci: str) -> dict[str, Any] | None:
        return self._admissions.get((position_key, band_id, move_uci))

    def get_failure_explanation(self, position_key: str, band_id: str, move_uci: str, mode_id: str) -> dict[str, Any] | None:
        return self._failure_explanations.get((position_key, band_id, move_uci, mode_id))

    def get_root_summary(self, position_key: str, band_id: str) -> dict[str, Any] | None:
        return self._root_summaries.get((position_key, band_id))


class ReconciledFailureRenderer:
    @staticmethod
    def render(explanation: dict[str, Any], *, requested_band_id: str | None, resolved_band_id: str | None) -> str:
        reason_code = explanation.get("reason_code")
        family_label = explanation.get("family_label") or "line"
        max_practical = explanation.get("max_practical_band_id")
        first_failure = explanation.get("first_failure_band_id")
        if reason_code == "would_pass_if_sharp_toggle_enabled":
            text = f"This {family_label} is disabled in your current mode. Enable sharp/gambit lines to allow it up to {max_practical or 'the practical ceiling'}."
        elif reason_code == "outgrown_above_band":
            text = f"This {family_label} is practical through {max_practical or 'an earlier band'}, but is outgrown by {first_failure or 'a stronger band'}."
        elif reason_code == "failed_below_threshold":
            text = "This move falls below the minimum accepted threshold for this band."
        elif reason_code == "strict_mode_rejects_good":
            text = "This move is only accepted when Good moves are enabled."
        elif reason_code == "no_threshold_available":
            text = "This move is not accepted here because no qualifying threshold was available for this position."
        else:
            text = explanation.get("rendered_preview") or "This move is not accepted for the current training policy."
        if requested_band_id and resolved_band_id and requested_band_id != resolved_band_id:
            text += f" (requested band {requested_band_id}, using {resolved_band_id})"
        return text


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_band_order(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                return [str(item).strip() for item in parsed if str(item).strip()]
            except json.JSONDecodeError:
                return []
        return [item.strip() for item in stripped.split(",") if item.strip()]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1]).strip()
        for row in conn.execute(f"PRAGMA table_info({table_name})")
        if len(row) > 1 and str(row[1]).strip()
    }
