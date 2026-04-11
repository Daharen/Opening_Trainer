from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


EXPECTED_ARTIFACT_ROLE = "practical_risk_reconciled"

_REQUIRED_ADMISSION_COLUMNS = {
    "position_key",
    "band_id",
    "move_uci",
    "local_admitted_if_good_accepted",
    "local_admitted_if_good_rejected",
    "reconciled_admitted_if_good_accepted",
    "reconciled_admitted_if_good_rejected",
}
_REQUIRED_FAILURE_EXPLANATION_COLUMNS = {
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
_REQUIRED_ROOT_SUMMARY_COLUMNS = {
    "position_key",
    "band_id",
    "local_admitted_if_good_accepted_count",
    "local_admitted_if_good_rejected_count",
    "reconciled_admitted_if_good_accepted_count",
    "reconciled_admitted_if_good_rejected_count",
}
_ROOT_SUMMARY_COUNT_COLUMNS = (
    "local_admitted_if_good_accepted_count",
    "local_admitted_if_good_rejected_count",
    "reconciled_admitted_if_good_accepted_count",
    "reconciled_admitted_if_good_rejected_count",
)
_ROOT_SUMMARY_REQUIRED_ID_COLUMNS = {"position_key", "band_id"}


@dataclass(frozen=True)
class ReconciledBandResolution:
    requested_band_id: str | None
    resolved_band_id: str | None
    provenance: str


@dataclass(frozen=True)
class MoveFamilyPolicy:
    family_label: str | None
    is_sharp_gambit_family: bool | None
    toggle_state_required: str | None
    source: str
    supporting_reason_codes: tuple[str, ...] = ()


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
        self._failure_explanations_by_move: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._root_summaries: dict[tuple[str, str], dict[str, Any]] = {}
        self.root_summary_status: str = "not_loaded"
        self.root_summary_activation_warning: str | None = None
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
        conn.row_factory = sqlite3.Row
        try:
            required = {
                "artifact_metadata",
                "reconciled_move_admissions",
                "failure_explanations",
            }
            tables = {
                row["name"]
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            missing = sorted(required - tables)
            if missing:
                self.activation_error = f"missing required tables: {', '.join(missing)}"
                return
            self._validate_schema(conn, tables)
            if self.activation_error:
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
            try:
                self._load_root_summaries(conn, tables)
            except sqlite3.Error as exc:
                self.root_summary_status = "load_error_ignored"
                self.root_summary_activation_warning = f"optional root summary load failed: {exc}"
            self.active = True
        except sqlite3.Error as exc:
            self.activation_error = f"sqlite read error: {exc}"
        finally:
            conn.close()

    def _validate_schema(self, conn: sqlite3.Connection, tables: set[str]) -> None:
        admission_columns = self._table_columns(conn, "reconciled_move_admissions")
        missing_admission = sorted(_REQUIRED_ADMISSION_COLUMNS - admission_columns)
        if missing_admission:
            self.activation_error = (
                "artifact schema mismatch in reconciled_move_admissions: "
                f"missing columns: {', '.join(missing_admission)}"
            )
            return

        failure_columns = self._table_columns(conn, "failure_explanations")
        missing_failure = sorted(_REQUIRED_FAILURE_EXPLANATION_COLUMNS - failure_columns)
        if missing_failure:
            self.activation_error = (
                "artifact schema mismatch in failure_explanations: "
                f"missing columns: {', '.join(missing_failure)}"
            )
            return

        if "reconciled_root_summaries" not in tables:
            self.root_summary_status = "missing_optional_table"
            self.root_summary_activation_warning = "optional table reconciled_root_summaries is missing"
            return

        root_summary_columns = self._table_columns(conn, "reconciled_root_summaries")
        missing_root_summary = sorted(
            (_ROOT_SUMMARY_REQUIRED_ID_COLUMNS | set(_ROOT_SUMMARY_COUNT_COLUMNS)) - root_summary_columns
        )
        if missing_root_summary:
            self.root_summary_status = "optional_schema_mismatch_ignored"
            self.root_summary_activation_warning = (
                "optional root summary schema mismatch ignored; missing columns: "
                f"{', '.join(missing_root_summary)}"
            )
            return

        self.root_summary_status = "available_schema_valid"

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
        return {str(row["name"]).strip() for row in conn.execute(f"PRAGMA table_info({table_name})")}

    def _load_metadata(self, conn: sqlite3.Connection) -> None:
        rows = list(conn.execute("SELECT key, value FROM artifact_metadata"))
        metadata = {str(row["key"]): row["value"] for row in rows}
        self.artifact_role = _as_text(metadata.get("artifact_role"))
        self.time_control_id = _as_text(metadata.get("time_control_id"))
        self.family_id = _as_text(metadata.get("artifact_family_id") or metadata.get("family_id"))
        parsed_order = _parse_band_order(metadata.get("included_band_order") or metadata.get("included_band_ids"))
        if parsed_order:
            self.band_order = tuple(parsed_order)

    def _load_admissions(self, conn: sqlite3.Connection) -> None:
        columns = self._table_columns(conn, "reconciled_move_admissions")
        optional_columns = [
            "local_admission_origin_if_good_accepted",
            "local_admission_origin_if_good_rejected",
            "reconciled_admission_origin_if_good_accepted",
            "reconciled_admission_origin_if_good_rejected",
            "engine_quality_class",
            "local_reason",
            "practical_ceiling_band_id",
            "max_engine_loss_cp",
            "family_label",
            "failure_reason_code",
        ]
        selected_columns = [
            "position_key",
            "band_id",
            "move_uci",
            "local_admitted_if_good_accepted",
            "local_admitted_if_good_rejected",
            "reconciled_admitted_if_good_accepted",
            "reconciled_admitted_if_good_rejected",
            *[column for column in optional_columns if column in columns],
        ]
        cursor = conn.execute(f"SELECT {', '.join(selected_columns)} FROM reconciled_move_admissions")
        for row in cursor:
            position_key, band_id, move_uci = _as_text(row["position_key"]), _as_text(row["band_id"]), _as_text(row["move_uci"])
            if not position_key or not band_id or not move_uci:
                continue
            admission = {
                "position_key": position_key,
                "band_id": band_id,
                "move_uci": move_uci,
                "local_admitted_if_good_accepted": bool(row["local_admitted_if_good_accepted"]),
                "local_admitted_if_good_rejected": bool(row["local_admitted_if_good_rejected"]),
                "reconciled_admitted_if_good_accepted": bool(row["reconciled_admitted_if_good_accepted"]),
                "reconciled_admitted_if_good_rejected": bool(row["reconciled_admitted_if_good_rejected"]),
                "admitted_good_inclusive": bool(row["reconciled_admitted_if_good_accepted"]),
                "admitted_good_exclusive": bool(row["reconciled_admitted_if_good_rejected"]),
            }
            for column in optional_columns:
                if column in selected_columns:
                    admission[column] = _as_text(row[column])
            admission["admission_origin"] = _as_text(
                admission.get("reconciled_admission_origin_if_good_accepted")
                or admission.get("reconciled_admission_origin_if_good_rejected")
                or admission.get("local_admission_origin_if_good_accepted")
                or admission.get("local_admission_origin_if_good_rejected")
            )
            self._admissions[(position_key, band_id, move_uci)] = admission
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
            key = tuple(_as_text(row[column]) for column in ("position_key", "band_id", "move_uci", "mode_id"))
            if not all(key):
                continue
            self._failure_explanations[key] = {
                "position_key": key[0],
                "band_id": key[1],
                "move_uci": key[2],
                "mode_id": key[3],
                "reason_code": _as_text(row["reason_code"]),
                "template_id": _as_text(row["template_id"]),
                "family_label": _as_text(row["family_label"]),
                "max_practical_band_id": _as_text(row["max_practical_band_id"]),
                "first_failure_band_id": _as_text(row["first_failure_band_id"]),
                "toggle_state_required": _as_text(row["toggle_state_required"]),
                "rendered_preview": _as_text(row["rendered_preview"]),
            }
            move_key = (key[0], key[2])
            self._failure_explanations_by_move.setdefault(move_key, []).append(self._failure_explanations[key])

    def _load_root_summaries(self, conn: sqlite3.Connection, tables: set[str]) -> None:
        if "reconciled_root_summaries" not in tables:
            return
        columns = self._table_columns(conn, "reconciled_root_summaries")
        missing_for_load = (_ROOT_SUMMARY_REQUIRED_ID_COLUMNS | set(_ROOT_SUMMARY_COUNT_COLUMNS)) - columns
        if missing_for_load:
            if self.root_summary_status == "not_loaded":
                self.root_summary_status = "skipped_missing_optional_columns"
                self.root_summary_activation_warning = (
                    "optional root summaries skipped; missing columns: "
                    f"{', '.join(sorted(missing_for_load))}"
                )
            return
        optional_columns = sorted(column for column in columns if column not in _REQUIRED_ROOT_SUMMARY_COLUMNS)
        selected_columns = [
            "position_key",
            "band_id",
            *_ROOT_SUMMARY_COUNT_COLUMNS,
            *optional_columns,
        ]
        cursor = conn.execute(
            """
            SELECT {columns}
            FROM reconciled_root_summaries
            """
            .format(columns=", ".join(selected_columns))
        )
        for row in cursor:
            position_key, band_id = _as_text(row["position_key"]), _as_text(row["band_id"])
            if not position_key or not band_id:
                continue
            summary_counts = {column: int(row[column] or 0) for column in _ROOT_SUMMARY_COUNT_COLUMNS}
            optional_summary_fields = {
                column: row[column]
                for column in optional_columns
            }
            self._root_summaries[(position_key, band_id)] = {
                "position_key": position_key,
                "band_id": band_id,
                **summary_counts,
                **optional_summary_fields,
            }
        self.root_summary_status = "loaded"

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

    def get_move_family_policy(
        self,
        *,
        position_key: str,
        move_uci: str,
        band_id: str,
        mode_id: str,
        admission: dict[str, Any] | None = None,
        current_band_explanation: dict[str, Any] | None = None,
    ) -> MoveFamilyPolicy:
        local_admission = admission or self.get_move_admission(position_key, band_id, move_uci)
        if local_admission:
            sharp_local = self.admission_is_sharp_gambit_family(local_admission)
            if sharp_local:
                return MoveFamilyPolicy(
                    family_label=_as_text(local_admission.get("family_label")) or "sharp/gambit",
                    is_sharp_gambit_family=True,
                    toggle_state_required="sharp_on" if str(local_admission.get("failure_reason_code") or "").strip().lower() == "would_pass_if_sharp_toggle_enabled" else None,
                    source="admission_metadata",
                    supporting_reason_codes=tuple(
                        code for code in (str(local_admission.get("failure_reason_code") or "").strip(),) if code
                    ),
                )

        explanation = current_band_explanation or self.get_failure_explanation(position_key, band_id, move_uci, mode_id)
        if explanation is not None:
            sharp_explanation = self.explanation_is_sharp_gambit_family(explanation)
            if sharp_explanation:
                return MoveFamilyPolicy(
                    family_label=_as_text(explanation.get("family_label")) or "sharp/gambit",
                    is_sharp_gambit_family=True,
                    toggle_state_required=_as_text(explanation.get("toggle_state_required")),
                    source="current_band_failure_explanation",
                    supporting_reason_codes=tuple(
                        code for code in (str(explanation.get("reason_code") or "").strip(),) if code
                    ),
                )

        global_candidates = self._failure_explanations_by_move.get((position_key, move_uci), [])
        for candidate in global_candidates:
            if self.explanation_is_sharp_gambit_family(candidate):
                return MoveFamilyPolicy(
                    family_label=_as_text(candidate.get("family_label")) or "sharp/gambit",
                    is_sharp_gambit_family=True,
                    toggle_state_required=_as_text(candidate.get("toggle_state_required")),
                    source="global_failure_explanations",
                    supporting_reason_codes=tuple(
                        sorted(
                            {
                                str(row.get("reason_code") or "").strip()
                                for row in global_candidates
                                if str(row.get("reason_code") or "").strip()
                            }
                        )
                    ),
                )

        if local_admission:
            return MoveFamilyPolicy(
                family_label=_as_text(local_admission.get("family_label")),
                is_sharp_gambit_family=False,
                toggle_state_required=None,
                source="admission_metadata_non_sharp",
                supporting_reason_codes=(),
            )

        return MoveFamilyPolicy(
            family_label=None,
            is_sharp_gambit_family=None,
            toggle_state_required=None,
            source="unknown_family",
            supporting_reason_codes=(),
        )

    @staticmethod
    def admission_is_sharp_gambit_family(admission: dict[str, Any] | None) -> bool:
        if not admission:
            return False
        family_label = str(admission.get("family_label") or "").strip().lower()
        if family_label == "sharp/gambit":
            return True
        reason_code = str(admission.get("failure_reason_code") or "").strip().lower()
        if reason_code == "would_pass_if_sharp_toggle_enabled":
            return True
        local_reason = str(admission.get("local_reason") or "").strip().lower()
        if "sharp" in local_reason or "gambit" in local_reason:
            return True
        return False

    @staticmethod
    def explanation_is_sharp_gambit_family(explanation: dict[str, Any] | None) -> bool:
        if not explanation:
            return False
        reason_code = str(explanation.get("reason_code") or "").strip().lower()
        toggle_state_required = str(explanation.get("toggle_state_required") or "").strip().lower()
        family_label = str(explanation.get("family_label") or "").strip().lower()
        if reason_code == "would_pass_if_sharp_toggle_enabled":
            return True
        if toggle_state_required == "sharp_on":
            return True
        if family_label == "sharp/gambit":
            return True
        return False


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
