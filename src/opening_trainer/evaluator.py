from __future__ import annotations

import chess

from .bundle_corpus import normalize_builder_position_key
from .evaluation import (
    AuthoritySource,
    CanonicalJudgment,
    EngineAuthority,
    EvaluationResult,
    EvaluatorConfig,
    OpeningBookAuthority,
    OverlayClassifier,
    OverlayLabel,
    ReasonCode,
    resolve_canonical_judgment,
)
from .practical_risk_reconciled import (
    PracticalRiskReconciledService,
    ReconciledFailureRenderer,
    admission_is_sharp_gambit_family,
)
from .session_logging import log_line


class MoveEvaluator:
    def __init__(
        self,
        config: EvaluatorConfig | None = None,
        book_authority: OpeningBookAuthority | None = None,
        engine_authority: EngineAuthority | None = None,
        overlay_classifier: OverlayClassifier | None = None,
        reconciled_service: PracticalRiskReconciledService | None = None,
    ):
        self.config = config or EvaluatorConfig()
        self.book_authority = book_authority or OpeningBookAuthority()
        self.engine_authority = engine_authority or EngineAuthority(self.config)
        self.overlay_classifier = overlay_classifier or OverlayClassifier(self.config)
        self.reconciled_service = reconciled_service

    def evaluate(
        self,
        board_before_move: chess.Board,
        played_move: chess.Move,
        player_move_number: int,
        mode: str = "default",
        requested_band_id: str | None = None,
        allow_sharp_gambit_lines: bool = False,
    ) -> EvaluationResult:
        legal_move_confirmed = played_move in board_before_move.legal_moves
        move_uci = played_move.uci()

        if not legal_move_confirmed:
            return EvaluationResult(
                accepted=False,
                canonical_judgment=CanonicalJudgment.FAIL,
                overlay_label=OverlayLabel.BLUNDER,
                reason_code=ReasonCode.ILLEGAL_MOVE,
                reason_text="Illegal move rejected before evaluator analysis.",
                authority_source=AuthoritySource.NONE,
                move_uci=move_uci,
                legal_move_confirmed=False,
                metadata={"player_move_number": player_move_number, "thresholds": self.config.snapshot()},
            )

        book_result = self.book_authority.evaluate(board_before_move, played_move)
        engine_result = self.engine_authority.evaluate(board_before_move, played_move)
        accepted, canonical_judgment, authority_source = resolve_canonical_judgment(book_result, engine_result)

        if canonical_judgment == CanonicalJudgment.AUTHORITY_UNAVAILABLE:
            overlay_label = OverlayLabel.AUTHORITY_UNAVAILABLE
            overlay_reason_code = engine_result.reason_code
            overlay_reason_text = engine_result.reason_text
        else:
            overlay_label, overlay_reason_code, overlay_reason_text = self.overlay_classifier.classify(
                canonical_judgment,
                engine_result,
                mode,
            )

        if canonical_judgment == CanonicalJudgment.BOOK:
            reason_code = book_result.reason_code
            reason_text = book_result.reason_text
        elif engine_result.available:
            reason_code = overlay_reason_code
            reason_text = overlay_reason_text
        else:
            reason_code = engine_result.reason_code
            reason_text = engine_result.reason_text

        if accepted and overlay_label not in self.config.accepted_overlay_labels():
            accepted = False
            canonical_judgment = CanonicalJudgment.FAIL
            reason_text = 'Rejected under current acceptance policy: Good moves are configured to count as fails.'

        position_key = normalize_builder_position_key(board_before_move)
        reconciled_meta = {
            "requested_band_id": requested_band_id,
            "resolved_band_id": None,
            "band_resolution_provenance": "artifact_inactive",
            "mode_id": "good_inclusive" if self.config.good_moves_acceptable else "good_exclusive",
            "artifact_active": bool(self.reconciled_service and self.reconciled_service.active),
            "decision_source": "legacy_engine_book",
        }
        if canonical_judgment == CanonicalJudgment.FAIL:
            accepted, canonical_judgment, reason_text, reconciled_meta = self._apply_reconciled_rules(
                accepted=accepted,
                canonical_judgment=canonical_judgment,
                reason_text=reason_text,
                position_key=position_key,
                move_uci=move_uci,
                requested_band_id=requested_band_id,
                allow_sharp_gambit_lines=allow_sharp_gambit_lines,
                mode_id=reconciled_meta["mode_id"],
            )

        preferred_move_uci = None
        preferred_move_san = None
        if canonical_judgment == CanonicalJudgment.FAIL:
            preferred_move_uci = book_result.candidate_move_uci or engine_result.best_move_uci
            preferred_move_san = engine_result.best_move_san if preferred_move_uci == engine_result.best_move_uci else None

        metadata = {
            "player_move_number": player_move_number,
            "thresholds": self.config.snapshot(),
            "book": book_result.metadata,
            "engine": {
                **engine_result.metadata,
                "cp_loss": engine_result.cp_loss,
                "best_score_cp": engine_result.best_score_cp,
                "played_score_cp": engine_result.played_score_cp,
                "best_move_uci": engine_result.best_move_uci,
                "best_move_san": engine_result.best_move_san,
                "played_move_san": engine_result.played_move_san,
                "mate_for_side_to_move": engine_result.mate_for_side_to_move,
                "mate_after_move_for_side_to_move": engine_result.mate_after_move_for_side_to_move,
                "principal_variation": engine_result.principal_variation,
            },
            "reconciled": reconciled_meta,
        }

        return EvaluationResult(
            accepted=accepted,
            canonical_judgment=canonical_judgment,
            overlay_label=overlay_label,
            reason_code=reason_code,
            reason_text=reason_text,
            authority_source=authority_source,
            move_uci=move_uci,
            legal_move_confirmed=True,
            preferred_move_uci=preferred_move_uci,
            preferred_move_san=preferred_move_san,
            metadata=metadata,
        )

    def _apply_reconciled_rules(
        self,
        *,
        accepted: bool,
        canonical_judgment: CanonicalJudgment,
        reason_text: str,
        position_key: str,
        move_uci: str,
        requested_band_id: str | None,
        allow_sharp_gambit_lines: bool,
        mode_id: str,
    ) -> tuple[bool, CanonicalJudgment, str, dict[str, object]]:
        service = self.reconciled_service
        metadata: dict[str, object] = {
            "requested_band_id": requested_band_id,
            "resolved_band_id": None,
            "band_resolution_provenance": "artifact_inactive",
            "mode_id": mode_id,
            "artifact_active": bool(service and service.active),
            "decision_source": "legacy_engine_book",
        }
        if not service or not service.active:
            metadata["activation_error"] = getattr(service, "activation_error", "artifact_not_configured") if service else "artifact_not_configured"
            log_line(
                "PRACTICAL_RISK_FAIL_FALLBACK_TO_LEGACY "
                f"position_key={position_key} move_uci={move_uci} reason={metadata['activation_error']}",
                tag="evaluation",
            )
            return accepted, canonical_judgment, reason_text, metadata

        resolution = service.resolve_band_id(requested_band_id)
        metadata["resolved_band_id"] = resolution.resolved_band_id
        metadata["band_resolution_provenance"] = resolution.provenance
        log_line(
            "PRACTICAL_RISK_FAIL_INTERCEPT_BEGIN "
            f"position_key={position_key} move_uci={move_uci} requested_band={requested_band_id or 'unset'} "
            f"resolved_band={resolution.resolved_band_id or 'unset'} mode_id={mode_id} "
            f"sharp_toggle={'on' if allow_sharp_gambit_lines else 'off'}",
            tag="evaluation",
        )
        if not resolution.resolved_band_id:
            metadata["decision_source"] = "reconciled_missing_band"
            log_line(
                "PRACTICAL_RISK_FAIL_FALLBACK_TO_LEGACY "
                f"position_key={position_key} move_uci={move_uci} reason=missing_resolved_band",
                tag="evaluation",
            )
            return accepted, canonical_judgment, reason_text, metadata

        admission = service.get_move_admission(position_key, resolution.resolved_band_id, move_uci)
        if admission is None:
            metadata["decision_source"] = "reconciled_artifact_no_row"
            log_line(
                "PRACTICAL_RISK_FAIL_FALLBACK_TO_LEGACY "
                f"position_key={position_key} move_uci={move_uci} reason=no_admission_row",
                tag="evaluation",
            )
            return accepted, canonical_judgment, reason_text, metadata

        mode_to_reconciled_admission_column = {
            "good_inclusive": "reconciled_admitted_if_good_accepted",
            "good_exclusive": "reconciled_admitted_if_good_rejected",
        }
        mode_to_origin_column = {
            "good_inclusive": "reconciled_admission_origin_if_good_accepted",
            "good_exclusive": "reconciled_admission_origin_if_good_rejected",
        }
        admitted_column = mode_to_reconciled_admission_column.get(mode_id)
        origin_column = mode_to_origin_column.get(mode_id)
        admitted = bool(admission.get(admitted_column)) if admitted_column else False
        explanation = service.get_failure_explanation(position_key, resolution.resolved_band_id, move_uci, mode_id)
        is_sharp_family = admission_is_sharp_gambit_family(admission, explanation)

        metadata["admitted_column"] = admitted_column
        metadata["admission_origin"] = admission.get(origin_column) or admission.get("admission_origin")
        metadata["engine_quality_class"] = admission.get("engine_quality_class")
        metadata["local_reason"] = admission.get("local_reason")
        metadata["local_admitted_if_good_accepted"] = admission.get("local_admitted_if_good_accepted")
        metadata["local_admitted_if_good_rejected"] = admission.get("local_admitted_if_good_rejected")
        metadata["reconciled_admitted_if_good_accepted"] = admission.get("reconciled_admitted_if_good_accepted")
        metadata["reconciled_admitted_if_good_rejected"] = admission.get("reconciled_admitted_if_good_rejected")
        metadata["family_label"] = admission.get("family_label")
        metadata["is_sharp_gambit_family"] = is_sharp_family

        if admitted and is_sharp_family and not allow_sharp_gambit_lines:
            failure_row = explanation or {
                "reason_code": "would_pass_if_sharp_toggle_enabled",
                "template_id": "runtime_sharp_toggle_policy",
                "family_label": admission.get("family_label") or "sharp/gambit line",
                "max_practical_band_id": admission.get("practical_ceiling_band_id") or resolution.resolved_band_id,
                "first_failure_band_id": None,
                "toggle_state_required": "sharp_on",
                "rendered_preview": None,
            }
            rendered = ReconciledFailureRenderer.render(
                failure_row,
                requested_band_id=requested_band_id,
                resolved_band_id=resolution.resolved_band_id,
            )
            reason_code = failure_row.get("reason_code") or "would_pass_if_sharp_toggle_enabled"
            metadata["decision_source"] = "reconciled_admission_blocked_by_sharp_toggle"
            metadata["failure_explanation"] = failure_row
            metadata["would_pass_with_sharp_enabled"] = True
            log_line(
                "PRACTICAL_RISK_FAIL_CONFIRMED "
                f"position_key={position_key} move_uci={move_uci} reason_code={reason_code} "
                f"template_id={failure_row.get('template_id') or 'runtime_sharp_toggle_policy'} "
                f"max_practical_band={failure_row.get('max_practical_band_id') or 'unknown'} "
                f"first_failure_band={failure_row.get('first_failure_band_id') or 'unknown'} "
                f"toggle_state_required={failure_row.get('toggle_state_required') or 'sharp_on'}",
                tag="evaluation",
            )
            return False, CanonicalJudgment.FAIL, rendered, metadata

        if admitted:
            metadata["decision_source"] = "reconciled_admission"
            rescue_reason_text = "Accepted via practical-risk reconciliation for the current training band."
            log_line(
                "PRACTICAL_RISK_FAIL_RESCUED "
                f"position_key={position_key} move_uci={move_uci} reason=admitted "
                f"origin={admission.get('admission_origin') or 'unknown'} "
                f"resolved_band={resolution.resolved_band_id} mode_id={mode_id}",
                tag="evaluation",
            )
            return True, CanonicalJudgment.BETTER, rescue_reason_text, metadata

        if explanation is None:
            metadata["decision_source"] = "reconciled_reject_no_explanation"
            log_line(
                "PRACTICAL_RISK_FAIL_FALLBACK_TO_LEGACY "
                f"position_key={position_key} move_uci={move_uci} reason=missing_failure_explanation",
                tag="evaluation",
            )
            return False, CanonicalJudgment.FAIL, reason_text, metadata

        metadata["failure_explanation"] = explanation
        reason_code = explanation.get("reason_code")
        sharp_override_allowed = (
            allow_sharp_gambit_lines
            and reason_code == "would_pass_if_sharp_toggle_enabled"
            and explanation.get("toggle_state_required") == "sharp_on"
        )
        metadata["would_pass_with_sharp_enabled"] = reason_code == "would_pass_if_sharp_toggle_enabled"
        if sharp_override_allowed:
            metadata["decision_source"] = "sharp_toggle_override_from_failure_explanation"
            log_line(
                "PRACTICAL_RISK_FAIL_RESCUED "
                f"position_key={position_key} move_uci={move_uci} reason=sharp_toggle_override "
                f"origin={admission.get('admission_origin') or 'unknown'} "
                f"resolved_band={resolution.resolved_band_id} mode_id={mode_id}",
                tag="evaluation",
            )
            return True, CanonicalJudgment.BETTER, "Accepted via sharp/gambit override.", metadata

        rendered = ReconciledFailureRenderer.render(
            explanation,
            requested_band_id=requested_band_id,
            resolved_band_id=resolution.resolved_band_id,
        )
        metadata["decision_source"] = "reconciled_failure_explanation"
        log_line(
            "PRACTICAL_RISK_FAIL_CONFIRMED "
            f"position_key={position_key} move_uci={move_uci} reason_code={reason_code or 'unknown'} "
            f"template_id={explanation.get('template_id') or 'unknown'} "
            f"max_practical_band={explanation.get('max_practical_band_id') or 'unknown'} "
            f"first_failure_band={explanation.get('first_failure_band_id') or 'unknown'} "
            f"toggle_state_required={explanation.get('toggle_state_required') or 'none'}",
            tag="evaluation",
        )
        return False, CanonicalJudgment.FAIL, rendered, metadata
