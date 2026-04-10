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
from .practical_risk_reconciled import PracticalRiskReconciledService, ReconciledFailureRenderer


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
        if canonical_judgment != CanonicalJudgment.BOOK:
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
            return accepted, canonical_judgment, reason_text, metadata

        resolution = service.resolve_band_id(requested_band_id)
        metadata["resolved_band_id"] = resolution.resolved_band_id
        metadata["band_resolution_provenance"] = resolution.provenance
        if not resolution.resolved_band_id:
            metadata["decision_source"] = "reconciled_missing_band"
            return accepted, canonical_judgment, reason_text, metadata

        admission = service.get_move_admission(position_key, resolution.resolved_band_id, move_uci)
        if admission is None:
            metadata["decision_source"] = "reconciled_artifact_no_row"
            return accepted, canonical_judgment, reason_text, metadata

        admitted = bool(admission.get(f"admitted_{mode_id}"))
        metadata["admission_origin"] = admission.get("admission_origin")
        metadata["engine_quality_class"] = admission.get("engine_quality_class")
        metadata["local_reason"] = admission.get("local_reason")

        if admitted:
            metadata["decision_source"] = "reconciled_admission"
            return True, CanonicalJudgment.BETTER, reason_text, metadata

        explanation = service.get_failure_explanation(position_key, resolution.resolved_band_id, move_uci, mode_id)
        if explanation is None:
            metadata["decision_source"] = "reconciled_reject_no_explanation"
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
            return True, CanonicalJudgment.BETTER, "Accepted via sharp/gambit override.", metadata

        rendered = ReconciledFailureRenderer.render(
            explanation,
            requested_band_id=requested_band_id,
            resolved_band_id=resolution.resolved_band_id,
        )
        metadata["decision_source"] = "reconciled_failure_explanation"
        return False, CanonicalJudgment.FAIL, rendered, metadata
