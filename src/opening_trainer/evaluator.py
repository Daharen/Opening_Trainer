from __future__ import annotations

import chess
from .practical_risk_reconciled import render_failure_explanation

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


class MoveEvaluator:
    def __init__(
        self,
        config: EvaluatorConfig | None = None,
        book_authority: OpeningBookAuthority | None = None,
        engine_authority: EngineAuthority | None = None,
        overlay_classifier: OverlayClassifier | None = None,
    ):
        self.config = config or EvaluatorConfig()
        self.book_authority = book_authority or OpeningBookAuthority()
        self.engine_authority = engine_authority or EngineAuthority(self.config)
        self.overlay_classifier = overlay_classifier or OverlayClassifier(self.config)
        self.practical_risk_reconciled = None
        self.practical_risk_context: dict[str, object] = {}

    def evaluate(
        self,
        board_before_move: chess.Board,
        played_move: chess.Move,
        player_move_number: int,
        mode: str = "default",
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
        reconciled_metadata: dict[str, object] = {}
        reconciled = self.practical_risk_reconciled
        context = self.practical_risk_context if isinstance(self.practical_risk_context, dict) else {}
        if reconciled is not None and getattr(reconciled, "active", False):
            position_key = context.get("position_key")
            requested_band_id = context.get("requested_band_id")
            good_mode = bool(context.get("good_moves_acceptable", self.config.good_moves_acceptable))
            mode_id = "good_inclusive" if good_mode else "good_exclusive"
            allow_sharp = bool(context.get("allow_sharp_gambit_lines", False))
            if isinstance(position_key, str) and position_key.strip():
                band_resolution = reconciled.resolve_band_id(str(requested_band_id) if requested_band_id else None)
                admission = reconciled.get_move_admission(position_key, band_resolution.resolved_band_id, move_uci)
                if admission is not None:
                    admitted = bool(admission.get("admitted_good_inclusive")) if mode_id == "good_inclusive" else bool(admission.get("admitted_good_exclusive"))
                    reconciled_metadata = {
                        "requested_band_id": band_resolution.requested_band_id,
                        "resolved_band_id": band_resolution.resolved_band_id,
                        "band_resolution_provenance": band_resolution.provenance,
                        "admission_origin": admission.get("admission_origin"),
                        "engine_quality_class": admission.get("engine_quality_class"),
                        "local_reason": admission.get("local_reason"),
                        "reconciled_local_distinction": admission.get("reconciled_local_distinction"),
                        "mode_id": mode_id,
                    }
                    if admitted:
                        accepted = True
                        if canonical_judgment != CanonicalJudgment.BOOK:
                            canonical_judgment = CanonicalJudgment.BETTER
                            authority_source = AuthoritySource.ENGINE
                    else:
                        failure = reconciled.get_failure_explanation(position_key, band_resolution.resolved_band_id, move_uci, mode_id)
                        reconciled_metadata["failure_explanation"] = failure
                        sharp_override = (
                            allow_sharp
                            and isinstance(failure, dict)
                            and str(failure.get("reason_code")) == "would_pass_if_sharp_toggle_enabled"
                            and str(failure.get("toggle_state_required")) == "sharp_on"
                        )
                        if sharp_override:
                            accepted = True
                            if canonical_judgment != CanonicalJudgment.BOOK:
                                canonical_judgment = CanonicalJudgment.BETTER
                                authority_source = AuthoritySource.ENGINE
                            reconciled_metadata["override"] = "sharp_toggle_override_from_failure_explanation"
                        else:
                            accepted = False
                            canonical_judgment = CanonicalJudgment.FAIL
                            if failure is not None:
                                reconciled_metadata["failure_text"] = render_failure_explanation(
                                    failure,
                                    requested_band_id=band_resolution.requested_band_id,
                                    resolved_band_id=band_resolution.resolved_band_id,
                                )
                else:
                    reconciled_metadata = {"provenance": "reconciled_artifact_no_row"}

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
        elif reconciled_metadata.get("override"):
            reason_code = ReasonCode.SHARP_TOGGLE_OVERRIDE
            reason_text = "Accepted via sharp/gambit toggle override from reconciled artifact."
        elif canonical_judgment == CanonicalJudgment.FAIL and reconciled_metadata.get("failure_text"):
            reason_code = ReasonCode.RECONCILED_FAIL
            reason_text = str(reconciled_metadata["failure_text"])
        elif accepted and reconciled_metadata:
            reason_code = ReasonCode.RECONCILED_ADMISSION
            reason_text = "Accepted by reconciled practical-risk admissions."
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
            "reconciled": reconciled_metadata,
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
