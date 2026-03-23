from __future__ import annotations

import chess

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
