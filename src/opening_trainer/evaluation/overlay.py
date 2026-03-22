from __future__ import annotations

from .config import EvaluatorConfig
from .types import CanonicalJudgment, EngineAuthorityResult, OverlayLabel, ReasonCode


class OverlayClassifier:
    def __init__(self, config: EvaluatorConfig):
        self.config = config

    def classify(
        self,
        canonical_judgment: CanonicalJudgment,
        engine_result: EngineAuthorityResult,
        mode: str = "default",
    ) -> tuple[OverlayLabel, ReasonCode, str]:
        if canonical_judgment == CanonicalJudgment.BOOK:
            return OverlayLabel.BOOK, ReasonCode.BOOK_HIT, "Accepted via opening-book membership."

        cp_loss = engine_result.cp_loss if engine_result.cp_loss is not None else self.config.overlay_blunder_min_cp_loss

        if canonical_judgment == CanonicalJudgment.BETTER:
            if cp_loss <= self.config.overlay_best_max_cp_loss:
                return OverlayLabel.BEST, ReasonCode.ENGINE_PASS, "Accepted via engine tolerance as a best-equivalent move."
            if cp_loss <= self.config.overlay_excellent_max_cp_loss:
                return OverlayLabel.EXCELLENT, ReasonCode.ENGINE_PASS, "Accepted via engine tolerance as an excellent move."
            return OverlayLabel.GOOD, ReasonCode.ENGINE_PASS, "Accepted via engine tolerance as a good move."

        if self._is_missed_win(engine_result, mode):
            return OverlayLabel.MISSED_WIN, ReasonCode.MISSED_WIN, "Rejected because it missed a short forcing win inside the configured mate horizon."
        if cp_loss >= self.config.overlay_blunder_min_cp_loss or self._short_mate_collapsed(engine_result, mode):
            return OverlayLabel.BLUNDER, ReasonCode.ENGINE_FAIL, "Rejected as a blunder outside engine tolerance."
        if cp_loss >= self.config.overlay_mistake_min_cp_loss:
            return OverlayLabel.MISTAKE, ReasonCode.ENGINE_FAIL, "Rejected as a mistake outside engine tolerance."
        return OverlayLabel.INACCURACY, ReasonCode.ENGINE_FAIL, "Rejected as an inaccuracy outside engine tolerance."

    def _is_missed_win(self, engine_result: EngineAuthorityResult, mode: str) -> bool:
        if not self.config.missed_win_enabled:
            return False
        cap = self.config.mate_ply_cap_for_mode(mode)
        mate_before = engine_result.mate_for_side_to_move
        mate_after = engine_result.mate_after_move_for_side_to_move
        return (
            mate_before is not None
            and 0 < mate_before <= cap
            and (mate_after is None or mate_after <= 0 or mate_after > cap)
        )

    def _short_mate_collapsed(self, engine_result: EngineAuthorityResult, mode: str) -> bool:
        cap = self.config.mate_ply_cap_for_mode(mode)
        mate_before = engine_result.mate_for_side_to_move
        mate_after = engine_result.mate_after_move_for_side_to_move
        return mate_before is not None and 0 < mate_before <= cap and mate_after is None
