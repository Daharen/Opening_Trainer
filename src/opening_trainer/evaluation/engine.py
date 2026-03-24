from __future__ import annotations

import chess
import chess.engine

from .config import EvaluatorConfig
from .engine_process import launch_engine
from .types import EngineAuthorityResult, ReasonCode


class EngineAuthority:
    def __init__(self, config: EvaluatorConfig):
        self.config = config
        self._engine: chess.engine.SimpleEngine | None = None

    def best_reply(self, board: chess.Board) -> tuple[str | None, str | None]:
        try:
            engine = self._ensure_engine()
            limit = chess.engine.Limit(
                depth=self.config.engine_depth,
                time=self.config.engine_time_limit_seconds,
            )
            info = engine.analyse(board, limit)
            best_move = info.get("pv", [None])[0]
            if best_move is None:
                return None, None
            return best_move.uci(), board.san(best_move)
        except (FileNotFoundError, chess.engine.EngineError, OSError):
            self._close_engine()
            return None, None

    def evaluate(self, board_before_move: chess.Board, played_move: chess.Move) -> EngineAuthorityResult:
        played_move_uci = played_move.uci()
        played_move_san = board_before_move.san(played_move)

        try:
            engine = self._ensure_engine()
            limit = chess.engine.Limit(
                depth=self.config.engine_depth,
                time=self.config.engine_time_limit_seconds,
            )
            best_info = engine.analyse(board_before_move, limit)
            best_move = best_info.get("pv", [None])[0]

            if best_move is None:
                return EngineAuthorityResult(
                    accepted=False,
                    available=False,
                    reason_code=ReasonCode.ENGINE_UNAVAILABLE,
                    reason_text="Engine analysis returned no principal variation.",
                    played_move_uci=played_move_uci,
                    played_move_san=played_move_san,
                    metadata={"engine_available": False},
                )

            board_after_move = board_before_move.copy(stack=False)
            board_after_move.push(played_move)

            played_info = engine.analyse(board_after_move, limit)

        except (FileNotFoundError, chess.engine.EngineError, OSError) as exc:
            self._close_engine()
            return EngineAuthorityResult(
                accepted=False,
                available=False,
                reason_code=ReasonCode.ENGINE_UNAVAILABLE,
                reason_text=f"Engine analysis unavailable: {exc}",
                played_move_uci=played_move_uci,
                played_move_san=played_move_san,
                metadata={"engine_available": False, "engine_path": self.config.engine_path},
            )

        mover = board_before_move.turn
        best_score = self._score_for_side(best_info, mover)
        played_score = self._score_for_side(played_info, mover)
        cp_loss = self._centipawn_loss(best_score, played_score)
        mate_for_side_to_move = self._mate_for_side(best_info, mover)
        mate_after_move = self._mate_for_side(played_info, mover)
        best_move_san = board_before_move.san(best_move)
        accepted = cp_loss is not None and cp_loss <= self.config.better_max_cp_loss
        reason_text = (
            f"Engine accepted move within {cp_loss} centipawns of best play."
            if accepted
            else f"Engine rejected move at {cp_loss} centipawns below best play."
        )

        return EngineAuthorityResult(
            accepted=accepted,
            available=True,
            reason_code=ReasonCode.ENGINE_PASS if accepted else ReasonCode.ENGINE_FAIL,
            reason_text=reason_text,
            best_move_uci=best_move.uci(),
            best_move_san=best_move_san,
            played_move_uci=played_move_uci,
            played_move_san=played_move_san,
            cp_loss=cp_loss,
            best_score_cp=best_score,
            played_score_cp=played_score,
            mate_for_side_to_move=mate_for_side_to_move,
            mate_after_move_for_side_to_move=mate_after_move,
            principal_variation=[move.uci() for move in best_info.get("pv", [])],
            metadata={"engine_available": True},
        )

    def _score_for_side(self, info: dict, side_to_move: chess.Color) -> int | None:
        score = info.get("score")
        if score is None:
            return None
        pov = score.pov(side_to_move)
        if pov.is_mate():
            mate = pov.mate()
            if mate is None:
                return None
            return 100000 - abs(mate) if mate > 0 else -100000 + abs(mate)
        return pov.score(mate_score=100000)

    def _mate_for_side(self, info: dict, side_to_move: chess.Color) -> int | None:
        score = info.get("score")
        if score is None:
            return None
        pov = score.pov(side_to_move)
        return pov.mate() if pov.is_mate() else None

    def _centipawn_loss(self, best_score: int | None, played_score: int | None) -> int | None:
        if best_score is None or played_score is None:
            return None
        return max(0, best_score - played_score)

    def _ensure_engine(self) -> chess.engine.SimpleEngine:
        if self._engine is None:
            self._engine = launch_engine(self.config)
        return self._engine

    def _close_engine(self) -> None:
        if self._engine is None:
            return
        try:
            self._engine.quit()
        except Exception:
            pass
        self._engine = None

    def __del__(self) -> None:
        self._close_engine()
