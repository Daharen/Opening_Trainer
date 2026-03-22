from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class EvaluatorConfig:
    """
    Centralized thresholds for the opening evaluator.

    Defaults are intentionally conservative so accepted moves stay close to
    engine best play while still allowing a modest restart-friendly tolerance.
    """

    better_max_cp_loss: int = 90
    overlay_best_max_cp_loss: int = 15
    overlay_excellent_max_cp_loss: int = 45
    overlay_good_max_cp_loss: int = 90
    overlay_mistake_min_cp_loss: int = 140
    overlay_blunder_min_cp_loss: int = 260
    missed_win_enabled: bool = True
    missed_win_mate_ply_cap_by_mode: dict[str, int] = field(
        default_factory=lambda: {"default": 4}
    )
    engine_depth: int = 12
    engine_time_limit_seconds: float = 0.2
    engine_path: str = "stockfish"
    active_envelope_player_moves: int = 5

    def snapshot(self) -> dict[str, object]:
        return asdict(self)

    def mate_ply_cap_for_mode(self, mode: str = "default") -> int:
        return self.missed_win_mate_ply_cap_by_mode.get(
            mode,
            self.missed_win_mate_ply_cap_by_mode["default"],
        )
