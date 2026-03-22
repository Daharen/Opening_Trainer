from __future__ import annotations

from .types import EvaluationResult


def format_evaluation_feedback(result: EvaluationResult) -> list[str]:
    status = "ACCEPTED" if result.accepted else "REJECTED"
    if result.canonical_judgment.value == "AuthorityUnavailable":
        status = "UNAVAILABLE"
    lines = [
        f"{result.canonical_judgment.value} / {result.overlay_label.value} — {status}",
        result.reason_text,
    ]
    if result.preferred_move_san or result.preferred_move_uci:
        preferred = result.preferred_move_san or result.preferred_move_uci
        lines.append(f"Preferred move: {preferred}")
    return lines
