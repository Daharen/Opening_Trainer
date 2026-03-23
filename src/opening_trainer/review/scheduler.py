from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .models import ReviewItem, UrgencyTier, utc_now_iso

SUCCESS_LADDER_MINUTES = [5, 30, 240, 1440, 4320]


def _urgency_for_failures(item: ReviewItem) -> str:
    if item.consecutive_failures >= 5 and item.consecutive_successes < 3:
        return UrgencyTier.EXTREME.value
    if item.consecutive_failures >= 3:
        return UrgencyTier.BOOSTED.value
    return UrgencyTier.ORDINARY.value


def apply_failure(item: ReviewItem, failure_reason: str, preferred_move_uci: str | None, predecessor_path: list[dict], line_preview: str, routing_reason: str) -> ReviewItem:
    now = utc_now_iso()
    item.updated_at_utc = now
    item.last_seen_at_utc = now
    item.last_failed_at_utc = now
    item.times_seen += 1
    item.times_failed += 1
    item.consecutive_failures += 1
    item.consecutive_successes = 0
    item.mastery_score = max(0.0, item.mastery_score - 0.2)
    item.stability_score = max(0.0, item.stability_score - 0.2)
    item.urgency_tier = _urgency_for_failures(item)
    item.urgency_multiplier = 2.5 if item.urgency_tier == UrgencyTier.EXTREME.value else (1.5 if item.urgency_tier == UrgencyTier.BOOSTED.value else 1.0)
    item.due_at_utc = now
    item.failure_reason = failure_reason
    item.preferred_move_uci = preferred_move_uci
    item.predecessor_path = predecessor_path
    item.line_preview_san = line_preview
    item.last_routing_reason = routing_reason
    return item


def apply_success(item: ReviewItem, routing_reason: str) -> ReviewItem:
    now = utc_now_iso()
    item.updated_at_utc = now
    item.last_seen_at_utc = now
    item.last_passed_at_utc = now
    item.times_seen += 1
    item.times_passed += 1
    item.consecutive_successes += 1
    item.consecutive_failures = max(0, item.consecutive_failures - 1)
    item.mastery_score = min(1.0, item.mastery_score + 0.15)
    item.stability_score = min(1.0, item.stability_score + 0.2)
    if item.urgency_tier == UrgencyTier.EXTREME.value and item.consecutive_successes < 3:
        item.urgency_tier = UrgencyTier.EXTREME.value
    elif item.consecutive_failures >= 3:
        item.urgency_tier = UrgencyTier.BOOSTED.value
    else:
        item.urgency_tier = UrgencyTier.ORDINARY.value
    ladder_index = min(item.consecutive_successes - 1, len(SUCCESS_LADDER_MINUTES) - 1)
    interval = timedelta(minutes=SUCCESS_LADDER_MINUTES[max(ladder_index, 0)])
    item.due_at_utc = (datetime.now(timezone.utc) + interval).replace(microsecond=0).isoformat()
    item.last_routing_reason = routing_reason
    item.urgency_multiplier = 2.5 if item.urgency_tier == UrgencyTier.EXTREME.value else (1.5 if item.urgency_tier == UrgencyTier.BOOSTED.value else 1.0)
    return item
