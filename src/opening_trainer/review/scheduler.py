from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .models import ReviewItem, UrgencyTier, due_state, utc_now_iso

SUCCESS_LADDER_MINUTES = [5, 30, 240, 1440, 4320]
ORDINARY_DUE_RETIREMENT_THRESHOLD = 10


def _urgency_for_failures(item: ReviewItem) -> str:
    if item.consecutive_failures >= 5 and item.consecutive_successes < 3:
        return UrgencyTier.EXTREME.value
    if item.consecutive_failures >= 3:
        return UrgencyTier.BOOSTED.value
    return UrgencyTier.ORDINARY.value


def _clear_stubborn_state(item: ReviewItem) -> None:
    item.stubborn_extreme_state = 'none'
    item.stubborn_extra_repeat_consumed_until_success = False
    item.pending_forced_stubborn_repeat = False


def apply_failure(item: ReviewItem, failure_reason: str, preferred_move_uci: str | None, predecessor_path: list[dict], line_preview: str, routing_reason: str) -> ReviewItem:
    was_armed = item.stubborn_extreme_state == 'armed_after_fifth_failure'
    now = utc_now_iso()
    item.updated_at_utc = now
    item.last_seen_at_utc = now
    item.last_failed_at_utc = now
    item.times_seen += 1
    item.times_failed += 1
    item.consecutive_failures += 1
    item.consecutive_successes = 0
    item.success_streak = 0
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
    item.pending_forced_stubborn_repeat = False
    if item.urgency_tier == UrgencyTier.EXTREME.value and item.consecutive_failures >= 5 and item.stubborn_extreme_state == 'none':
        item.stubborn_extreme_state = 'armed_after_fifth_failure'
        item.last_routing_reason = f'{routing_reason}|EXTREME_STUBBORN_ARMED'
    if was_armed and item.urgency_tier == UrgencyTier.EXTREME.value:
        item.stubborn_extreme_state = 'cooldown_until_success'
        item.stubborn_extra_repeat_consumed_until_success = True
        item.pending_forced_stubborn_repeat = True
        item.last_routing_reason = f'{routing_reason}|EXTREME_STUBBORN_EXTRA_REPEAT|EXTREME_STUBBORN_COOLDOWN'
    return item


def apply_success(item: ReviewItem, routing_reason: str) -> ReviewItem:
    now = utc_now_iso()
    item.updated_at_utc = now
    item.last_seen_at_utc = now
    item.last_passed_at_utc = now
    item.times_seen += 1
    item.times_passed += 1
    item.consecutive_successes += 1
    item.success_streak += 1
    item.consecutive_failures = 0
    item.mastery_score = min(1.0, item.mastery_score + 0.15)
    item.stability_score = min(1.0, item.stability_score + 0.2)
    old_tier = item.urgency_tier
    demotion_marker = None
    if item.urgency_tier == UrgencyTier.EXTREME.value and item.success_streak >= 2:
        item.urgency_tier = UrgencyTier.BOOSTED.value
        item.skipped_review_slots = 0
        demotion_marker = 'TIER_DEMOTION extreme_to_boosted'
    elif item.urgency_tier == UrgencyTier.BOOSTED.value and item.success_streak >= 5:
        item.urgency_tier = UrgencyTier.ORDINARY.value
        item.skipped_review_slots = 0
        demotion_marker = 'TIER_DEMOTION boosted_to_due'
    elif item.urgency_tier == UrgencyTier.ORDINARY.value and item.success_streak >= ORDINARY_DUE_RETIREMENT_THRESHOLD:
        item.frequency_retired_for_current_due_cycle = True
        item.skipped_review_slots = 0
        demotion_marker = 'FREQUENCY_RETIRE_DUE_CYCLE'
    ladder_index = min(item.consecutive_successes - 1, len(SUCCESS_LADDER_MINUTES) - 1)
    interval = timedelta(minutes=SUCCESS_LADDER_MINUTES[max(ladder_index, 0)])
    item.due_at_utc = (datetime.now(timezone.utc) + interval).replace(microsecond=0).isoformat()
    item.last_routing_reason = f'{routing_reason}|{demotion_marker}' if demotion_marker else routing_reason
    item.urgency_multiplier = 2.5 if item.urgency_tier == UrgencyTier.EXTREME.value else (1.5 if item.urgency_tier == UrgencyTier.BOOSTED.value else 1.0)
    item.pending_forced_stubborn_repeat = False
    if item.stubborn_extreme_state == 'cooldown_until_success':
        item.last_routing_reason = f'{item.last_routing_reason}|EXTREME_STUBBORN_CLEARED'
    if old_tier != UrgencyTier.EXTREME.value or item.urgency_tier != UrgencyTier.EXTREME.value:
        _clear_stubborn_state(item)
    elif item.stubborn_extreme_state == 'cooldown_until_success':
        _clear_stubborn_state(item)
    return item


def sync_due_cycle_transition(item: ReviewItem) -> bool:
    now_due = due_state(item.due_at_utc) == 'due'
    changed = False
    if item.was_due_previous_check and not now_due:
        item.skipped_review_slots = 0
    if item.frequency_retired_for_current_due_cycle and (not item.was_due_previous_check) and now_due:
        item.frequency_retired_for_current_due_cycle = False
        item.success_streak = 0
        item.consecutive_successes = 0
        item.consecutive_failures = 0
        item.skipped_review_slots = 0
        _clear_stubborn_state(item)
        changed = True
    if item.was_due_previous_check != now_due:
        changed = True
    item.was_due_previous_check = now_due
    return changed
