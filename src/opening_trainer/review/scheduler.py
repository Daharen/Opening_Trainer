from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .models import ReviewItem, RoutingSource, UrgencyTier, due_state, utc_now_iso

SRS_INTERVAL_DAYS = [1, 3, 7, 14, 30, 60, 120]
ORDINARY_DUE_RETIREMENT_THRESHOLD = 10


def _urgency_for_failures(item: ReviewItem) -> str:
    if item.consecutive_failures >= 4:
        return UrgencyTier.EXTREME.value
    if item.consecutive_failures >= 2:
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
    item.frequency_state = item.urgency_tier
    item.frequency_state_entered_at_utc = now
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
    if routing_reason == RoutingSource.SRS_DUE_REVIEW.value:
        item.srs_stage_index = 0
        item.srs_lapse_count += 1
    if item.srs_stage_index == 0:
        item.srs_next_due_at_utc = (datetime.now(timezone.utc) + timedelta(days=SRS_INTERVAL_DAYS[0])).replace(microsecond=0).isoformat()
    item.srs_last_reviewed_at_utc = now
    item.srs_last_result = 'failure'
    return item


def apply_success(item: ReviewItem, routing_reason: str) -> ReviewItem:
    now = utc_now_iso()
    item.updated_at_utc = now
    item.last_seen_at_utc = now
    item.last_passed_at_utc = now
    item.times_seen += 1
    item.times_passed += 1
    item.consecutive_successes += 1
    item.success_streak = item.consecutive_successes
    item.consecutive_failures = 0
    item.mastery_score = min(1.0, item.mastery_score + 0.15)
    item.stability_score = min(1.0, item.stability_score + 0.2)
    old_tier = item.urgency_tier
    demotion_marker = None
    if item.urgency_tier == UrgencyTier.EXTREME.value and item.consecutive_successes >= 2:
        item.urgency_tier = UrgencyTier.BOOSTED.value
        item.frequency_state = item.urgency_tier
        item.frequency_state_entered_at_utc = now
        item.skipped_review_slots = 0
        demotion_marker = 'TIER_DEMOTION extreme_to_boosted'
    elif item.urgency_tier == UrgencyTier.BOOSTED.value and item.consecutive_successes >= 4:
        item.urgency_tier = UrgencyTier.ORDINARY.value
        item.frequency_state = item.urgency_tier
        item.frequency_state_entered_at_utc = now
        item.skipped_review_slots = 0
        demotion_marker = 'TIER_DEMOTION boosted_to_due'
    elif item.urgency_tier == UrgencyTier.ORDINARY.value and item.consecutive_successes >= ORDINARY_DUE_RETIREMENT_THRESHOLD:
        item.frequency_retired_for_current_due_cycle = True
        item.skipped_review_slots = 0
        demotion_marker = 'FREQUENCY_RETIRE_DUE_CYCLE'
    if routing_reason == RoutingSource.SRS_DUE_REVIEW.value:
        item.srs_stage_index = min(item.srs_stage_index + 1, len(SRS_INTERVAL_DAYS) - 1)
        interval_days = SRS_INTERVAL_DAYS[item.srs_stage_index]
        item.srs_next_due_at_utc = (datetime.now(timezone.utc) + timedelta(days=interval_days)).replace(microsecond=0).isoformat()
        item.srs_last_reviewed_at_utc = now
        item.srs_last_result = 'success'
    item.due_at_utc = now if item.urgency_tier != UrgencyTier.ORDINARY.value else item.due_at_utc
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
