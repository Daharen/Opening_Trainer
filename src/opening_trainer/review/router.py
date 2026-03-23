from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timezone

from .models import ReviewPlan, RoutingDecision, RoutingSource, UrgencyTier, due_state


@dataclass(frozen=True)
class RoutingConfig:
    corpus_share: float = 0.8
    review_share: float = 0.2
    extreme_bonus_review_share: float = 0.4
    per_item_recent_window: int = 10
    per_item_share_cap: float = 0.3


class ReviewRouter:
    def __init__(self, config: RoutingConfig | None = None):
        self.config = config or RoutingConfig()
        self.recent_item_ids = deque(maxlen=self.config.per_item_recent_window)
        self.turn_counter = 0

    def select(self, profile_id: str, items: list) -> RoutingDecision:
        self.turn_counter += 1
        due_items = [item for item in items if due_state(item.due_at_utc) == 'due']
        if not due_items:
            return RoutingDecision(RoutingSource.CORPUS.value, None, None, 'none_due', False, 'No due review items; using corpus play.', profile_id)
        due_items.sort(key=lambda item: (0 if item.urgency_tier == UrgencyTier.EXTREME.value else 1 if item.urgency_tier == UrgencyTier.BOOSTED.value else 2, item.due_at_utc, item.review_item_id))
        counts = Counter(self.recent_item_ids)
        selected = due_items[0]
        for item in due_items:
            if counts[item.review_item_id] / max(1, len(self.recent_item_ids)) <= self.config.per_item_share_cap:
                selected = item
                break
        is_extreme = selected.urgency_tier == UrgencyTier.EXTREME.value
        use_review = is_extreme or (self.turn_counter % max(1, round(1 / max(self.config.review_share, 0.01))) == 0)
        if not use_review:
            return RoutingDecision(RoutingSource.CORPUS.value, None, None, 'due_items_waiting', False, 'Due review items exist, but this turn remains in the corpus share.', profile_id)
        self.recent_item_ids.append(selected.review_item_id)
        source = RoutingSource.EXTREME.value if is_extreme else RoutingSource.SCHEDULED_REVIEW.value
        plan = ReviewPlan(
            root_fen='startpos',
            target_review_item_id=selected.review_item_id,
            target_position_key=selected.position_key,
            target_fen=selected.position_fen_normalized,
            predecessor_path=tuple(selected.predecessor_path),
            routing_reason=source,
        )
        return RoutingDecision(source, selected.review_item_id, selected.urgency_tier, due_state(selected.due_at_utc), bool(selected.predecessor_path), f'Selected review item {selected.review_item_id} with urgency {selected.urgency_tier}.', profile_id, review_plan=plan)

    def immediate_retry(self, profile_id: str, item) -> RoutingDecision:
        self.recent_item_ids.append(item.review_item_id)
        return RoutingDecision(RoutingSource.IMMEDIATE_RETRY.value, item.review_item_id, item.urgency_tier, 'immediate', bool(item.predecessor_path), 'Retrying the just-failed review item immediately.', profile_id, review_plan=ReviewPlan('startpos', item.review_item_id, item.position_key, item.position_fen_normalized, tuple(item.predecessor_path), RoutingSource.IMMEDIATE_RETRY.value))
