from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass
import hashlib

from .models import ReviewPlan, RoutingDecision, RoutingSource, UrgencyTier, due_state


@dataclass(frozen=True)
class RoutingConfig:
    corpus_share: float = 0.8
    review_share: float = 0.2
    due_baseline_corpus_share: float = 0.2
    boosted_corpus_penalty: float = 0.01
    extreme_corpus_penalty: float = 0.02
    per_item_recent_window: int = 10
    per_item_share_cap: float = 0.3


class ReviewRouter:
    def __init__(self, config: RoutingConfig | None = None):
        self.config = config or RoutingConfig()
        self.recent_item_ids = deque(maxlen=self.config.per_item_recent_window)
        self.turn_counter = 0

    @staticmethod
    def _tier_rank(item) -> int:
        if item.urgency_tier == UrgencyTier.EXTREME.value:
            return 0
        if item.urgency_tier == UrgencyTier.BOOSTED.value:
            return 1
        return 2

    @staticmethod
    def _stable_fraction(profile_id: str, turn_counter: int) -> float:
        token = f'{profile_id}:{turn_counter}'.encode('utf-8')
        digest = hashlib.sha256(token).hexdigest()[:12]
        return int(digest, 16) / float(16 ** 12)

    def select(self, profile_id: str, items: list) -> RoutingDecision:
        self.turn_counter += 1
        due_items = [item for item in items if due_state(item.due_at_utc) == 'due']
        if not due_items:
            return RoutingDecision(
                RoutingSource.CORPUS.value,
                None,
                None,
                'none_due',
                False,
                'ordinary_corpus_play selected: no due review items; corpus_share=0.80; review_share=0.20; boosted_due_count=0; extreme_due_count=0.',
                profile_id,
                corpus_share=self.config.corpus_share,
                review_share=self.config.review_share,
                boosted_due_count=0,
                extreme_due_count=0,
            )

        boosted_due_count = sum(1 for item in due_items if item.urgency_tier == UrgencyTier.BOOSTED.value)
        extreme_due_count = sum(1 for item in due_items if item.urgency_tier == UrgencyTier.EXTREME.value)
        corpus_share = max(
            0.0,
            self.config.due_baseline_corpus_share
            - (self.config.boosted_corpus_penalty * boosted_due_count)
            - (self.config.extreme_corpus_penalty * extreme_due_count),
        )
        review_share = max(0.0, 1.0 - corpus_share)

        due_items.sort(key=lambda item: (self._tier_rank(item), item.due_at_utc, -item.consecutive_failures, item.last_seen_at_utc, item.review_item_id))
        counts = Counter(self.recent_item_ids)
        selected = due_items[0]
        for item in due_items:
            if counts[item.review_item_id] / max(1, len(self.recent_item_ids)) <= self.config.per_item_share_cap:
                selected = item
                break

        use_review = self._stable_fraction(profile_id, self.turn_counter) < review_share
        if not use_review:
            return RoutingDecision(
                RoutingSource.CORPUS.value,
                None,
                None,
                'due_items_waiting',
                False,
                f'ordinary_corpus_play selected: due review exists; corpus_share={corpus_share:.2f}; review_share={review_share:.2f}; boosted_due_count={boosted_due_count}; extreme_due_count={extreme_due_count}; selected_routing_reason=ordinary_corpus_play.',
                profile_id,
                corpus_share=corpus_share,
                review_share=review_share,
                boosted_due_count=boosted_due_count,
                extreme_due_count=extreme_due_count,
            )

        self.recent_item_ids.append(selected.review_item_id)
        if selected.urgency_tier == UrgencyTier.EXTREME.value:
            source = RoutingSource.EXTREME.value
        elif selected.urgency_tier == UrgencyTier.BOOSTED.value:
            source = RoutingSource.BOOSTED_REVIEW.value
        else:
            source = RoutingSource.SCHEDULED_REVIEW.value
        plan = ReviewPlan(
            root_fen='startpos',
            target_review_item_id=selected.review_item_id,
            target_position_key=selected.position_key,
            target_fen=selected.position_fen_normalized,
            predecessor_path=tuple(selected.predecessor_path),
            routing_reason=source,
        )
        return RoutingDecision(
            source,
            selected.review_item_id,
            selected.urgency_tier,
            due_state(selected.due_at_utc),
            bool(selected.predecessor_path),
            f'{source} selected: corpus_share={corpus_share:.2f}; review_share={review_share:.2f}; boosted_due_count={boosted_due_count}; extreme_due_count={extreme_due_count}; selected_urgency_tier={selected.urgency_tier}; selected_routing_reason={source}.',
            profile_id,
            review_plan=plan,
            corpus_share=corpus_share,
            review_share=review_share,
            boosted_due_count=boosted_due_count,
            extreme_due_count=extreme_due_count,
        )

    def immediate_retry(self, profile_id: str, item) -> RoutingDecision:
        self.recent_item_ids.append(item.review_item_id)
        return RoutingDecision(RoutingSource.IMMEDIATE_RETRY.value, item.review_item_id, item.urgency_tier, 'immediate', bool(item.predecessor_path), 'Retrying the just-failed review item immediately.', profile_id, review_plan=ReviewPlan('startpos', item.review_item_id, item.position_key, item.position_fen_normalized, tuple(item.predecessor_path), RoutingSource.IMMEDIATE_RETRY.value))
