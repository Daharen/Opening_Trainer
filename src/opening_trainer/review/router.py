from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from math import floor

from .models import ReviewPlan, RoutingDecision, RoutingSource, UrgencyTier, due_state


CATEGORY_PRIORITY = ('E', 'B', 'D', 'C')
TIER_TO_CATEGORY = {
    UrgencyTier.ORDINARY.value: 'D',
    UrgencyTier.BOOSTED.value: 'B',
    UrgencyTier.EXTREME.value: 'E',
}
CATEGORY_TO_ROUTING = {
    'D': RoutingSource.SCHEDULED_REVIEW.value,
    'B': RoutingSource.BOOSTED_REVIEW.value,
    'E': RoutingSource.EXTREME.value,
}
ORDINARY_DUE_STARVATION_THRESHOLD = 8


@dataclass(frozen=True)
class RoutingConfig:
    corpus_share: float = 1.0
    review_share: float = 0.0


@dataclass
class DeckState:
    tokens: list[str] = field(default_factory=list)
    index: int = 0

    @property
    def exhausted(self) -> bool:
        return self.index >= len(self.tokens)

    def next_token(self) -> str:
        token = self.tokens[self.index]
        self.index += 1
        return token


class ReviewRouter:
    def __init__(self, config: RoutingConfig | None = None):
        self.config = config or RoutingConfig()
        self.deck = DeckState()
        self.last_emitted_category: str | None = None
        self.last_emitted_exact_item_id: str | None = None
        self.trailing_run_length: int = 0
        self.pending_rebuild_trigger: str | None = None
        self.prev_due_ids: dict[str, set[str]] = {'D': set(), 'B': set(), 'E': set()}
        self.addition_counters: dict[str, int] = {'D': 0, 'B': 0}
        self.removal_counters: dict[str, int] = {'D': 0, 'B': 0}
        self.tier_queues: dict[str, deque[str]] = {'D': deque(), 'B': deque(), 'E': deque()}
        self.deck_counts: dict[str, int] = {'C': 0, 'D': 0, 'B': 0, 'E': 0}
        self.last_shares: tuple[float, float] = (1.0, 0.0)

    @staticmethod
    def _queue_sort_key(item) -> tuple[str, str, str]:
        return (item.due_at_utc, item.last_seen_at_utc, item.review_item_id)

    @staticmethod
    def _deck_size(total_due: int) -> int:
        if total_due < 5:
            return 20
        if total_due < 10:
            return 40
        return 80

    @staticmethod
    def _ordinary_penalty(total_due: int) -> float:
        if total_due == 0:
            return 0.0
        return min(0.80, 0.20 + 0.15 * int(total_due >= 2) + 0.10 * int(total_due >= 3) + 0.05 * max(0, total_due - 3))

    def _compute_shares(self, d: int, b: int, e: int) -> dict[str, float]:
        total_due = d + b + e
        if total_due == 0:
            return {'C': 1.0, 'D': 0.0, 'B': 0.0, 'E': 0.0, 'corpus': 1.0, 'review': 0.0}
        ordinary_penalty = self._ordinary_penalty(total_due)
        boosted_penalty = min(0.06, 0.02 * b)
        extreme_penalty = min(0.04, 0.01 * e)
        review_share = ordinary_penalty + boosted_penalty + extreme_penalty
        corpus_share = 1.0 - review_share
        mass_d, mass_b, mass_e = d, 2 * b, 4 * e
        mass_total = mass_d + mass_b + mass_e
        if mass_total == 0:
            return {'C': 1.0, 'D': 0.0, 'B': 0.0, 'E': 0.0, 'corpus': 1.0, 'review': 0.0}
        return {
            'C': corpus_share,
            'D': review_share * mass_d / mass_total,
            'B': review_share * mass_b / mass_total,
            'E': review_share * mass_e / mass_total,
            'corpus': corpus_share,
            'review': review_share,
        }

    def _allocate_counts(self, shares: dict[str, float], n: int, d: int, b: int, e: int) -> dict[str, int]:
        eligible = {'C': True, 'D': d > 0, 'B': b > 0, 'E': e > 0}
        targets = {k: shares[k] * n for k in ('C', 'D', 'B', 'E')}
        counts = {k: floor(targets[k]) if eligible[k] else 0 for k in targets}
        remaining = n - sum(counts.values())
        while remaining > 0:
            candidates = [k for k in ('C', 'D', 'B', 'E') if eligible[k]]
            chosen = max(candidates, key=lambda k: (targets[k] - counts[k], -CATEGORY_PRIORITY.index(k)))
            counts[chosen] += 1
            remaining -= 1
        return counts

    def _build_deck(self, counts: dict[str, int], n: int) -> list[str]:
        placed = {k: 0 for k in counts}
        remaining = counts.copy()
        result: list[str] = []
        for i in range(n):
            prev = result[-1] if result else None
            deficits: list[tuple[float, str]] = []
            for category in ('C', 'D', 'B', 'E'):
                if remaining[category] <= 0:
                    continue
                target_prefix = ((i + 1) * counts[category]) / n
                deficit = target_prefix - placed[category]
                deficits.append((deficit, category))
            max_deficit = max(deficits, key=lambda row: row[0])[0]
            tied = [category for deficit, category in deficits if abs(deficit - max_deficit) < 1e-12]
            if prev is not None:
                not_prev = [category for category in tied if category != prev]
                if not_prev:
                    tied = not_prev
            chosen = min(tied, key=lambda category: CATEGORY_PRIORITY.index(category))
            result.append(chosen)
            placed[chosen] += 1
            remaining[chosen] -= 1
        return result

    @staticmethod
    def _self_run_max(tokens: list[str]) -> dict[str, int]:
        n = len(tokens)
        doubled = tokens + tokens
        out = {k: 0 for k in ('C', 'D', 'B', 'E')}
        if n == 0:
            return out
        run_cat = doubled[0]
        run_len = 1
        for token in doubled[1:]:
            if token == run_cat:
                run_len += 1
            else:
                out[run_cat] = max(out[run_cat], min(run_len, n))
                run_cat = token
                run_len = 1
        out[run_cat] = max(out[run_cat], min(run_len, n))
        return out

    @staticmethod
    def _rotate(tokens: list[str], index: int) -> list[str]:
        return tokens[index:] + tokens[:index]

    @staticmethod
    def _prefix_run(tokens: list[str], category: str) -> int:
        run = 0
        for token in tokens:
            if token == category:
                run += 1
            else:
                break
        return run

    def _choose_rotation(self, tokens: list[str]) -> list[str]:
        if not tokens or self.last_emitted_category is None:
            return tokens
        n = len(tokens)
        self_run = self._self_run_max(tokens)
        last = self.last_emitted_category
        valid: list[int] = []
        scored: list[tuple[int, int]] = []
        for rotation in range(n):
            candidate = self._rotate(tokens, rotation)
            prefix = self._prefix_run(candidate, last)
            overflow = max(0, self.trailing_run_length + prefix - self_run[last])
            scored.append((overflow, rotation))
            if overflow == 0:
                valid.append(rotation)
        if valid:
            return self._rotate(tokens, min(valid))
        scored.sort()
        return self._rotate(tokens, scored[0][1])

    def _sync_queues(self, tier_items: dict[str, list], ids_by_tier: dict[str, set[str]]) -> None:
        for category in ('D', 'B', 'E'):
            queue = self.tier_queues[category]
            allowed = ids_by_tier[category]
            retained = [item_id for item_id in queue if item_id in allowed]
            queue.clear()
            queue.extend(retained)
            existing = set(queue)
            for item in tier_items[category]:
                if item.review_item_id not in existing:
                    queue.append(item.review_item_id)

    def _track_state_changes(self, ids_by_tier: dict[str, set[str]]) -> None:
        for cat in ('D', 'B', 'E'):
            added = ids_by_tier[cat] - self.prev_due_ids[cat]
            removed = self.prev_due_ids[cat] - ids_by_tier[cat]
            if cat == 'E' and (added or removed):
                self.pending_rebuild_trigger = 'extreme_state_change'
            if cat in self.addition_counters:
                self.addition_counters[cat] += len(added)
                self.removal_counters[cat] += len(removed)
        if self.pending_rebuild_trigger is None:
            if self.addition_counters['B'] >= 2:
                self.pending_rebuild_trigger = 'boosted_additions_threshold'
            elif self.addition_counters['D'] >= 3:
                self.pending_rebuild_trigger = 'due_additions_threshold'
            elif self.removal_counters['B'] >= 2:
                self.pending_rebuild_trigger = 'boosted_removals_threshold'
            elif self.removal_counters['D'] >= 3:
                self.pending_rebuild_trigger = 'due_removals_threshold'
        self.prev_due_ids = {k: set(v) for k, v in ids_by_tier.items()}

    def _rebuild_deck(self, d: int, b: int, e: int) -> None:
        shares = self._compute_shares(d, b, e)
        n = self._deck_size(d + b + e)
        counts = self._allocate_counts(shares, n, d, b, e)
        counts = self._enforce_minimum_tier_representation(counts, d, b, e)
        deck = self._build_deck(counts, n)
        deck = self._choose_rotation(deck)
        first_review_category = next((token for token in deck if token in ('D', 'B', 'E')), None)
        if first_review_category and self.last_emitted_exact_item_id and self.tier_queues[first_review_category]:
            if self.tier_queues[first_review_category][0] == self.last_emitted_exact_item_id:
                self.tier_queues[first_review_category].rotate(-1)
        self.deck = DeckState(tokens=deck, index=0)
        self.deck_counts = counts
        self.last_shares = (shares['corpus'], shares['review'])
        self.addition_counters = {'D': 0, 'B': 0}
        self.removal_counters = {'D': 0, 'B': 0}

    def _enforce_minimum_tier_representation(self, counts: dict[str, int], d: int, b: int, e: int) -> dict[str, int]:
        adjusted = counts.copy()
        non_empty = {'D': d > 0, 'B': b > 0, 'E': e > 0}
        tiers = [tier for tier, ok in non_empty.items() if ok]
        review_total = adjusted['D'] + adjusted['B'] + adjusted['E']
        if review_total < len(tiers):
            return adjusted
        forced = False
        while True:
            zero_tiers = [tier for tier in tiers if adjusted[tier] == 0]
            if not zero_tiers:
                break
            donor = self._choose_min_representation_donor(adjusted)
            if donor is None:
                break
            receiver = zero_tiers[0]
            adjusted[donor] -= 1
            adjusted[receiver] += 1
            forced = True
        if forced:
            self.pending_rebuild_trigger = 'MIN_TIER_REPRESENTATION_FORCED'
        return adjusted

    @staticmethod
    def _choose_min_representation_donor(counts: dict[str, int]) -> str | None:
        donors = [tier for tier in ('E', 'B', 'D') if counts[tier] > 1]
        if not donors:
            return None
        max_count = max(counts[tier] for tier in donors)
        for tier in ('E', 'B', 'D'):
            if tier in donors and counts[tier] == max_count:
                return tier
        return None

    @staticmethod
    def _ordinary_due_starved_items(items: list) -> list:
        return [item for item in items if item.skipped_review_slots >= ORDINARY_DUE_STARVATION_THRESHOLD]

    @staticmethod
    def _starved_sort_key(item) -> tuple[int, str, str, str]:
        return (-item.skipped_review_slots, item.due_at_utc, item.last_seen_at_utc, item.review_item_id)

    def _increment_due_skipped_slots(self, due_items: list, selected_item_id: str | None) -> None:
        for item in due_items:
            if item.review_item_id == selected_item_id:
                continue
            item.skipped_review_slots += 1

    def _update_boundary(self, category: str, item_id: str | None) -> None:
        if self.last_emitted_category == category:
            self.trailing_run_length += 1
        else:
            self.trailing_run_length = 1
        self.last_emitted_category = category
        self.last_emitted_exact_item_id = item_id if category in ('D', 'B', 'E') else None

    def select(self, profile_id: str, items: list) -> RoutingDecision:
        due_items = [item for item in items if due_state(item.due_at_utc) == 'due' and not item.frequency_retired_for_current_due_cycle]
        tier_items = {
            'D': sorted([i for i in due_items if i.urgency_tier == UrgencyTier.ORDINARY.value], key=self._queue_sort_key),
            'B': sorted([i for i in due_items if i.urgency_tier == UrgencyTier.BOOSTED.value], key=self._queue_sort_key),
            'E': sorted([i for i in due_items if i.urgency_tier == UrgencyTier.EXTREME.value], key=self._queue_sort_key),
        }
        ids_by_tier = {category: {item.review_item_id for item in bucket} for category, bucket in tier_items.items()}
        self._sync_queues(tier_items, ids_by_tier)
        self._track_state_changes(ids_by_tier)
        due_count, boosted_count, extreme_count = len(tier_items['D']), len(tier_items['B']), len(tier_items['E'])
        total_due = due_count + boosted_count + extreme_count

        rebuild_trigger: str | None = None
        if self.pending_rebuild_trigger is not None:
            rebuild_trigger = self.pending_rebuild_trigger
            self.pending_rebuild_trigger = None
            self._rebuild_deck(due_count, boosted_count, extreme_count)
        elif self.deck.exhausted:
            rebuild_trigger = 'deck_exhausted'
            self._rebuild_deck(due_count, boosted_count, extreme_count)

        token = self.deck.next_token() if self.deck.tokens else 'C'
        selected_item = None
        selected_category = token
        queue_before = None
        queue_after = None

        if token in ('D', 'B', 'E') and not self.tier_queues[token]:
            fallback = {'E': ('B', 'D', 'C'), 'B': ('D', 'C'), 'D': ('C',)}
            for category in fallback[token]:
                if category == 'C' or self.tier_queues[category]:
                    selected_category = category
                    break

        if selected_category in ('D', 'B', 'E'):
            queue = self.tier_queues[selected_category]
            queue_before = 0
            if selected_category == 'D':
                starved = self._ordinary_due_starved_items(tier_items['D'])
                if starved:
                    chosen = sorted(starved, key=self._starved_sort_key)[0]
                    item_id = chosen.review_item_id
                    try:
                        queue.remove(item_id)
                    except ValueError:
                        pass
                    rebuild_trigger = rebuild_trigger or 'ORDINARY_DUE_STARVATION_BUMP'
                else:
                    item_id = queue.popleft()
            else:
                item_id = queue.popleft()
            queue.append(item_id)
            queue_after = len(queue) - 1
            selected_item = next(item for item in tier_items[selected_category] if item.review_item_id == item_id)
            if selected_category == 'D':
                selected_item.skipped_review_slots = 0
            if token in ('D', 'B', 'E'):
                self._increment_due_skipped_slots(tier_items['D'], selected_item.review_item_id if selected_category == 'D' else None)
            routing_source = CATEGORY_TO_ROUTING[selected_category]
            plan = ReviewPlan(
                root_fen='startpos',
                target_review_item_id=selected_item.review_item_id,
                target_position_key=selected_item.position_key,
                target_fen=selected_item.position_fen_normalized,
                predecessor_path=tuple(selected_item.predecessor_path),
                routing_reason=routing_source,
            )
            self._update_boundary(selected_category, selected_item.review_item_id)
            return RoutingDecision(
                routing_source,
                selected_item.review_item_id,
                selected_item.urgency_tier,
                due_state(selected_item.due_at_utc),
                bool(selected_item.predecessor_path),
                f'{routing_source} selected via deterministic deck token={selected_category}.',
                profile_id,
                review_plan=plan,
                corpus_share=self.last_shares[0],
                review_share=self.last_shares[1],
                due_count=due_count,
                boosted_due_count=boosted_count,
                extreme_due_count=extreme_count,
                deck_size=len(self.deck.tokens),
                token_counts=self.deck_counts.copy(),
                selected_token_category=selected_category,
                queue_position_before=queue_before,
                queue_position_after=queue_after,
                rebuild_trigger=rebuild_trigger,
            )

        self._update_boundary('C', None)
        return RoutingDecision(
            RoutingSource.CORPUS.value,
            None,
            None,
            'none_due' if total_due == 0 else 'due_items_waiting',
            False,
            'ordinary_corpus_play selected via deterministic deck token=C.',
            profile_id,
            corpus_share=self.last_shares[0],
            review_share=self.last_shares[1],
            due_count=due_count,
            boosted_due_count=boosted_count,
            extreme_due_count=extreme_count,
            deck_size=len(self.deck.tokens),
            token_counts=self.deck_counts.copy(),
            selected_token_category='C',
            queue_position_before=None,
            queue_position_after=None,
            rebuild_trigger=rebuild_trigger,
        )

    def immediate_retry(self, profile_id: str, item) -> RoutingDecision:
        return RoutingDecision(
            RoutingSource.IMMEDIATE_RETRY.value,
            item.review_item_id,
            item.urgency_tier,
            'immediate',
            bool(item.predecessor_path),
            'Retrying the just-failed review item immediately.',
            profile_id,
            review_plan=ReviewPlan('startpos', item.review_item_id, item.position_key, item.position_fen_normalized, tuple(item.predecessor_path), RoutingSource.IMMEDIATE_RETRY.value),
        )

    def stubborn_extreme_repeat(self, profile_id: str, item) -> RoutingDecision:
        return RoutingDecision(
            RoutingSource.STUBBORN_EXTREME_REPEAT.value,
            item.review_item_id,
            item.urgency_tier,
            'immediate',
            bool(item.predecessor_path),
            'Forced bounded stubborn-extreme repeat outside ordinary deck flow.',
            profile_id,
            review_plan=ReviewPlan('startpos', item.review_item_id, item.position_key, item.position_fen_normalized, tuple(item.predecessor_path), RoutingSource.STUBBORN_EXTREME_REPEAT.value),
        )
