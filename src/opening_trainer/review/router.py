from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from math import floor

from .models import HijackStage, ReviewPlan, RoutingDecision, RoutingSource, UrgencyTier, due_state


CATEGORY_PRIORITY = ('E', 'B', 'H80', 'H60', 'H40', 'H20', 'D', 'C')
REVIEW_CATEGORIES = ('D', 'H80', 'H60', 'H40', 'H20', 'B', 'E')
H_CATEGORIES = ('H80', 'H60', 'H40', 'H20')
CATEGORY_TO_ROUTING = {
    'D': RoutingSource.SCHEDULED_REVIEW.value,
    'B': RoutingSource.BOOSTED_REVIEW.value,
    'E': RoutingSource.EXTREME.value,
    'H80': RoutingSource.HIJACK_REENTRY.value,
    'H60': RoutingSource.HIJACK_REENTRY.value,
    'H40': RoutingSource.HIJACK_REENTRY.value,
    'H20': RoutingSource.HIJACK_REENTRY.value,
}
HIJACK_PASS_RULES = {
    HijackStage.H80.value: {5},
    HijackStage.H60.value: {1, 5},
    HijackStage.H40.value: {1, 3, 5},
    HijackStage.H20.value: {1, 3, 4, 5},
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
        self.prev_due_ids: dict[str, set[str]] = {category: set() for category in REVIEW_CATEGORIES}
        self.addition_counters: dict[str, int] = {'D': 0, 'B': 0}
        self.removal_counters: dict[str, int] = {'D': 0, 'B': 0}
        self.tier_queues: dict[str, deque[str]] = {category: deque() for category in REVIEW_CATEGORIES}
        self.srs_due_queue: deque[str] = deque()
        self.deck_counts: dict[str, int] = {category: 0 for category in CATEGORY_PRIORITY[::-1]}
        self.last_shares: tuple[float, float] = (1.0, 0.0)

    @staticmethod
    def _queue_sort_key(item) -> tuple[str, str, str]:
        return (item.srs_next_due_at_utc, item.last_seen_at_utc, item.review_item_id)

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
        return min(0.55, 0.20 + 0.05 * max(0, total_due - 1))

    def _compute_shares(self, d: int, h80: int, h60: int, h40: int, h20: int, b: int, e: int) -> dict[str, float]:
        total_due = d + h80 + h60 + h40 + h20 + b + e
        if total_due == 0:
            return {category: (1.0 if category == 'C' else 0.0) for category in ('C', *REVIEW_CATEGORIES)} | {'corpus': 1.0, 'review': 0.0}
        review_share = self._ordinary_penalty(total_due)
        corpus_share = 1.0 - review_share
        masses = {
            'D': 1 * d,
            'H80': 1 * h80,
            'H60': 1 * h60,
            'H40': 1 * h40,
            'H20': 1 * h20,
            'B': 2 * b,
            'E': 4 * e,
        }
        mass_total = sum(masses.values())
        if mass_total == 0:
            return {category: (1.0 if category == 'C' else 0.0) for category in ('C', *REVIEW_CATEGORIES)} | {'corpus': 1.0, 'review': 0.0}
        out = {'C': corpus_share, 'corpus': corpus_share, 'review': review_share}
        for category, mass in masses.items():
            out[category] = review_share * mass / mass_total
        return out

    def _allocate_counts(self, shares: dict[str, float], n: int, counts_by_category: dict[str, int]) -> dict[str, int]:
        eligible = {'C': True} | {cat: counts_by_category.get(cat, 0) > 0 for cat in REVIEW_CATEGORIES}
        targets = {k: shares[k] * n for k in ('C', *REVIEW_CATEGORIES)}
        counts = {k: floor(targets[k]) if eligible[k] else 0 for k in targets}
        remaining = n - sum(counts.values())
        while remaining > 0:
            candidates = [k for k in ('C', *REVIEW_CATEGORIES) if eligible[k]]
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
            for category in ('C', *REVIEW_CATEGORIES):
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
        out = {k: 0 for k in ('C', *REVIEW_CATEGORIES)}
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
        for category in REVIEW_CATEGORIES:
            queue = self.tier_queues[category]
            allowed = ids_by_tier[category]
            retained = [item_id for item_id in queue if item_id in allowed]
            queue.clear()
            queue.extend(retained)
            existing = set(queue)
            for item in tier_items[category]:
                if item.review_item_id not in existing:
                    queue.append(item.review_item_id)

    def _sync_srs_queue(self, srs_due_items: list) -> None:
        allowed = {item.review_item_id for item in srs_due_items}
        retained = [item_id for item_id in self.srs_due_queue if item_id in allowed]
        self.srs_due_queue.clear()
        self.srs_due_queue.extend(retained)
        existing = set(self.srs_due_queue)
        for item in sorted(srs_due_items, key=self._queue_sort_key):
            if item.review_item_id not in existing:
                self.srs_due_queue.append(item.review_item_id)

    def _track_state_changes(self, ids_by_tier: dict[str, set[str]]) -> None:
        for cat in REVIEW_CATEGORIES:
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

    def _rebuild_deck(self, counts_by_category: dict[str, int]) -> None:
        shares = self._compute_shares(
            counts_by_category.get('D', 0),
            counts_by_category.get('H80', 0),
            counts_by_category.get('H60', 0),
            counts_by_category.get('H40', 0),
            counts_by_category.get('H20', 0),
            counts_by_category.get('B', 0),
            counts_by_category.get('E', 0),
        )
        n = self._deck_size(sum(counts_by_category.get(cat, 0) for cat in REVIEW_CATEGORIES))
        counts = self._allocate_counts(shares, n, counts_by_category)
        counts = self._enforce_minimum_tier_representation(counts, counts_by_category)
        deck = self._build_deck(counts, n)
        deck = self._choose_rotation(deck)
        first_review_category = next((token for token in deck if token in REVIEW_CATEGORIES), None)
        if first_review_category and self.last_emitted_exact_item_id and self.tier_queues[first_review_category]:
            if self.tier_queues[first_review_category][0] == self.last_emitted_exact_item_id:
                self.tier_queues[first_review_category].rotate(-1)
        self.deck = DeckState(tokens=deck, index=0)
        self.deck_counts = counts
        self.last_shares = (shares['corpus'], shares['review'])
        self.addition_counters = {'D': 0, 'B': 0}
        self.removal_counters = {'D': 0, 'B': 0}

    def _enforce_minimum_tier_representation(self, counts: dict[str, int], due_counts: dict[str, int]) -> dict[str, int]:
        adjusted = counts.copy()
        non_empty = {cat: due_counts.get(cat, 0) > 0 for cat in REVIEW_CATEGORIES}
        tiers = [tier for tier, ok in non_empty.items() if ok]
        review_total = sum(adjusted[cat] for cat in REVIEW_CATEGORIES)
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
        donors = [tier for tier in ('E', 'B', 'H80', 'H60', 'H40', 'H20', 'D') if counts[tier] > 1]
        if not donors:
            return None
        max_count = max(counts[tier] for tier in donors)
        for tier in ('E', 'B', 'H80', 'H60', 'H40', 'H20', 'D'):
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
        self.last_emitted_exact_item_id = item_id if category in REVIEW_CATEGORIES else None

    @staticmethod
    def _hijack_category_for_stage(stage: str) -> str | None:
        return {'h80': 'H80', 'h60': 'H60', 'h40': 'H40', 'h20': 'H20'}.get(stage)

    @staticmethod
    def _advance_hijack_ticker(item) -> int:
        item.hijack_pass_ticker += 1
        if item.hijack_pass_ticker == 6:
            item.hijack_pass_ticker = 1
        return item.hijack_pass_ticker

    @staticmethod
    def _ticker_is_pass(stage: str, ticker: int) -> bool:
        return ticker in HIJACK_PASS_RULES.get(stage, set())

    def activate_hijack_from_due(self, item) -> None:
        if item.urgency_tier != UrgencyTier.ORDINARY.value:
            return
        item.hijack_stage = HijackStage.H80.value
        item.hijack_pass_ticker = 0
        item.dormant = False
        item.avoidance_count += 1
        item.last_hijack_routing_source = 'HIJACK_ACTIVATED_H80'
        try:
            self.tier_queues['D'].remove(item.review_item_id)
        except ValueError:
            pass
        if item.review_item_id not in self.tier_queues['H80']:
            self.tier_queues['H80'].append(item.review_item_id)
        self.pending_rebuild_trigger = 'HIJACK_ACTIVATED_H80'

    def resolve_hijack_no_anchor(self, item) -> None:
        category = self._hijack_category_for_stage(item.hijack_stage)
        if category is None:
            return
        queue = self.tier_queues[category]
        try:
            queue.remove(item.review_item_id)
        except ValueError:
            pass
        queue.append(item.review_item_id)
        item.last_hijack_routing_source = 'HIJACK_NO_ANCHOR'

    def resolve_hijack_target_reached(self, item) -> None:
        for cat in H_CATEGORIES:
            try:
                self.tier_queues[cat].remove(item.review_item_id)
            except ValueError:
                pass
        item.hijack_stage = HijackStage.NONE.value
        item.hijack_pass_ticker = 0
        item.dormant = False
        item.last_hijack_routing_source = 'HIJACK_TARGET_REACHED'
        self.pending_rebuild_trigger = 'HIJACK_TARGET_REACHED'

    def resolve_hijack_miss_decay(self, item) -> str:
        old_category = self._hijack_category_for_stage(item.hijack_stage)
        if old_category:
            try:
                self.tier_queues[old_category].remove(item.review_item_id)
            except ValueError:
                pass
        item.avoidance_count += 1
        stage_order = [HijackStage.H80.value, HijackStage.H60.value, HijackStage.H40.value, HijackStage.H20.value, HijackStage.DORMANT.value]
        current_index = stage_order.index(item.hijack_stage) if item.hijack_stage in stage_order else 0
        item.hijack_stage = stage_order[min(current_index + 1, len(stage_order) - 1)]
        item.last_hijack_routing_source = 'HIJACK_MISS_DECAY'
        if item.hijack_stage == HijackStage.DORMANT.value:
            item.dormant = True
            self.pending_rebuild_trigger = 'HIJACK_DORMANT'
            return item.hijack_stage
        item.dormant = False
        new_category = self._hijack_category_for_stage(item.hijack_stage)
        if new_category:
            self.tier_queues[new_category].append(item.review_item_id)
        self.pending_rebuild_trigger = 'HIJACK_MISS_DECAY'
        return item.hijack_stage

    def revive_dormant(self, item) -> None:
        if not item.dormant:
            return
        item.hijack_stage = HijackStage.H80.value
        item.dormant = False
        item.hijack_pass_ticker = 0
        item.avoidance_count = 0
        item.last_hijack_routing_source = 'HIJACK_REVIVED'
        self.tier_queues['H80'].append(item.review_item_id)
        self.pending_rebuild_trigger = 'HIJACK_REVIVED'

    def select(self, profile_id: str, items: list) -> RoutingDecision:
        manual_targets = [
            item for item in items
            if item.origin_kind == 'manual_target'
            and due_state(item.due_at_utc) == 'due'
            and not item.frequency_retired_for_current_due_cycle
        ]
        if manual_targets:
            selected_item = sorted(manual_targets, key=self._queue_sort_key)[0]
            plan = ReviewPlan(
                root_fen='startpos' if selected_item.predecessor_path else selected_item.position_fen_normalized,
                target_review_item_id=selected_item.review_item_id,
                target_position_key=selected_item.position_key,
                target_fen=selected_item.position_fen_normalized,
                predecessor_path=tuple(selected_item.predecessor_path),
                routing_reason=RoutingSource.MANUAL_TARGET.value,
            )
            return RoutingDecision(
                RoutingSource.MANUAL_TARGET.value,
                selected_item.review_item_id,
                selected_item.urgency_tier,
                due_state(selected_item.due_at_utc),
                bool(selected_item.predecessor_path),
                'manual_target selected ahead of ordinary review/corpus routing.',
                profile_id,
                review_plan=plan,
                corpus_share=self.last_shares[0],
                review_share=self.last_shares[1],
                due_count=0,
                boosted_due_count=0,
                extreme_due_count=0,
                deck_size=len(self.deck.tokens),
                token_counts=self.deck_counts.copy(),
                selected_token_category='MANUAL',
                queue_position_before=0,
                queue_position_after=0,
                rebuild_trigger=None,
            )

        srs_due_items = [item for item in items if due_state(item.srs_next_due_at_utc) == 'due' and not item.frequency_retired_for_current_due_cycle]
        self._sync_srs_queue(srs_due_items)
        if self.srs_due_queue:
            item_id = self.srs_due_queue.popleft()
            self.srs_due_queue.append(item_id)
            selected_item = next(item for item in srs_due_items if item.review_item_id == item_id)
            plan = ReviewPlan(
                root_fen='startpos',
                target_review_item_id=selected_item.review_item_id,
                target_position_key=selected_item.position_key,
                target_fen=selected_item.position_fen_normalized,
                predecessor_path=tuple(selected_item.predecessor_path),
                routing_reason=RoutingSource.SRS_DUE_REVIEW.value,
            )
            return RoutingDecision(
                RoutingSource.SRS_DUE_REVIEW.value,
                selected_item.review_item_id,
                selected_item.urgency_tier,
                due_state(selected_item.srs_next_due_at_utc),
                bool(selected_item.predecessor_path),
                'srs_due_review selected before finite review deck.',
                profile_id,
                review_plan=plan,
                corpus_share=self.last_shares[0],
                review_share=self.last_shares[1],
                due_count=0,
                boosted_due_count=0,
                extreme_due_count=0,
                deck_size=len(self.deck.tokens),
                token_counts=self.deck_counts.copy(),
                selected_token_category='SRS',
                queue_position_before=0,
                queue_position_after=len(self.srs_due_queue) - 1,
                rebuild_trigger=None,
            )

        srs_due_ids = {item.review_item_id for item in srs_due_items}
        pressure_items = [item for item in items if item.review_item_id not in srs_due_ids and not item.frequency_retired_for_current_due_cycle]
        tier_items = {
            'D': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.ORDINARY.value and i.hijack_stage == HijackStage.NONE.value and due_state(i.due_at_utc) == 'due' and not i.dormant], key=self._queue_sort_key),
            'H80': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.ORDINARY.value and i.hijack_stage == HijackStage.H80.value and not i.dormant], key=self._queue_sort_key),
            'H60': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.ORDINARY.value and i.hijack_stage == HijackStage.H60.value and not i.dormant], key=self._queue_sort_key),
            'H40': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.ORDINARY.value and i.hijack_stage == HijackStage.H40.value and not i.dormant], key=self._queue_sort_key),
            'H20': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.ORDINARY.value and i.hijack_stage == HijackStage.H20.value and not i.dormant], key=self._queue_sort_key),
            'B': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.BOOSTED.value and due_state(i.due_at_utc) == 'due'], key=self._queue_sort_key),
            'E': sorted([i for i in pressure_items if i.urgency_tier == UrgencyTier.EXTREME.value and due_state(i.due_at_utc) == 'due'], key=self._queue_sort_key),
        }
        ids_by_tier = {category: {item.review_item_id for item in bucket} for category, bucket in tier_items.items()}
        self._sync_queues(tier_items, ids_by_tier)
        self._track_state_changes(ids_by_tier)
        counts_by_category = {category: len(tier_items[category]) for category in REVIEW_CATEGORIES}
        due_count = counts_by_category['D']
        boosted_count, extreme_count = counts_by_category['B'], counts_by_category['E']
        total_due = due_count + boosted_count + extreme_count

        rebuild_trigger: str | None = None
        if self.pending_rebuild_trigger is not None:
            rebuild_trigger = self.pending_rebuild_trigger
            self.pending_rebuild_trigger = None
            self._rebuild_deck(counts_by_category)
        elif self.deck.exhausted:
            rebuild_trigger = 'deck_exhausted'
            self._rebuild_deck(counts_by_category)

        token = self.deck.next_token() if self.deck.tokens else 'C'
        selected_item = None
        selected_category = token
        queue_before = None
        queue_after = None

        if token in REVIEW_CATEGORIES and not self.tier_queues[token]:
            for category in ('E', 'B', 'H80', 'H60', 'H40', 'H20', 'D', 'C'):
                if category == 'C' or self.tier_queues.get(category):
                    selected_category = category
                    break

        if selected_category in REVIEW_CATEGORIES:
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
                queue.append(item_id)
            else:
                item_id = queue.popleft()
                queue.append(item_id)

            queue_after = len(queue) - 1
            selected_item = next(item for item in tier_items[selected_category] if item.review_item_id == item_id)
            if selected_category == 'D':
                selected_item.skipped_review_slots = 0
            if token in REVIEW_CATEGORIES:
                self._increment_due_skipped_slots(tier_items['D'], selected_item.review_item_id if selected_category == 'D' else None)

            if selected_category in H_CATEGORIES:
                ticker = self._advance_hijack_ticker(selected_item)
                selected_item.last_hijack_routing_source = 'HIJACK_TOKEN_SELECTED'
                if self._ticker_is_pass(selected_item.hijack_stage, ticker):
                    selected_item.last_hijack_routing_source = 'HIJACK_DECAY_PASS'
                    self._update_boundary('C', None)
                    return RoutingDecision(
                        RoutingSource.HIJACK_DECAY_PASS.value,
                        None,
                        None,
                        'due_items_waiting',
                        False,
                        f'hijack_decay_pass token={selected_category} ticker={ticker}; resolved as corpus play.',
                        profile_id,
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
