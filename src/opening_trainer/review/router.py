from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from math import floor

from opening_trainer.session_logging import log_line

from .models import (
    HijackStage,
    ManualPresentationMode,
    ReviewPlan,
    RoutingDecision,
    RoutingSource,
    UrgencyTier,
    due_state,
)


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
PRESSURE_TIER_DEFAULT_CAPACITY = {'D': 5, 'B': 3, 'E': 2}
PRESSURE_TIERS = ('D', 'B', 'E')
PRESSURE_ROUTING_TO_CATEGORY = {
    RoutingSource.SCHEDULED_REVIEW.value: 'D',
    RoutingSource.BOOSTED_REVIEW.value: 'B',
    RoutingSource.EXTREME.value: 'E',
}
PRESSURE_CATEGORY_TO_LAYERS = {
    'D': (0,),
    'B': (0, 1),
    'E': (0, 1, 2, 3),
}


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


@dataclass
class PressureTierControllerState:
    capacity: int
    active_deck: list[str] = field(default_factory=list)
    waiting_queue: list[str] = field(default_factory=list)
    round_seen_count: int = 0
    round_miss_count: int = 0
    round_target_size: int = 0
    round_started_saturated: bool = False
    active_insert_serials: dict[str, int] = field(default_factory=dict)
    next_insert_serial: int = 1


@dataclass
class StableReviewDeckState:
    cards: list[tuple[int, str]] = field(default_factory=list)
    cursor: int = 0
    anchor_serials: dict[str, int] = field(default_factory=dict)
    next_anchor_serial: int = 1
    last_mutation_reason: str = 'startup'
    last_routing_action: str = 'not_started'
    last_card_consumed_item_id: str | None = None
    last_corpus_step: str | None = None


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
        self.last_share_breakdown: dict[str, int] = {
            'due_active': 0,
            'boosted_active': 0,
            'urgent_active': 0,
            'due_total': 0,
            'boosted_total': 0,
            'urgent_total': 0,
            'due_equivalent': 0,
            'boosted_equivalent': 0,
            'urgent_equivalent': 0,
            'due_pct': 0,
            'boosted_pct': 0,
            'urgent_pct': 0,
            'training_pct': 0,
            'corpus_pct': 100,
        }
        self.profile_pressure_state: dict[str, dict[str, PressureTierControllerState]] = {}
        self.profile_review_decks: dict[str, StableReviewDeckState] = {}

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
    def _due_training_pct(count: int) -> int:
        if count <= 0:
            return 0
        pct = 20
        if count >= 2:
            pct += 15
        if count >= 3:
            pct += 10
        if count >= 4:
            pct += 5 * (count - 3)
        return min(80, pct)

    @staticmethod
    def _boosted_training_pct(count: int) -> int:
        return 2 * min(3, max(0, count))

    @staticmethod
    def _urgent_training_pct(count: int) -> int:
        return min(4, max(0, count))

    def _compute_training_and_corpus_share_from_pressure_counts(
        self,
        due_count: int,
        boosted_count: int,
        urgent_count: int,
        *,
        active_due_count: int | None = None,
        active_boosted_count: int | None = None,
        active_urgent_count: int | None = None,
    ) -> tuple[float, float, dict[str, int]]:
        breakdown = self._compute_share_breakdown(
            due_count=due_count,
            boosted_count=boosted_count,
            urgent_count=urgent_count,
            active_due_count=active_due_count,
            active_boosted_count=active_boosted_count,
            active_urgent_count=active_urgent_count,
        )
        return (breakdown['corpus_pct'] / 100.0, breakdown['training_pct'] / 100.0, breakdown)

    def _compute_share_breakdown(
        self,
        *,
        due_count: int,
        boosted_count: int,
        urgent_count: int,
        active_due_count: int | None = None,
        active_boosted_count: int | None = None,
        active_urgent_count: int | None = None,
    ) -> dict[str, int]:
        due_equivalent = due_count + boosted_count + urgent_count
        boosted_equivalent = boosted_count + urgent_count
        urgent_equivalent = urgent_count

        due_pct = self._due_training_pct(due_equivalent)
        boosted_pct = self._boosted_training_pct(boosted_equivalent)
        urgent_pct = self._urgent_training_pct(urgent_equivalent)
        training_pct = due_pct + boosted_pct + urgent_pct
        training_pct = min(90, training_pct)
        corpus_pct = 100 - training_pct
        due_active = due_count if active_due_count is None else active_due_count
        boosted_active = boosted_count if active_boosted_count is None else active_boosted_count
        urgent_active = urgent_count if active_urgent_count is None else active_urgent_count
        return {
            'due_active': due_active,
            'boosted_active': boosted_active,
            'urgent_active': urgent_active,
            'due_total': due_count,
            'boosted_total': boosted_count,
            'urgent_total': urgent_count,
            'due_equivalent': due_equivalent,
            'boosted_equivalent': boosted_equivalent,
            'urgent_equivalent': urgent_equivalent,
            'due_pct': due_pct,
            'boosted_pct': boosted_pct,
            'urgent_pct': urgent_pct,
            'training_pct': training_pct,
            'corpus_pct': corpus_pct,
        }

    def _compute_shares(
        self,
        d: int,
        h80: int,
        h60: int,
        h40: int,
        h20: int,
        b: int,
        e: int,
        *,
        active_due_count: int | None = None,
        active_boosted_count: int | None = None,
        active_urgent_count: int | None = None,
    ) -> dict[str, object]:
        due_count = d + h80 + h60 + h40 + h20
        boosted_count = b
        urgent_count = e
        total_due = due_count + boosted_count + urgent_count
        if total_due == 0:
            return {category: (1.0 if category == 'C' else 0.0) for category in ('C', *REVIEW_CATEGORIES)} | {
                'corpus': 1.0,
                'review': 0.0,
                'share_breakdown': self._compute_share_breakdown(
                    due_count=0,
                    boosted_count=0,
                    urgent_count=0,
                    active_due_count=active_due_count,
                    active_boosted_count=active_boosted_count,
                    active_urgent_count=active_urgent_count,
                ),
            }
        corpus_share, review_share, breakdown = self._compute_training_and_corpus_share_from_pressure_counts(
            due_count=due_count,
            boosted_count=boosted_count,
            urgent_count=urgent_count,
            active_due_count=active_due_count,
            active_boosted_count=active_boosted_count,
            active_urgent_count=active_urgent_count,
        )
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
            return {category: (1.0 if category == 'C' else 0.0) for category in ('C', *REVIEW_CATEGORIES)} | {
                'corpus': 1.0,
                'review': 0.0,
                'share_breakdown': self._compute_share_breakdown(
                    due_count=due_count,
                    boosted_count=boosted_count,
                    urgent_count=urgent_count,
                    active_due_count=active_due_count,
                    active_boosted_count=active_boosted_count,
                    active_urgent_count=active_urgent_count,
                ),
            }
        out = {'C': corpus_share, 'corpus': corpus_share, 'review': review_share, 'share_breakdown': breakdown}
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
            if category in PRESSURE_TIERS:
                continue
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

    def _rebuild_deck(
        self,
        counts_by_category: dict[str, int],
        *,
        active_counts_by_category: dict[str, int] | None = None,
    ) -> None:
        active_counts = active_counts_by_category or {}
        shares = self._compute_shares(
            counts_by_category.get('D', 0),
            counts_by_category.get('H80', 0),
            counts_by_category.get('H60', 0),
            counts_by_category.get('H40', 0),
            counts_by_category.get('H20', 0),
            counts_by_category.get('B', 0),
            counts_by_category.get('E', 0),
            active_due_count=active_counts.get('D'),
            active_boosted_count=active_counts.get('B'),
            active_urgent_count=active_counts.get('E'),
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
        self.last_share_breakdown = dict(shares.get('share_breakdown', self.last_share_breakdown))
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

    def _ensure_review_deck(self, profile_id: str) -> StableReviewDeckState:
        if profile_id not in self.profile_review_decks:
            self.profile_review_decks[profile_id] = StableReviewDeckState()
        return self.profile_review_decks[profile_id]

    @staticmethod
    def _tier_layers(category: str) -> tuple[int, ...]:
        return PRESSURE_CATEGORY_TO_LAYERS.get(category, ())

    def _ensure_anchor(self, deck: StableReviewDeckState, review_item_id: str) -> int:
        serial = deck.anchor_serials.get(review_item_id)
        if serial is not None:
            return serial
        serial = deck.next_anchor_serial
        deck.next_anchor_serial += 1
        deck.anchor_serials[review_item_id] = serial
        return serial

    def _insert_card(self, deck: StableReviewDeckState, layer: int, review_item_id: str) -> None:
        anchor = self._ensure_anchor(deck, review_item_id)
        insert_at = len(deck.cards)
        for idx, (existing_layer, existing_item_id) in enumerate(deck.cards):
            if existing_layer > layer:
                insert_at = idx
                break
            if existing_layer == layer and deck.anchor_serials.get(existing_item_id, 0) > anchor:
                insert_at = idx
                break
        deck.cards.insert(insert_at, (layer, review_item_id))
        if insert_at < deck.cursor:
            deck.cursor += 1

    def _remove_card(self, deck: StableReviewDeckState, layer: int, review_item_id: str) -> None:
        for idx, (existing_layer, existing_item_id) in enumerate(deck.cards):
            if existing_layer == layer and existing_item_id == review_item_id:
                deck.cards.pop(idx)
                if idx < deck.cursor:
                    deck.cursor -= 1
                elif idx == deck.cursor and deck.cursor >= len(deck.cards):
                    deck.cursor = 0 if deck.cards else 0
                return

    def _record_deck_mutation(self, profile_id: str, reason: str) -> None:
        deck = self._ensure_review_deck(profile_id)
        deck.last_mutation_reason = reason
        log_line(f'REVIEW_DECK_MUTATION profile={profile_id} reason={reason}', tag='review')

    def _set_active_membership(self, deck: StableReviewDeckState, review_item_id: str, old_category: str | None, new_category: str | None) -> None:
        old_layers = set(self._tier_layers(old_category or ''))
        new_layers = set(self._tier_layers(new_category or ''))
        if new_layers:
            self._ensure_anchor(deck, review_item_id)
        for layer in sorted(old_layers - new_layers):
            self._remove_card(deck, layer, review_item_id)
        for layer in sorted(new_layers - old_layers):
            self._insert_card(deck, layer, review_item_id)
        if not new_layers:
            deck.anchor_serials.pop(review_item_id, None)
        if not deck.cards:
            deck.cursor = 0

    @staticmethod
    def _next_newest_active(state: PressureTierControllerState) -> str | None:
        if not state.active_deck:
            return None
        return max(state.active_deck, key=lambda item_id: state.active_insert_serials.get(item_id, 0))

    def export_profile_state(self, profile_id: str) -> dict[str, dict]:
        profile_state = self.profile_pressure_state.get(profile_id, {})
        exported = {
            category: {
                'capacity': state.capacity,
                'active_deck': list(state.active_deck),
                'waiting_queue': list(state.waiting_queue),
                'round_seen_count': state.round_seen_count,
                'round_miss_count': state.round_miss_count,
                'round_target_size': state.round_target_size,
                'round_started_saturated': state.round_started_saturated,
                'active_insert_serials': dict(state.active_insert_serials),
                'next_insert_serial': state.next_insert_serial,
            }
            for category, state in profile_state.items()
        }
        stable_deck = self.profile_review_decks.get(profile_id)
        if stable_deck is not None:
            exported['stable_review_deck'] = {
                'cards': [{'layer': layer, 'review_item_id': item_id} for layer, item_id in stable_deck.cards],
                'cursor': stable_deck.cursor,
                'anchor_serials': dict(stable_deck.anchor_serials),
                'next_anchor_serial': stable_deck.next_anchor_serial,
                'last_mutation_reason': stable_deck.last_mutation_reason,
                'last_routing_action': stable_deck.last_routing_action,
                'last_card_consumed_item_id': stable_deck.last_card_consumed_item_id,
                'last_corpus_step': stable_deck.last_corpus_step,
            }
        return exported

    def import_profile_state(self, profile_id: str, payload: dict[str, dict] | None) -> None:
        state: dict[str, PressureTierControllerState] = {}
        for category in PRESSURE_TIERS:
            row = (payload or {}).get(category, {})
            capacity = max(2, int(row.get('capacity', PRESSURE_TIER_DEFAULT_CAPACITY[category])))
            controller = PressureTierControllerState(
                capacity=capacity,
                active_deck=list(row.get('active_deck', [])),
                waiting_queue=list(row.get('waiting_queue', [])),
                round_seen_count=max(0, int(row.get('round_seen_count', 0))),
                round_miss_count=max(0, int(row.get('round_miss_count', 0))),
                round_target_size=max(0, int(row.get('round_target_size', 0))),
                round_started_saturated=bool(row.get('round_started_saturated', False)),
                active_insert_serials={str(k): int(v) for k, v in dict(row.get('active_insert_serials', {})).items()},
                next_insert_serial=max(1, int(row.get('next_insert_serial', 1))),
            )
            state[category] = controller
        self.profile_pressure_state[profile_id] = state
        deck_payload = dict((payload or {}).get('stable_review_deck', {}))
        cards_payload = list(deck_payload.get('cards', []))
        cards: list[tuple[int, str]] = []
        for row in cards_payload:
            if not isinstance(row, dict):
                continue
            cards.append((int(row.get('layer', 0)), str(row.get('review_item_id', ''))))
        stable_deck = StableReviewDeckState(
            cards=[(layer, item_id) for layer, item_id in cards if item_id],
            cursor=max(0, int(deck_payload.get('cursor', 0))),
            anchor_serials={str(k): int(v) for k, v in dict(deck_payload.get('anchor_serials', {})).items()},
            next_anchor_serial=max(1, int(deck_payload.get('next_anchor_serial', 1))),
            last_mutation_reason=str(deck_payload.get('last_mutation_reason', 'migration_or_load')),
            last_routing_action=str(deck_payload.get('last_routing_action', 'not_started')),
            last_card_consumed_item_id=deck_payload.get('last_card_consumed_item_id'),
            last_corpus_step=deck_payload.get('last_corpus_step'),
        )
        if not stable_deck.cards:
            for category in ('D', 'B', 'E'):
                for item_id in state[category].active_deck:
                    self._set_active_membership(stable_deck, item_id, None, category)
        if stable_deck.cursor >= len(stable_deck.cards):
            stable_deck.cursor = 0 if stable_deck.cards else 0
        self.profile_review_decks[profile_id] = stable_deck
        for category in PRESSURE_TIERS:
            self._ensure_tier_fill_invariant(profile_id, category, state[category], mutation_reason='fill_vacancy_from_waiting_on_load')

    def clear_profile_state(self, profile_id: str) -> None:
        self.profile_pressure_state.pop(profile_id, None)
        self.profile_review_decks.pop(profile_id, None)

    def _ensure_pressure_state(self, profile_id: str) -> dict[str, PressureTierControllerState]:
        if profile_id not in self.profile_pressure_state:
            self.profile_pressure_state[profile_id] = {
                category: PressureTierControllerState(capacity=PRESSURE_TIER_DEFAULT_CAPACITY[category])
                for category in PRESSURE_TIERS
            }
        self._ensure_review_deck(profile_id)
        return self.profile_pressure_state[profile_id]

    def _sync_pressure_state(self, profile_id: str, pressure_state: dict[str, PressureTierControllerState], tier_items: dict[str, list]) -> None:
        stable_deck = self._ensure_review_deck(profile_id)
        old_active_membership = {
            item_id: category
            for category in PRESSURE_TIERS
            for item_id in pressure_state[category].active_deck
        }
        target_membership = {
            item.review_item_id: category
            for category in PRESSURE_TIERS
            for item in tier_items[category]
        }
        for category in PRESSURE_TIERS:
            state = pressure_state[category]
            allowed_ids = {item.review_item_id for item in tier_items[category]}
            for item_id in list(state.active_deck):
                if item_id not in allowed_ids:
                    state.active_deck.remove(item_id)
                    state.active_insert_serials.pop(item_id, None)
                    destination = target_membership.get(item_id)
                    if destination not in PRESSURE_TIERS:
                        self._set_active_membership(stable_deck, item_id, category, None)
                    self._record_deck_mutation(profile_id, 'remove_ineligible_active_item')
            state.waiting_queue = [item_id for item_id in state.waiting_queue if item_id in allowed_ids and item_id not in state.active_deck]
            if state.round_target_size > len(state.active_deck):
                state.round_target_size = len(state.active_deck)
            if state.round_seen_count > state.round_target_size:
                state.round_seen_count = state.round_target_size

        movers = [
            item_id
            for item_id, old_category in old_active_membership.items()
            if target_membership.get(item_id) in PRESSURE_TIERS and target_membership.get(item_id) != old_category
        ]
        for item_id in movers:
            destination = target_membership[item_id]
            dest_state = pressure_state[destination]
            if item_id in dest_state.active_deck:
                continue
            if len(dest_state.active_deck) >= dest_state.capacity:
                newest = self._next_newest_active(dest_state)
                if newest is not None and newest != item_id:
                    dest_state.active_deck.remove(newest)
                    dest_state.active_insert_serials.pop(newest, None)
                    dest_state.waiting_queue.insert(0, newest)
                    self._set_active_membership(stable_deck, newest, destination, None)
                    self._record_deck_mutation(profile_id, 'privileged_cross_tier_displace_newest')
            if len(dest_state.active_deck) < dest_state.capacity:
                dest_state.active_deck.append(item_id)
                dest_state.active_insert_serials[item_id] = dest_state.next_insert_serial
                dest_state.next_insert_serial += 1
                self._set_active_membership(stable_deck, item_id, old_active_membership[item_id], destination)
                self._record_deck_mutation(profile_id, 'privileged_cross_tier_insert')

        for category in PRESSURE_TIERS:
            state = pressure_state[category]
            existing = set(state.active_deck) | set(state.waiting_queue)
            for item in tier_items[category]:
                item_id = item.review_item_id
                if item_id in existing:
                    continue
                if len(state.active_deck) < state.capacity:
                    state.active_deck.append(item_id)
                    state.active_insert_serials[item_id] = state.next_insert_serial
                    state.next_insert_serial += 1
                    self._set_active_membership(stable_deck, item_id, None, category)
                    self._record_deck_mutation(profile_id, 'admit_new_item_into_active')
                else:
                    state.waiting_queue.append(item_id)
                existing.add(item_id)
            self._ensure_tier_fill_invariant(profile_id, category, state, mutation_reason='fill_vacancy_from_waiting')

    def _ensure_tier_fill_invariant(
        self,
        profile_id: str,
        category: str,
        state: PressureTierControllerState,
        *,
        mutation_reason: str,
    ) -> None:
        stable_deck = self._ensure_review_deck(profile_id)
        while len(state.active_deck) < state.capacity and state.waiting_queue:
            promoted = state.waiting_queue.pop(0)
            state.active_deck.append(promoted)
            state.active_insert_serials[promoted] = state.next_insert_serial
            state.next_insert_serial += 1
            self._set_active_membership(stable_deck, promoted, None, category)
            self._record_deck_mutation(profile_id, mutation_reason)

    def _finalize_round_if_complete(self, profile_id: str, category: str, state: PressureTierControllerState) -> None:
        if state.round_target_size <= 0 or state.round_seen_count < state.round_target_size:
            return
        if state.round_miss_count == 0:
            if state.round_started_saturated:
                state.capacity += 1
                self._record_deck_mutation(profile_id, 'capacity_growth')
            else:
                self._record_deck_mutation(profile_id, 'capacity_growth_blocked_underfilled_round')
        elif state.round_miss_count >= 2:
            state.capacity = max(2, state.capacity - 1)
            self._record_deck_mutation(profile_id, 'capacity_shrink')
        stable_deck = self._ensure_review_deck(profile_id)
        while len(state.active_deck) > state.capacity:
            newest = max(state.active_deck, key=lambda item_id: state.active_insert_serials.get(item_id, 0))
            state.active_deck.remove(newest)
            state.active_insert_serials.pop(newest, None)
            state.waiting_queue.insert(0, newest)
            self._set_active_membership(stable_deck, newest, category, None)
            self._record_deck_mutation(profile_id, 'capacity_shrink_evict_newest')
        self._ensure_tier_fill_invariant(profile_id, category, state, mutation_reason='fill_vacancy_from_waiting')
        state.round_seen_count = 0
        state.round_miss_count = 0
        state.round_target_size = 0
        state.round_started_saturated = False

    def record_presented_review(self, profile_id: str, category: str, review_item_id: str) -> None:
        if category not in PRESSURE_TIERS:
            return
        state = self._ensure_pressure_state(profile_id)[category]
        if review_item_id not in state.active_deck:
            return
        if state.round_target_size == 0:
            state.round_target_size = len(state.active_deck)
            state.round_started_saturated = len(state.active_deck) >= state.capacity
        state.round_seen_count += 1

    def record_review_result(self, profile_id: str, routing_source: str, was_miss: bool) -> None:
        category = PRESSURE_ROUTING_TO_CATEGORY.get(routing_source)
        if category is None:
            return
        state = self._ensure_pressure_state(profile_id)[category]
        if state.round_target_size == 0:
            return
        if was_miss:
            state.round_miss_count += 1
        self._finalize_round_if_complete(profile_id, category, state)

    def select(self, profile_id: str, items: list) -> RoutingDecision:
        stable_deck = self._ensure_review_deck(profile_id)
        manual_targets = [
            item for item in items
            if item.origin_kind == 'manual_target'
            and due_state(item.due_at_utc) == 'due'
            and not item.frequency_retired_for_current_due_cycle
        ]
        if manual_targets:
            selected_item = sorted(manual_targets, key=self._queue_sort_key)[0]
            presentation_mode = selected_item.manual_presentation_mode or ManualPresentationMode.PLAY_TO_POSITION.value
            use_force_start = presentation_mode in {
                ManualPresentationMode.FORCE_TARGET_START.value,
                ManualPresentationMode.MANUAL_SETUP_START.value,
            }
            plan = ReviewPlan(
                root_fen=selected_item.position_fen_normalized if use_force_start else 'startpos',
                target_review_item_id=selected_item.review_item_id,
                target_position_key=selected_item.position_key,
                target_fen=selected_item.position_fen_normalized,
                predecessor_path=tuple(selected_item.predecessor_path),
                routing_reason=RoutingSource.MANUAL_TARGET.value,
            )
            stable_deck.last_routing_action = RoutingSource.MANUAL_TARGET.value
            stable_deck.last_card_consumed_item_id = selected_item.review_item_id
            stable_deck.last_corpus_step = None
            return RoutingDecision(
                RoutingSource.MANUAL_TARGET.value,
                selected_item.review_item_id,
                selected_item.urgency_tier,
                due_state(selected_item.due_at_utc),
                bool(selected_item.predecessor_path),
                (
                    f'manual_target selected ahead of ordinary review/corpus routing; '
                    f'presentation_mode={presentation_mode}; forced_color={selected_item.manual_forced_player_color}.'
                ),
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
            stable_deck.last_routing_action = RoutingSource.SRS_DUE_REVIEW.value
            stable_deck.last_card_consumed_item_id = selected_item.review_item_id
            stable_deck.last_corpus_step = None
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
        pressure_state = self._ensure_pressure_state(profile_id)
        self._sync_pressure_state(profile_id, pressure_state, tier_items)
        for category in PRESSURE_TIERS:
            self.tier_queues[category] = deque(pressure_state[category].active_deck)
        ids_by_tier = {category: {item.review_item_id for item in bucket} for category, bucket in tier_items.items()}
        self._sync_queues(tier_items, ids_by_tier)
        self._track_state_changes(ids_by_tier)
        counts_by_category = {category: len(tier_items[category]) for category in REVIEW_CATEGORIES}
        active_due_count = len(pressure_state['D'].active_deck)
        active_boosted_count = len(pressure_state['B'].active_deck)
        active_urgent_count = len(pressure_state['E'].active_deck)
        due_count = counts_by_category['D']
        boosted_count, extreme_count = counts_by_category['B'], counts_by_category['E']
        total_due = due_count + boosted_count + extreme_count
        active_counts_by_category = {'D': active_due_count, 'B': active_boosted_count, 'E': active_urgent_count}

        rebuild_trigger: str | None = None
        if self.pending_rebuild_trigger is not None:
            rebuild_trigger = self.pending_rebuild_trigger
            self.pending_rebuild_trigger = None
            self._rebuild_deck(counts_by_category, active_counts_by_category=active_counts_by_category)
        elif self.deck.exhausted:
            rebuild_trigger = 'deck_exhausted'
            self._rebuild_deck(counts_by_category, active_counts_by_category=active_counts_by_category)

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
            if selected_category in PRESSURE_TIERS:
                item_id = None
                if stable_deck.cards:
                    attempts = 0
                    while attempts < len(stable_deck.cards):
                        if stable_deck.cursor >= len(stable_deck.cards):
                            stable_deck.cursor = 0
                        layer, candidate_item_id = stable_deck.cards[stable_deck.cursor]
                        stable_deck.cursor = (stable_deck.cursor + 1) % len(stable_deck.cards)
                        attempts += 1
                        if not (
                            candidate_item_id in pressure_state['E'].active_deck
                            or candidate_item_id in pressure_state['B'].active_deck
                            or candidate_item_id in pressure_state['D'].active_deck
                        ):
                            continue
                        if token in PRESSURE_TIERS:
                            if candidate_item_id in pressure_state[token].active_deck:
                                item_id = candidate_item_id
                                break
                            continue
                        item_id = candidate_item_id
                        break
                if item_id is None:
                    if token in PRESSURE_TIERS:
                        active_union = list(pressure_state[token].active_deck)
                    else:
                        active_union = pressure_state['E'].active_deck + pressure_state['B'].active_deck + pressure_state['D'].active_deck
                    if active_union:
                        item_id = active_union[0]
                if item_id is None:
                    selected_category = 'C'
                    queue_before = None
                    queue_after = None
                else:
                    if item_id in pressure_state['E'].active_deck:
                        selected_category = 'E'
                    elif item_id in pressure_state['B'].active_deck:
                        selected_category = 'B'
                    else:
                        selected_category = 'D'
                    queue_before = 0
                    queue_after = 0
            else:
                queue = self.tier_queues[selected_category]
                queue_before = 0
                item_id = None
            if selected_category == 'D':
                starved = self._ordinary_due_starved_items(tier_items['D'])
                if starved:
                    chosen = sorted(starved, key=self._starved_sort_key)[0]
                    item_id = chosen.review_item_id
                    rebuild_trigger = rebuild_trigger or 'ORDINARY_DUE_STARVATION_BUMP'
                elif item_id is None:
                    item_id = queue.popleft()
                    queue.append(item_id)
            elif selected_category in REVIEW_CATEGORIES and selected_category not in PRESSURE_TIERS:
                item_id = queue.popleft()
                queue.append(item_id)

            if selected_category not in PRESSURE_TIERS and selected_category in REVIEW_CATEGORIES:
                queue_after = len(queue) - 1
            selected_item = next(item for item in tier_items[selected_category] if item.review_item_id == item_id)
            if selected_category == 'D':
                selected_item.skipped_review_slots = 0
            if token in REVIEW_CATEGORIES:
                self._increment_due_skipped_slots(tier_items['D'], selected_item.review_item_id if selected_category == 'D' else None)
            if selected_category in PRESSURE_TIERS:
                self.record_presented_review(profile_id, selected_category, selected_item.review_item_id)
                self.tier_queues[selected_category] = deque(pressure_state[selected_category].active_deck)

            if selected_category in H_CATEGORIES:
                ticker = self._advance_hijack_ticker(selected_item)
                selected_item.last_hijack_routing_source = 'HIJACK_TOKEN_SELECTED'
                if self._ticker_is_pass(selected_item.hijack_stage, ticker):
                    selected_item.last_hijack_routing_source = 'HIJACK_DECAY_PASS'
                    self._update_boundary('C', None)
                    stable_deck.last_routing_action = RoutingSource.HIJACK_DECAY_PASS.value
                    stable_deck.last_card_consumed_item_id = None
                    stable_deck.last_corpus_step = 'hijack_decay_pass'
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
            stable_deck.last_routing_action = routing_source
            stable_deck.last_card_consumed_item_id = selected_item.review_item_id
            stable_deck.last_corpus_step = None
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
        stable_deck.last_routing_action = RoutingSource.CORPUS.value
        stable_deck.last_card_consumed_item_id = None
        stable_deck.last_corpus_step = 'corpus_move_emitted'
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
