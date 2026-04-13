from __future__ import annotations

import random
import time
import json
from dataclasses import dataclass, replace
from dataclasses import asdict
from pathlib import Path
from typing import Callable

import chess

from .board import GameBoard
from .bundle_corpus import normalize_builder_position_key
from .corpus import load_artifact
from .developer_timing import DeveloperTimingOverrideState, DeveloperTimingOverrideStore, LiveTimingDebugState, parse_overlay_key_dimensions
from .evaluation import CanonicalJudgment, EngineAuthority, EvaluatorConfig, OpeningBookAuthority, ReasonCode, format_evaluation_feedback
from .practical_risk_reconciled import PracticalRiskReconciledService
from .evaluator import MoveEvaluator
from .models import EvaluationResult, MoveHistoryEntry, SessionOutcome, SessionState, SessionView
from .opening_names import OpeningNameDataset
from .opening_locked_mode import (
    OpeningLockedModeState,
    OpeningLockedProvider,
    OpeningLockedSessionState,
    OpeningTransitionClassification,
)
from .opponent import OpponentProvider
from .review.models import (
    ManualForcedPlayerColor,
    ManualPresentationMode,
    ReviewItem,
    ReviewItemOrigin,
    ReviewPathMove,
    RoutingDecision,
    RoutingSource,
    utc_now_iso,
)
from .review.manual_target import create_manual_target_item, validate_manual_target
from .review.profile_service import ProfileService
from .review.router import ReviewRouter
from .review.scheduler import apply_failure, apply_success, sync_due_cycle_transition
from .review.storage import ReviewStorage
from .runtime import (
    RuntimeContext,
    RuntimeOverrides,
    bundle_retained_ply_depth_from_metadata,
    inspect_corpus_bundle,
    load_runtime_config,
    max_supported_player_moves_from_retained_plies,
)
from .settings import CONSERVATIVE_FALLBACK_MAX_DEPTH, MANUAL_MODE, SMART_PROFILE_MODE, TrainerSettings, TrainerSettingsStore
from .session_events import build_event, event_to_dict
from .session_logging import log_line
from .smart_profile import SmartProfileService
from .timing import (
    DynamicTimingContext,
    TimingContext,
    bucket_clock_pressure,
    bucket_opening_ply_band,
    bucket_prev_opp_think,
    fallback_keys_for_context,
    fallback_keys_for_dynamic_context,
)


@dataclass
class TimedSessionState:
    time_control_id: str
    initial_seconds: float
    increment_seconds: float
    white_remaining_ms: int
    black_remaining_ms: int
    previous_player_think_seconds: float | None = None
    previous_opponent_think_seconds: float | None = None


@dataclass
class PendingOpponentAction:
    board_before: chess.Board
    choice: object
    native_components: dict[str, object] | None
    adjusted_components: dict[str, object] | None
    effective_key: str | None
    fallback_keys_attempted: tuple[str, ...]
    review_predecessor_bypassed: bool
    visible_delay_seconds: float


class TrainingSession:
    restart_delay_ms = 900
    opponent_visible_delay_min_seconds = 0.15
    opponent_visible_delay_max_seconds = 2.5
    opponent_visible_delay_speed_multiplier = 1.0
    premove_execution_time_cost_seconds = 0.1

    def __init__(self, runtime_context: RuntimeContext | None = None, mode: str = 'cli', review_storage: ReviewStorage | None = None):
        self.runtime_context = runtime_context or load_runtime_config(RuntimeOverrides())
        self.mode = mode
        self.board = GameBoard()
        self.config = self.runtime_context.evaluator_config
        self.opponent = OpponentProvider(
            artifact_path=self.runtime_context.config.corpus_artifact_path,
            bundle_dir=self.runtime_context.config.corpus_bundle_dir,
            evaluator_config=self.config,
            rng=random,
            opponent_fallback_mode=self.runtime_context.config.opponent_fallback_mode,
        )
        time_control_id, _rating_band = self._timing_contract_metadata()
        self.practical_risk_reconciled = PracticalRiskReconciledService(
            self.runtime_context.config.practical_risk_reconciled_path,
            expected_time_control_id=time_control_id,
        )
        self.evaluator = MoveEvaluator(
            config=self.config,
            book_authority=OpeningBookAuthority(self.runtime_context.book.path if self.runtime_context.book.available else None),
            engine_authority=EngineAuthority(self.config),
            reconciled_service=self.practical_risk_reconciled,
        )
        self.required_player_moves = self.config.active_envelope_player_moves
        default_review_root = self.runtime_context.runtime_paths.profile_root
        self.review_storage = review_storage or ReviewStorage(default_review_root)
        self.profile_service = ProfileService(self.review_storage)
        self.router = ReviewRouter()
        self.active_profile_id = self.profile_service.get_active_profile_id()
        self.router.import_profile_state(self.active_profile_id, self.review_storage.load_router_state(self.active_profile_id))
        self.settings_store = TrainerSettingsStore(self.review_storage.root)
        self.developer_timing_store = DeveloperTimingOverrideStore(self.review_storage.root)
        self.smart_profile = SmartProfileService(self.review_storage, self.active_profile_id)
        self.player_color = chess.WHITE
        self.player_move_count = 0
        self.state = SessionState.IDLE
        self.last_evaluation: EvaluationResult | None = None
        self.last_outcome: SessionOutcome | None = None
        self.last_opponent_choice = None
        self.current_routing: RoutingDecision | None = None
        self.current_review_item_id: str | None = None
        self.active_review_plan = None
        self.run_path: list[ReviewPathMove] = []
        self.settings = self.settings_store.load(maximum_depth=self.max_supported_training_depth())
        runtime_fallback_mode = self.runtime_context.config.opponent_fallback_mode
        if runtime_fallback_mode and self.settings.opponent_fallback_mode == "current_bundle_only" and runtime_fallback_mode != "current_bundle_only":
            self.settings = TrainerSettings(
                good_moves_acceptable=self.settings.good_moves_acceptable,
                active_training_ply_depth=self.settings.active_training_ply_depth,
                smart_profile_enabled=self.settings.smart_profile_enabled,
                training_mode=self.settings.training_mode,
                opening_locked_mode_enabled=self.settings.opening_locked_mode_enabled,
                selected_opening_name=self.settings.selected_opening_name,
                selected_smart_track=self.settings.selected_smart_track,
                selected_time_control_id=self.settings.selected_time_control_id,
                side_panel_visible=self.settings.side_panel_visible,
                move_list_visible=self.settings.move_list_visible,
                dark_mode_enabled=self.settings.dark_mode_enabled,
                allow_sharp_gambit_lines=self.settings.allow_sharp_gambit_lines,
                training_panel_visible_columns=self.settings.training_panel_visible_columns,
                last_bundle_path=self.settings.last_bundle_path,
                last_corpus_catalog_root=self.settings.last_corpus_catalog_root,
                opponent_fallback_mode=runtime_fallback_mode,
                last_seen_installed_app_version=self.settings.last_seen_installed_app_version,
                last_seen_installed_build_id=self.settings.last_seen_installed_build_id,
            )
        opening_locked_status = getattr(self.runtime_context, "opening_locked_artifact", None)
        self.opening_locked_provider: OpeningLockedProvider | None = None
        if opening_locked_status is not None and opening_locked_status.loaded and opening_locked_status.sqlite_path is not None:
            self.opening_locked_provider = OpeningLockedProvider(opening_locked_status.sqlite_path)
        self.opening_locked_state = OpeningLockedSessionState()
        self.developer_timing_overrides = self.developer_timing_store.load()
        self.live_timing_debug_state = self._initial_timing_debug_state()
        self._apply_settings(self.settings)
        self.timed_state: TimedSessionState | None = None
        self._player_turn_started_at: float | None = None
        self.pending_opponent_action: PendingOpponentAction | None = None
        self._pending_smart_level_change: tuple[int, int] | None = None
        self.opening_name: str | None = None
        self.opening_name_frozen = False
        self.opening_names = OpeningNameDataset.load()
        self._review_deck_observers: list[Callable[[dict[str, object]], None]] = []
        self._review_deck_event_serial = 0
        self._inspector_last_mutation_reason = 'startup'
        self._inspector_last_routing_action = 'not_started'
        self._inspector_prev_pressure_state = self.router.export_profile_state(self.active_profile_id)
        dataset_status = self.opening_names.status()
        log_line(
            "GUI_OPENING_NAME_DATASET_STATUS "
            f"loaded={'yes' if dataset_status.loaded else 'no'}; "
            f"source={dataset_status.source_path or 'unresolved'}; "
            f"entries={dataset_status.entry_count}",
            tag='gui',
        )

    def register_review_deck_observer(self, callback: Callable[[dict[str, object]], None]) -> None:
        self._review_deck_observers.append(callback)

    def unregister_review_deck_observer(self, callback: Callable[[dict[str, object]], None]) -> None:
        self._review_deck_observers = [existing for existing in self._review_deck_observers if existing is not callback]

    def _notify_review_deck_observers(self, event_type: str, **payload: object) -> None:
        if not self._review_deck_observers:
            return
        next_serial = int(getattr(self, '_review_deck_event_serial', 0)) + 1
        self._review_deck_event_serial = next_serial
        event_payload = {'event_type': event_type, 'event_index': next_serial, 'snapshot': self.review_deck_inspector_snapshot()} | payload
        for callback in list(self._review_deck_observers):
            try:
                callback(event_payload)
            except Exception:
                continue

    def review_deck_inspector_snapshot(self) -> dict[str, object]:
        items = self._items()
        item_by_id = {item.review_item_id: item for item in items}
        pressure_state = self.router.export_profile_state(self.active_profile_id)
        due_active = list(pressure_state.get('D', {}).get('active_deck', []))
        boosted_active = list(pressure_state.get('B', {}).get('active_deck', []))
        urgent_active = list(pressure_state.get('E', {}).get('active_deck', []))
        ordered_active = due_active + boosted_active + urgent_active
        stable_deck = pressure_state.get('stable_review_deck', {})
        tier_map = {
            'due': pressure_state.get('D', {}),
            'boosted': pressure_state.get('B', {}),
            'urgent': pressure_state.get('E', {}),
        }
        live_card_counts: dict[str, int] = {}
        for card in stable_deck.get('cards', []):
            item_id = str(card.get('review_item_id', ''))
            if item_id:
                live_card_counts[item_id] = live_card_counts.get(item_id, 0) + 1
        deck_card_multiplicity = {'ordinary_review': 1, 'boosted_review': 2, 'extreme_urgency': 4}
        active_rows: list[dict[str, object]] = []
        for item_id in ordered_active:
            item = item_by_id.get(item_id)
            if item is None:
                continue
            active_rows.append(
                {
                    'review_item_id': item.review_item_id,
                    'position': item.position_key,
                    'frequency_state': item.frequency_state,
                    'deck_cards': live_card_counts.get(item.review_item_id, deck_card_multiplicity.get(item.urgency_tier, 1)),
                    'fails': item.consecutive_failures,
                    'success_streak': item.success_streak,
                    'tier': item.urgency_tier,
                }
            )
        return {
            'training_share': float(getattr(self.router, 'last_shares', (1.0, 0.0))[1]),
            'corpus_share': float(getattr(self.router, 'last_shares', (1.0, 0.0))[0]),
            'share_breakdown': dict(getattr(self.router, 'last_share_breakdown', {})),
            'active_rows': active_rows,
            'tiers': {
                tier: {
                    'active_members': list(state.get('active_deck', [])),
                    'waiting_members': list(state.get('waiting_queue', [])),
                    'capacity': int(state.get('capacity', 0)),
                    'round_seen_count': int(state.get('round_seen_count', 0)),
                    'round_miss_count': int(state.get('round_miss_count', 0)),
                }
                for tier, state in tier_map.items()
            },
            'stable_review_deck': {
                'cards': list(stable_deck.get('cards', [])),
                'cursor': stable_deck.get('cursor', getattr(getattr(self.router, 'deck', None), 'index', None)),
            },
            'waiting_sizes': {
                'due': len(tier_map['due'].get('waiting_queue', [])),
                'boosted': len(tier_map['boosted'].get('waiting_queue', [])),
                'urgent': len(tier_map['urgent'].get('waiting_queue', [])),
            },
            'summary': {
                'due_streak': tier_map['due'].get('round_seen_count', 0),
                'boosted_streak': tier_map['boosted'].get('round_seen_count', 0),
                'urgent_streak': tier_map['urgent'].get('round_seen_count', 0),
                'due_misses': tier_map['due'].get('round_miss_count', 0),
                'boosted_misses': tier_map['boosted'].get('round_miss_count', 0),
                'urgent_misses': tier_map['urgent'].get('round_miss_count', 0),
                'due_active': len(tier_map['due'].get('active_deck', [])),
                'boosted_active': len(tier_map['boosted'].get('active_deck', [])),
                'urgent_active': len(tier_map['urgent'].get('active_deck', [])),
                'due_capacity': tier_map['due'].get('capacity', 0),
                'boosted_capacity': tier_map['boosted'].get('capacity', 0),
                'urgent_capacity': tier_map['urgent'].get('capacity', 0),
                'due_underfill': int(tier_map['due'].get('capacity', 0)) - len(tier_map['due'].get('active_deck', [])),
                'boosted_underfill': int(tier_map['boosted'].get('capacity', 0)) - len(tier_map['boosted'].get('active_deck', [])),
                'urgent_underfill': int(tier_map['urgent'].get('capacity', 0)) - len(tier_map['urgent'].get('active_deck', [])),
                'deck_cursor': stable_deck.get('cursor', getattr(getattr(self.router, 'deck', None), 'index', None)),
                'last_mutation_reason': self._inspector_last_mutation_reason,
                'last_routing_action': self._inspector_last_routing_action,
            },
            'card_count_source': 'live_deck_cards' if live_card_counts else 'tier_multiplicity_fallback',
        }

    def _emit_pressure_state_changes(self) -> None:
        current = self.router.export_profile_state(self.active_profile_id)
        previous = self._inspector_prev_pressure_state or {}
        for category in ('D', 'B', 'E'):
            before = previous.get(category, {})
            after = current.get(category, {})
            if list(before.get('active_deck', [])) != list(after.get('active_deck', [])):
                self._notify_review_deck_observers('active_stack_snapshot_changed', category=category)
            if list(before.get('waiting_queue', [])) != list(after.get('waiting_queue', [])):
                self._notify_review_deck_observers('waiting_queue_snapshot_changed', category=category)
            if int(before.get('capacity', 0)) != int(after.get('capacity', 0)):
                self._notify_review_deck_observers('capacity_changed', category=category)
            before_active = set(before.get('active_deck', []))
            after_active = set(after.get('active_deck', []))
            for item_id in sorted(after_active - before_active):
                self._notify_review_deck_observers('item_entered_active', category=category, review_item_id=item_id)
            for item_id in sorted(before_active - after_active):
                self._notify_review_deck_observers('item_left_active', category=category, review_item_id=item_id)
            before_waiting = set(before.get('waiting_queue', []))
            after_waiting = set(after.get('waiting_queue', []))
            for item_id in sorted(after_waiting - before_waiting):
                self._notify_review_deck_observers('item_entered_waiting', category=category, review_item_id=item_id)
        self._inspector_prev_pressure_state = current

    def _select_routing_with_inspector(self, items: list[ReviewItem], *, reason: str) -> RoutingDecision:
        decision = self.router.select(self.active_profile_id, items)
        self._inspector_last_routing_action = decision.routing_source
        if decision.rebuild_trigger:
            self._inspector_last_mutation_reason = decision.rebuild_trigger
            self._notify_review_deck_observers('last_mutation_reason_updated', reason=decision.rebuild_trigger)
        else:
            self._inspector_last_mutation_reason = reason
        self._emit_pressure_state_changes()
        if decision.selected_review_item_id:
            self._notify_review_deck_observers(
                'training_card_consumed',
                review_item_id=decision.selected_review_item_id,
                routing_source=decision.routing_source,
            )
        else:
            self._notify_review_deck_observers('corpus_move_emitted', routing_source=decision.routing_source)
        self._notify_review_deck_observers('last_routing_action_updated', routing_source=decision.routing_source)
        return decision

    @property
    def timing_diagnostics(self) -> LiveTimingDebugState:
        return self.live_timing_debug_state

    def _initial_timing_debug_state(self) -> LiveTimingDebugState:
        provider = getattr(self.opponent, "bundle_provider", None)
        bundle = getattr(provider, "bundle", None)
        return LiveTimingDebugState(
            bundle_path=str(self.runtime_context.config.corpus_bundle_dir) if self.runtime_context.config.corpus_bundle_dir else None,
            overlay_available=bool(getattr(bundle, "timing_overlay_available", False)),
            overlay_source=self._normalize_overlay_source(getattr(bundle, "overlay_source", "absent")),
            lookup_mode=str(getattr(bundle, "timing_lookup_mode", "full_key")),
            bundle_invariant_time_control_id=getattr(bundle, "bundle_invariant_time_control_id", None),
            bundle_invariant_rating_band=getattr(bundle, "bundle_invariant_rating_band", None),
        )

    def max_supported_training_depth(self) -> int:
        contract = self._canonical_corpus_contract()
        max_supported_player_moves = contract.get("max_supported_player_moves")
        if isinstance(max_supported_player_moves, int) and max_supported_player_moves >= 2:
            return max_supported_player_moves
        retained_ply_depth = self.bundle_retained_ply_depth()
        supported_depth = max_supported_player_moves_from_retained_plies(retained_ply_depth)
        if supported_depth is not None:
            return supported_depth
        artifact_path = self.runtime_context.config.corpus_artifact_path
        if artifact_path:
            try:
                return max(2, int(load_artifact(artifact_path).retained_ply_depth) // 2)
            except Exception:
                return CONSERVATIVE_FALLBACK_MAX_DEPTH
        return CONSERVATIVE_FALLBACK_MAX_DEPTH

    def bundle_retained_ply_depth(self) -> int | None:
        contract = self._canonical_corpus_contract()
        max_supported_player_moves = contract.get("max_supported_player_moves")
        if isinstance(max_supported_player_moves, int) and max_supported_player_moves >= 2:
            return max_supported_player_moves * 2
        bundle_dir = self.runtime_context.config.corpus_bundle_dir
        if bundle_dir:
            try:
                compatibility = inspect_corpus_bundle(Path(bundle_dir))
            except Exception:
                compatibility = None
            if compatibility is not None and compatibility.retained_ply_depth is not None:
                return compatibility.retained_ply_depth
            try:
                provider = self.opponent.bundle_provider
            except AttributeError:
                provider = None
            manifest = getattr(getattr(provider, 'bundle', None), 'manifest', None)
            retained_ply_depth, _source = bundle_retained_ply_depth_from_metadata(Path(bundle_dir), manifest)
            if retained_ply_depth is not None:
                return retained_ply_depth
        artifact_path = self.runtime_context.config.corpus_artifact_path
        if artifact_path:
            try:
                return int(load_artifact(artifact_path).retained_ply_depth)
            except Exception:
                return None
        return None

    def update_settings(self, settings: TrainerSettings) -> TrainerSettings:
        saved = self.settings_store.save(settings, maximum_depth=self.max_supported_training_depth())
        self._apply_settings(saved)
        return saved

    def _apply_settings(self, settings: TrainerSettings) -> None:
        self.settings = settings.normalized(maximum_depth=self.max_supported_training_depth())
        self.smart_profile.set_mode(self.settings.training_mode)
        self.smart_profile.set_selected_track(self.settings.selected_smart_track)
        if not self.smart_profile.set_selected_time_control(self.settings.selected_time_control_id):
            self.smart_profile.set_selected_track(self.settings.selected_smart_track)
        if self.settings.training_mode == SMART_PROFILE_MODE:
            required_moves, good_accepted = self.smart_profile.enforce_runtime_contract(
                fallback_turns=self.settings.active_training_ply_depth,
                fallback_good_accepted=self.settings.good_moves_acceptable,
            )
        else:
            required_moves, good_accepted = self.settings.active_training_ply_depth, self.settings.good_moves_acceptable
        self.required_player_moves = required_moves
        self.config = type(self.config)(**{**self.config.snapshot(), 'active_envelope_player_moves': self.required_player_moves, 'good_moves_acceptable': good_accepted})
        if hasattr(self.evaluator, "config"):
            self.evaluator.config = self.config
        if hasattr(self.evaluator, "overlay_classifier") and hasattr(self.evaluator.overlay_classifier, "config"):
            self.evaluator.overlay_classifier.config = self.config
        if hasattr(self.evaluator, "engine_authority") and hasattr(self.evaluator.engine_authority, "config"):
            self.evaluator.engine_authority.config = self.config
        self._refresh_practical_risk_reconciled()
        self.opponent.set_fallback_mode(self.settings.opponent_fallback_mode)
        artifact_ready = bool(self.opening_locked_provider is not None and getattr(self.runtime_context, "opening_locked_artifact", None) and self.runtime_context.opening_locked_artifact.loaded)
        requested = bool(self.settings.opening_locked_mode_enabled)
        selected_name = self.settings.selected_opening_name
        selected_family = self.settings.opening_locked_family_name
        selected_variation = self.settings.opening_locked_variation_name
        effective_node = selected_name
        allowed_space: tuple[str, ...] = ()
        if self.opening_locked_provider is not None and self.opening_locked_provider.supports_family_ui():
            family_roots = self.opening_locked_provider.list_family_root_names()
            if not selected_family and selected_name:
                if selected_name in family_roots:
                    selected_family = selected_name
                else:
                    for family_name in family_roots:
                        descendants = set(self.opening_locked_provider.list_variation_names_for_family(family_name))
                        if selected_name in descendants:
                            selected_family = family_name
                            selected_variation = selected_name
                            break
            effective_node = self.opening_locked_provider.resolve_effective_selected_opening(selected_family, selected_variation) or selected_name
            allowed_space = self.opening_locked_provider.resolve_allowed_opening_space(effective_node)
            if effective_node and not selected_name:
                selected_name = effective_node
        effective = bool(artifact_ready and requested and effective_node)
        self.opening_locked_state = OpeningLockedSessionState(
            enabled=effective,
            selected_opening_name=selected_name,
            selected_family_name=selected_family,
            selected_variation_name=selected_variation,
            effective_opening_lock_node=effective_node,
            allowed_opening_space=allowed_space,
            lock_released_by_opponent=False,
            current_transition_state=OpeningLockedModeState.OPENING_LOCKED,
        )
        ineffective_reason = (
            "active"
            if effective
            else ("artifact unavailable" if not artifact_ready else ("mode toggle off" if not requested else "no opening selected"))
        )
        log_line(
            "OPENING_LOCKED_ACTIVATION "
            f"artifact_available={'yes' if artifact_ready else 'no'}; "
            f"requested={'yes' if requested else 'no'}; "
            f"selected_opening={selected_name or 'none'}; "
            f"family={selected_family or 'none'}; "
            f"variation={selected_variation or 'none'}; "
            f"effective={'yes' if effective else 'no'}; "
            f"reason={ineffective_reason}",
            tag='session',
        )

    def _refresh_practical_risk_reconciled(self) -> None:
        time_control_id, _rating_band, _source = self._effective_training_contract_metadata(source_context="refresh_reconciled_service")
        self.practical_risk_reconciled = PracticalRiskReconciledService(
            self.runtime_context.config.practical_risk_reconciled_path,
            expected_time_control_id=time_control_id,
        )
        self.evaluator.reconciled_service = self.practical_risk_reconciled

    def _profile_name(self) -> str:
        return self.review_storage.load_profile_meta(self.active_profile_id).display_name

    def _items(self):
        return self.review_storage.load_items(self.active_profile_id)

    def _save_items(self, items):
        self.review_storage.save_items(self.active_profile_id, items)

    def add_manual_target(
        self,
        *,
        target_fen: str,
        predecessor_line_uci: str | None,
        urgency_tier: str,
        allow_below_threshold_reach: bool,
        manual_presentation_mode: str = ManualPresentationMode.PLAY_TO_POSITION.value,
        manual_forced_player_color: str = ManualForcedPlayerColor.AUTO.value,
        operator_note: str | None = None,
    ) -> ReviewItem:
        target_board, predecessor_path, normalized_line = validate_manual_target(
            target_fen=target_fen,
            predecessor_line_uci=predecessor_line_uci,
            presentation_mode=manual_presentation_mode,
            auto_resolve_predecessor=manual_presentation_mode == ManualPresentationMode.PLAY_TO_POSITION.value,
            predecessor_master_db_path=self.runtime_context.config.predecessor_master_db_path,
        )
        if manual_presentation_mode == ManualPresentationMode.PLAY_TO_POSITION.value and not predecessor_line_uci:
            log_line(
                (
                    "Manual target predecessor lookup attempted; "
                    f"db_path={self.runtime_context.config.predecessor_master_db_path!r}; "
                    f"target_key={normalize_builder_position_key(target_board)}; "
                    f"success={bool(normalized_line)}; ply_count={len(predecessor_path)}"
                ),
                tag="review",
            )
        item = create_manual_target_item(
            profile_id=self.active_profile_id,
            target_board=target_board,
            predecessor_path=predecessor_path,
            predecessor_line_uci=normalized_line,
            urgency_tier=urgency_tier,
            allow_below_threshold_reach=allow_below_threshold_reach,
            manual_presentation_mode=manual_presentation_mode,
            manual_forced_player_color=manual_forced_player_color,
            operator_note=operator_note,
        )
        items = self._items()
        items = [existing for existing in items if existing.review_item_id != item.review_item_id]
        items.append(item)
        self._save_items(items)
        return item

    def edit_review_item(self, review_item_id: str, **changes) -> ReviewItem:
        items = self._items()
        item = next((candidate for candidate in items if candidate.review_item_id == review_item_id), None)
        if item is None:
            raise ValueError('Review item not found.')
        manual_fields_requested = any(
            key in changes
            for key in (
                'target_fen',
                'predecessor_line_uci',
                'allow_below_threshold_reach',
                'manual_presentation_mode',
                'manual_forced_player_color',
            )
        )
        if item.origin_kind == ReviewItemOrigin.MANUAL_TARGET.value or manual_fields_requested:
            target_fen = changes.get('target_fen', item.manual_target_fen or item.position_fen_normalized)
            predecessor_line_uci = changes.get('predecessor_line_uci', item.predecessor_line_uci)
            manual_presentation_mode = changes.get('manual_presentation_mode', item.manual_presentation_mode)
            target_board, predecessor_path, normalized_line = validate_manual_target(
                target_fen=target_fen,
                predecessor_line_uci=predecessor_line_uci,
                presentation_mode=manual_presentation_mode,
                auto_resolve_predecessor=manual_presentation_mode == ManualPresentationMode.PLAY_TO_POSITION.value,
                predecessor_master_db_path=self.runtime_context.config.predecessor_master_db_path,
            )
            if manual_presentation_mode == ManualPresentationMode.PLAY_TO_POSITION.value and not predecessor_line_uci:
                log_line(
                    (
                        "Manual target predecessor lookup attempted during edit; "
                        f"db_path={self.runtime_context.config.predecessor_master_db_path!r}; "
                        f"target_key={normalize_builder_position_key(target_board)}; "
                        f"success={bool(normalized_line)}; ply_count={len(predecessor_path)}"
                    ),
                    tag="review",
                )
            item.position_fen_normalized = target_board.fen()
            item.position_key = normalize_builder_position_key(target_board)
            item.side_to_move = 'white' if target_board.turn == chess.WHITE else 'black'
            item.predecessor_line_uci = normalized_line
            item.predecessor_line_notation_kind = 'uci' if normalized_line else None
            item.predecessor_path = [asdict(move) for move in predecessor_path]
            item.line_preview_san = ' '.join(move.san for move in predecessor_path[-6:])
            item.manual_target_fen = target_board.fen()
            item.allow_below_threshold_reach = bool(changes.get('allow_below_threshold_reach', item.allow_below_threshold_reach))
            item.manual_presentation_mode = manual_presentation_mode
            item.manual_forced_player_color = changes.get('manual_forced_player_color', item.manual_forced_player_color)
            item.operator_note = changes.get('operator_note', item.operator_note)
            if item.origin_kind != ReviewItemOrigin.MANUAL_TARGET.value:
                item.origin_kind = ReviewItemOrigin.MANUAL_TARGET.value
                if getattr(item, "manual_parent_review_item_id", None) is None:
                    item.manual_parent_review_item_id = review_item_id
        item.urgency_tier = changes.get('urgency_tier', item.urgency_tier)
        item.frequency_state = item.urgency_tier
        if item.origin_kind == ReviewItemOrigin.MANUAL_TARGET.value:
            item.manual_initial_urgency_tier = item.urgency_tier
        item.updated_at_utc = utc_now_iso()
        self._save_items(items)
        return item

    def start_new_game(self) -> SessionView:
        self.cancel_pending_opponent_action()
        self.state = SessionState.STARTING_GAME
        self.board.reset()
        self._clear_opening_name_state(reason='new_game_start')
        self.player_color = random.choice([chess.WHITE, chess.BLACK])
        self.player_move_count = 0
        self.last_evaluation = None
        self.last_outcome = None
        self.last_opponent_choice = None
        if self.opening_locked_state.enabled:
            self.opening_locked_state.lock_released_by_opponent = False
            self.opening_locked_state.current_transition_state = OpeningLockedModeState.OPENING_LOCKED
        self.timed_state = self._build_timed_state_from_bundle()
        self._player_turn_started_at = None
        self.run_path = []
        items = self._items()
        transition_changed = False
        for item in items:
            transition_changed = sync_due_cycle_transition(item) or transition_changed
        if transition_changed:
            self._save_items(items)
        self.current_routing = self._select_routing_with_inspector(items, reason="start_new_game")
        self.review_storage.save_router_state(self.active_profile_id, self.router.export_profile_state(self.active_profile_id))
        self.current_review_item_id = self.current_routing.selected_review_item_id
        self.active_review_plan = self.current_routing.review_plan
        if self.active_review_plan and self.active_review_plan.root_fen != 'startpos':
            self.board.board = chess.Board(self.active_review_plan.root_fen)
        self._apply_manual_forced_color(items)
        self._print_new_game_banner()
        effective_time_control_id, effective_rating_band, contract_source = self._effective_training_contract_metadata(
            source_context="start_new_game"
        )
        log_line(
            "TRAINING_CONTRACT_RESOLVED "
            f"requested_band={effective_rating_band or 'unknown'} source={contract_source} "
            f"time_control={effective_time_control_id or 'unknown'}",
            tag="startup",
        )
        log_line(
            "TRAINING_BUNDLE_BIND_REUSED "
            f"bundle={self.runtime_context.config.corpus_bundle_dir or 'none'} "
            f"band={effective_rating_band or 'unknown'} source={contract_source}",
            tag="startup",
        )
        log_line(self.opponent.status_message, tag='startup')
        self._print_startup_summary()
        log_line(f'Routing: {self.current_routing.selection_explanation}', tag='review')
        if self.board.turn() == self.player_color:
            self.state = SessionState.PLAYER_TURN
            self._player_turn_started_at = None if self.mode == 'gui' else time.monotonic()
        else:
            self.state = SessionState.OPPONENT_TURN
            if self.mode != 'gui':
                self.advance_until_user_turn()
        self._refresh_opening_name_state(reason='position_refresh')
        return self.get_view()

    def get_view(self) -> SessionView:
        return SessionView(
            self.board.board.fen(),
            self.player_color,
            self.state,
            self.player_move_count,
            self.required_player_moves,
            self.last_evaluation,
            self.last_outcome,
            self.current_routing,
            tuple(self.move_history()),
            self.corpus_summary_text(),
            self.opening_name,
            self.opening_name_frozen,
            self.opening_locked_state.current_transition_state.value if self.opening_locked_state.enabled else None,
            self.opening_locked_state.selected_opening_name if self.opening_locked_state.enabled else None,
        )

    def _apply_manual_forced_color(self, items: list[ReviewItem]) -> None:
        if not self.current_routing or self.current_routing.routing_source != RoutingSource.MANUAL_TARGET.value:
            return
        item = next((candidate for candidate in items if candidate.review_item_id == self.current_review_item_id), None)
        if item is None:
            return
        forced_color = item.manual_forced_player_color or ManualForcedPlayerColor.AUTO.value
        if forced_color == ManualForcedPlayerColor.WHITE.value:
            self.player_color = chess.WHITE
        elif forced_color == ManualForcedPlayerColor.BLACK.value:
            self.player_color = chess.BLACK
        log_line(
            f"Manual target presentation={item.manual_presentation_mode}; "
            f"forced_color={forced_color}; allow_below_threshold_reach={item.allow_below_threshold_reach}",
            tag='review',
        )

    def _clear_opening_name_state(self, *, reason: str) -> None:
        if self.opening_name is not None or self.opening_name_frozen:
            log_line(f"GUI_OPENING_NAME_CLEARED old_value={self.opening_name!r}; reason={reason}", tag='gui')
        self.opening_name = None
        self.opening_name_frozen = False

    def _refresh_opening_name_state(self, *, reason: str) -> None:
        if self.opening_name_frozen:
            return
        opening_name = self.opening_names.opening_name_for_board(self.board.board)
        if isinstance(opening_name, str):
            opening_name = opening_name.strip() or None
        else:
            opening_name = None
        if opening_name is not None:
            if opening_name != self.opening_name:
                update_reason = 'refined' if self.opening_name else 'dataset_hit'
                log_line(
                    f"GUI_OPENING_NAME_UPDATED old_value={self.opening_name!r}; new_value={opening_name!r}; reason={update_reason}",
                    tag='gui',
                )
                self.opening_name = opening_name
            return
        if self.opening_name is not None:
            self.opening_name_frozen = True
            log_line(
                f"GUI_OPENING_NAME_FROZEN final_value={self.opening_name!r}; reason=book_exit_or_lookup_miss; trigger={reason}",
                tag='gui',
            )

    def current_board(self) -> chess.Board:
        return self.board.board.copy(stack=True)

    def legal_moves_from(self, square: chess.Square) -> list[chess.Move]:
        return self.board.legal_moves_from(square)

    def is_awaiting_user_input(self) -> bool:
        return self.state == SessionState.PLAYER_TURN

    def has_failed(self) -> bool:
        return self.state == SessionState.FAIL_RESOLUTION or (self.last_outcome is not None and self.last_outcome.terminal_kind == 'fail')

    def has_passed(self) -> bool:
        return self.state == SessionState.SUCCESS_RESOLUTION or (self.last_outcome is not None and self.last_outcome.terminal_kind == 'pass')

    def has_authority_unavailable(self) -> bool:
        return self.state == SessionState.AUTHORITY_UNAVAILABLE_RESOLUTION or (self.last_outcome is not None and self.last_outcome.terminal_kind == 'authority_unavailable')

    def switch_profile(self, profile_id: str) -> None:
        self.profile_service.switch_profile(profile_id)
        self.active_profile_id = profile_id
        self.smart_profile.switch_profile(profile_id)
        self.router.import_profile_state(profile_id, self.review_storage.load_router_state(profile_id))

    def reset_profile(self, profile_id: str) -> bool:
        self.profile_service.reset_profile(profile_id)
        self.router.clear_profile_state(profile_id)
        self.review_storage.save_router_state(profile_id, {})
        is_active_profile = self.active_profile_id == profile_id
        if is_active_profile:
            self.smart_profile.reset_all()
        return is_active_profile

    def run_session(self, input_func=None) -> None:
        if input_func is None:
            input_func = input
        while True:
            if self.state == SessionState.PLAYER_TURN:
                self._handle_player_turn(input_func)
                continue
            if self.state == SessionState.OPPONENT_TURN:
                self.advance_until_user_turn()
                continue
            if self.state == SessionState.FAIL_RESOLUTION:
                self._resolve_fail(); return
            if self.state == SessionState.SUCCESS_RESOLUTION:
                self._resolve_success(); return
            if self.state == SessionState.AUTHORITY_UNAVAILABLE_RESOLUTION:
                self._resolve_authority_unavailable(); return
            if self.state == SessionState.RESTART_PENDING:
                return
            raise RuntimeError(f'Unexpected session state: {self.state}')

    def submit_user_move_uci(self, move_uci: str, *, premove_executed: bool = False) -> SessionView:
        return self._submit_user_move(move_uci.strip(), premove_executed=premove_executed)

    def submit_user_move(self, move_text: str) -> SessionView:
        return self._submit_user_move(move_text.strip())

    def advance_until_user_turn(self) -> SessionView:
        while self.state == SessionState.OPPONENT_TURN:
            self._handle_opponent_turn()
        return self.get_view()

    def _handle_player_turn(self, input_func=None) -> None:
        if input_func is None:
            input_func = input
        log_line('', tag='startup')
        log_line(str(self.board), tag='startup')
        log_line('', tag='startup')
        log_line('Your move prompt displayed.', tag='startup')
        self._submit_user_move(input_func().strip())

    def _record_path_move(self, board_before: chess.Board, move: chess.Move) -> None:
        self.run_path.append(ReviewPathMove(len(board_before.move_stack), 'white' if board_before.turn == chess.WHITE else 'black', move.uci(), board_before.san(move), board_before.fen()))

    def move_history(self) -> list[MoveHistoryEntry]:
        history: list[MoveHistoryEntry] = []
        for move in self.run_path:
            actor = 'player' if ((move.side_to_move == 'white') == (self.player_color == chess.WHITE)) else 'opponent'
            history.append(MoveHistoryEntry(move.ply_index, move.side_to_move, move.move_uci, move.san or move.move_uci, actor))
        return history

    def corpus_summary_text(self) -> str:
        timing_text = self._timing_summary_text()
        bundle_dir = self.runtime_context.config.corpus_bundle_dir
        if bundle_dir:
            provider = getattr(self.opponent, 'bundle_provider', None)
            bundle_handle = getattr(provider, 'bundle', None)
            manifest = getattr(bundle_handle, 'manifest', None)
            if not isinstance(manifest, dict):
                metadata = getattr(bundle_handle, 'metadata', None)
                manifest = getattr(metadata, 'manifest', None)
            if isinstance(manifest, dict):
                band = manifest.get('target_rating_band') or manifest.get('rating_band') or manifest.get('elo_band')
                retained = manifest.get('retained_ply_depth')
                max_supported_player_moves = manifest.get("max_supported_player_moves")
                if max_supported_player_moves is not None:
                    retained = f"{max_supported_player_moves} player moves"
                rating_policy = manifest.get("rating_policy")
                band_text = self._format_rating_band(band) or self._bundle_name_fallback(bundle_dir)
                retained_text = f' | Retained depth: {retained}' if retained is not None else ''
                policy_text = f' | Rating policy: {rating_policy}' if isinstance(rating_policy, str) and rating_policy.strip() else ''
                return f'Corpus: {band_text}{retained_text}{policy_text}{timing_text}'
            return f'Corpus: {self._bundle_name_fallback(bundle_dir)}{timing_text}'
        artifact_path = self.runtime_context.config.corpus_artifact_path
        if artifact_path:
            try:
                artifact = load_artifact(artifact_path)
                band_text = self._format_rating_band(getattr(artifact, 'target_rating_band', None)) or 'artifact'
                return f'Corpus: {band_text} | Retained depth: {artifact.retained_ply_depth}{timing_text}'
            except Exception:
                return f'Corpus: legacy artifact{timing_text}'
        return f'Corpus: fallback / no bundle metadata{timing_text}'

    def _format_rating_band(self, band: object) -> str | None:
        if isinstance(band, dict):
            minimum = band.get('minimum')
            maximum = band.get('maximum')
            if minimum is not None and maximum is not None:
                return f'{minimum}-{maximum}'
        if isinstance(band, str) and band.strip():
            return band.strip()
        return None

    def _bundle_name_fallback(self, bundle_dir: object) -> str:
        try:
            name = getattr(bundle_dir, 'name', None) or str(bundle_dir).rstrip('/').split('/')[-1]
        except Exception:
            name = str(bundle_dir)
        return name.replace('_', ' ')

    def _timing_contract_metadata(self) -> tuple[str | None, str | None]:
        time_control_id, rating_band, _source = self._effective_training_contract_metadata(source_context="legacy_call")
        return time_control_id, rating_band

    def _effective_training_contract_metadata(self, *, source_context: str) -> tuple[str | None, str | None, str]:
        settings = getattr(self, "settings", None)
        if settings is not None and settings.training_mode == SMART_PROFILE_MODE:
            status = self.smart_profile.status(
                routing_source=RoutingSource.CORPUS.value,
                bundle_available=bool(self.runtime_context.config.corpus_bundle_dir),
                time_control_id=settings.selected_time_control_id,
                bundle_rating_band=None,
                required_turns=getattr(self, "required_player_moves", self.config.active_envelope_player_moves),
                good_accepted=self.config.good_moves_acceptable,
                catalog_root=settings.last_corpus_catalog_root,
            )
            if status.category_id and status.expected_rating_band:
                return status.category_id, status.expected_rating_band, "smart_profile_level"

        provider = getattr(self.opponent, "bundle_provider", None)
        bundle = getattr(provider, "bundle", None)
        manifest = getattr(bundle, "manifest", None)
        if not isinstance(manifest, dict):
            metadata = getattr(bundle, "metadata", None)
            manifest = getattr(metadata, "manifest", None)
        time_control_id = None
        rating_band = None
        if isinstance(manifest, dict):
            raw_time_control = manifest.get("time_control_id")
            if raw_time_control is not None and str(raw_time_control).strip():
                time_control_id = str(raw_time_control).strip()
            rating_band = self._format_rating_band(manifest.get("target_rating_band") or manifest.get("rating_band") or manifest.get("elo_band"))
            if not time_control_id and manifest.get("time_format_label"):
                time_control_id = str(manifest.get("time_format_label")).strip()
        if not time_control_id:
            time_control_id = getattr(bundle, "bundle_invariant_time_control_id", None)
        if not rating_band:
            rating_band = getattr(bundle, "bundle_invariant_rating_band", None)
        if time_control_id or rating_band:
            return time_control_id, rating_band, "bundle_manifest"
        selected_time_control = getattr(settings, "selected_time_control_id", None) if settings is not None else None
        return selected_time_control, None, f"fallback_settings_{source_context}"

    def _canonical_corpus_contract(self) -> dict[str, object]:
        provider = getattr(self.opponent, "bundle_provider", None)
        bundle = getattr(provider, "bundle", None)
        manifest = getattr(bundle, "manifest", None)
        if not isinstance(manifest, dict):
            metadata = getattr(bundle, "metadata", None)
            manifest = getattr(metadata, "manifest", None)
        if not isinstance(manifest, dict):
            bundle_dir = self.runtime_context.config.corpus_bundle_dir
            if bundle_dir:
                manifest_path = Path(bundle_dir) / "manifest.json"
                if manifest_path.exists():
                    try:
                        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    except Exception:
                        manifest = None
        if not isinstance(manifest, dict):
            return {}

        contract: dict[str, object] = {}
        for key in (
            "retained_ply_depth",
            "max_supported_player_moves",
            "time_control_id",
            "initial_time_seconds",
            "increment_seconds",
            "time_format_label",
            "target_rating_band",
            "rating_policy",
            "canonical_exact_payload_file",
            "compatibility_exact_payload_file",
            "payload_format",
            "payload_version",
        ):
            value = manifest.get(key)
            if value is not None:
                contract[key] = value
        return contract

    def _is_final_canonical_bundle(self, manifest: dict[str, object]) -> bool:
        return any(
            key in manifest
            for key in (
                "canonical_exact_payload_file",
                "compatibility_exact_payload_file",
                "payload_version",
                "max_supported_player_moves",
                "time_format_label",
            )
        )

    def _opening_lock_active(self) -> bool:
        return bool(
            self.opening_locked_state.enabled
            and not self.opening_locked_state.lock_released_by_opponent
            and self.opening_locked_provider is not None
            and self.opening_locked_state.selected_opening_name
        )

    def _classify_opening_transition(self, board_after_move: chess.Board) -> OpeningTransitionClassification:
        if not self._opening_lock_active():
            return OpeningTransitionClassification.UNKNOWN
        position_key = normalize_builder_position_key(board_after_move)
        transition = self.opening_locked_provider.classify_transition(
            successor_position_key=position_key,
            selected_opening_name=str(self.opening_locked_state.selected_opening_name),
            allowed_opening_space=(
                set(self.opening_locked_state.allowed_opening_space)
                if self.opening_locked_state.allowed_opening_space
                else None
            ),
        )
        return transition.classification

    def _submit_user_move(self, move_str: str, *, premove_executed: bool = False) -> SessionView:
        if self.state != SessionState.PLAYER_TURN:
            raise RuntimeError('Cannot submit a user move when the session is not awaiting player input.')
        if not self.board.is_legal(move_str):
            log_line('Illegal move. Try again.', tag='evaluation')
            return self.get_view()
        board_before_move = self.board.board.copy(stack=True)
        if self._player_turn_started_at is None:
            self._player_turn_started_at = time.monotonic()
        self._consume_player_think_time(
            additional_seconds=(
                self.premove_execution_time_cost_seconds
                if premove_executed
                else 0.0
            )
        )
        pre_fail_fen = board_before_move.fen()
        move = self.board.push(move_str)
        self._record_path_move(board_before_move, move)
        self.player_move_count += 1
        setup_override_feedback = self._manual_target_setup_override_feedback(board_before_move, move)
        if setup_override_feedback is not None:
            self.last_evaluation = setup_override_feedback
            self._refresh_opening_name_state(reason='position_refresh')
            self._print_evaluation_feedback(setup_override_feedback)
            if self._resolve_terminal_board_state():
                return self.get_view()
            self.state = SessionState.OPPONENT_TURN
            if self.mode != 'gui':
                self.advance_until_user_turn()
            return self.get_view()
        if self._resolve_terminal_board_state():
            return self.get_view()
        _time_control_id, rating_band, contract_source = self._effective_training_contract_metadata(source_context="submit_user_move")
        log_line(
            "TRAINING_CONTRACT_RESOLVED "
            f"requested_band={rating_band or 'unknown'} source={contract_source} "
            f"time_control={_time_control_id or 'unknown'}",
            tag="evaluation",
        )
        try:
            evaluation = self.evaluator.evaluate(
                board_before_move,
                move,
                self.player_move_count,
                requested_band_id=rating_band,
                allow_sharp_gambit_lines=self.settings.allow_sharp_gambit_lines,
            )
        except TypeError:
            evaluation = self.evaluator.evaluate(board_before_move, move, self.player_move_count)
        transition_classification = self._classify_opening_transition(self.board.board)
        if (
            evaluation.accepted
            and self._opening_lock_active()
            and transition_classification in {
                OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING,
                OpeningTransitionClassification.LEFT_TO_UNNAMED,
            }
        ):
            canonical = self.opening_locked_provider.canonical_continuation(
                position_key=normalize_builder_position_key(board_before_move),
                selected_opening_name=str(self.opening_locked_state.selected_opening_name),
                max_plies=8,
            )
            evaluation = replace(
                evaluation,
                accepted=False,
                canonical_judgment=CanonicalJudgment.FAIL,
                reason_code=ReasonCode.OPENING_EXIT_BEFORE_OPPONENT,
                reason_text="This move may be acceptable under ordinary policy, but it failed because it left the selected opening before the opponent did.",
                preferred_move_uci=canonical.next_move_uci or evaluation.preferred_move_uci,
                metadata={
                    **(evaluation.metadata if isinstance(evaluation.metadata, dict) else {}),
                    "opening_locked": {
                        "selected_opening_name": self.opening_locked_state.selected_opening_name,
                        "transition_classification": transition_classification.value,
                        "ordinary_policy_would_accept": True,
                        "canonical_line": list(canonical.line),
                    },
                },
            )
        self.last_evaluation = evaluation
        self._refresh_opening_name_state(reason='position_refresh')
        self._print_evaluation_feedback(evaluation)
        if evaluation.canonical_judgment == CanonicalJudgment.AUTHORITY_UNAVAILABLE:
            self.last_outcome = SessionOutcome(False, evaluation.reason_text, None, evaluation, 'authority_unavailable', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', 'ordinary_corpus_play', self._profile_name(), 'No review item recorded because the authority was unavailable.')
            self.state = SessionState.AUTHORITY_UNAVAILABLE_RESOLUTION
            self._resolve_authority_unavailable()
            return self.get_view()
        if not evaluation.accepted:
            post_fail_fen = self.board.board.fen()
            punishment_line = self.get_best_continuation_from_board(self.board.board.copy(stack=True), max_plies=15)
            punishing_reply_uci = punishment_line[0][0] if punishment_line else None
            punishing_reply_san = punishment_line[0][1] if punishment_line else None
            corrective_line = self.get_corrective_continuation(board_before_move, evaluation.preferred_move_uci, max_plies=15)
            excellent_moves, good_moves = self._lookup_recommended_alternatives(board_before_move, evaluation)
            item, impact_summary, next_reason = self._capture_failure(board_before_move, evaluation)
            self.last_outcome = SessionOutcome(
                False,
                evaluation.reason_text,
                evaluation.preferred_move_san or evaluation.preferred_move_uci,
                evaluation,
                'fail',
                self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play',
                next_reason,
                self._profile_name(),
                impact_summary,
                pre_fail_fen=pre_fail_fen,
                post_fail_fen=post_fail_fen,
                preferred_move_uci=evaluation.preferred_move_uci,
                preferred_move_san=evaluation.preferred_move_san,
                punishing_reply_uci=punishing_reply_uci,
                punishing_reply_san=punishing_reply_san,
                punishment_line=tuple(punishment_line),
                corrective_line=tuple(corrective_line),
                corrective_root_fen=board_before_move.fen() if corrective_line else None,
                corrective_move_uci=evaluation.preferred_move_uci if corrective_line else None,
                corrective_move_san=evaluation.preferred_move_san if corrective_line else None,
                excellent_moves=tuple(excellent_moves),
                good_moves=tuple(good_moves),
                player_color=self.player_color,
            )
            self.state = SessionState.FAIL_RESOLUTION
            self._record_smart_profile_outcome(False)
            self._resolve_fail()
            return self.get_view()
        if self._resolve_terminal_board_state():
            return self.get_view()
        if self.player_move_count >= self.required_player_moves:
            impact_summary, next_reason = self._capture_success_if_needed()
            self.last_outcome = SessionOutcome(True, f'Completed {self.required_player_moves} accepted player moves inside the opening window.', None, evaluation, 'pass', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', next_reason, self._profile_name(), impact_summary)
            self.state = SessionState.SUCCESS_RESOLUTION
            self._record_smart_profile_outcome(True)
            self._resolve_success()
            return self.get_view()
        self.state = SessionState.OPPONENT_TURN
        if self.mode != 'gui':
            self.advance_until_user_turn()
        return self.get_view()


    def _terminal_outcome_message(self, outcome: chess.Outcome) -> str:
        termination_name = outcome.termination.name.replace('_', ' ').title()
        if outcome.winner == self.player_color:
            return f'Run ended with {termination_name.lower()}; the player reached a genuine terminal win inside the active envelope.'
        if outcome.winner is None:
            return f'Run ended with {termination_name.lower()} inside the active envelope.'
        return f'Run ended with {termination_name.lower()}; the player was defeated inside the active envelope.'

    def _resolve_terminal_board_state(self) -> bool:
        outcome = self.board.board.outcome(claim_draw=True)
        if outcome is None:
            return False
        reason = self._terminal_outcome_message(outcome)
        if outcome.winner == self.player_color:
            impact_summary, next_reason = self._capture_success_if_needed()
            self.last_outcome = SessionOutcome(True, reason, None, self.last_evaluation, 'pass', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', next_reason, self._profile_name(), impact_summary)
            self.state = SessionState.SUCCESS_RESOLUTION
            self._record_smart_profile_outcome(True)
            self._resolve_success()
            return True
        impact_summary = 'Terminal game state reached; no additional review item recorded.'
        next_reason = self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play'
        self.last_outcome = SessionOutcome(False, reason, None, self.last_evaluation, 'fail', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', next_reason, self._profile_name(), impact_summary, player_color=self.player_color)
        self.state = SessionState.FAIL_RESOLUTION
        self._record_smart_profile_outcome(False)
        self._resolve_fail()
        return True


    def smart_profile_expected_bundle_path(self) -> str | None:
        resolution = self.smart_profile.resolve_expected_bundle(self.settings.last_corpus_catalog_root)
        if resolution.resolved_entry is None:
            return None
        return str(resolution.resolved_entry.bundle_dir)

    def smart_profile_status(self):
        time_control_id, rating_band = self._timing_contract_metadata()
        routing_source = self.current_routing.routing_source if self.current_routing else 'not_started'
        return self.smart_profile.status(
            routing_source=routing_source,
            bundle_available=bool(self.runtime_context.config.corpus_bundle_dir),
            time_control_id=time_control_id,
            bundle_rating_band=rating_band,
            required_turns=self.required_player_moves,
            good_accepted=self.config.good_moves_acceptable,
            catalog_root=self.settings.last_corpus_catalog_root,
        )

    def opening_locked_artifact_status(self):
        return getattr(self.runtime_context, "opening_locked_artifact", None)

    def opening_locked_opening_names(self) -> list[str]:
        if self.opening_locked_provider is None:
            return []
        try:
            if self.opening_locked_provider.supports_family_ui():
                return self.opening_locked_provider.list_family_root_names()
            return self.opening_locked_provider.list_exact_opening_names()
        except Exception:
            return []

    def opening_locked_variation_names(self, family_name: str | None) -> list[str]:
        if self.opening_locked_provider is None:
            return []
        if not family_name:
            return []
        try:
            if not self.opening_locked_provider.supports_family_ui():
                return []
            return self.opening_locked_provider.list_variation_names_for_family(str(family_name))
        except Exception:
            return []

    def opening_locked_supports_family_aware(self) -> bool:
        return bool(self.opening_locked_provider is not None and self.opening_locked_provider.supports_family_aware())

    def _record_smart_profile_outcome(self, passed: bool) -> None:
        if self.settings.training_mode != SMART_PROFILE_MODE:
            return
        track_state, _contract = self.smart_profile.current_track_state()
        previous_level = int(track_state.current_level)
        time_control_id, rating_band = self._timing_contract_metadata()
        routing_source = self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play'
        if routing_source == 'manual_target':
            return
        eligibility = self.smart_profile.evaluate_eligibility(
            routing_source=routing_source,
            bundle_available=bool(self.runtime_context.config.corpus_bundle_dir),
            time_control_id=time_control_id,
            bundle_rating_band=rating_band,
            required_turns=self.required_player_moves,
            good_accepted=self.config.good_moves_acceptable,
            catalog_root=self.settings.last_corpus_catalog_root,
        )
        if not eligibility.eligible and (
            "mismatch" in eligibility.reason.lower() or "no corpus bundle is active" in eligibility.reason.lower()
        ):
            log_line(
                f"GUI_SMART_ELIGIBILITY_MISMATCH reason={eligibility.reason} "
                f"routing={routing_source} active_control={time_control_id or 'unknown'} active_band={(rating_band or 'unknown')}",
                tag="smart_profile",
            )
        self.smart_profile.apply_eligible_result(
            eligibility,
            passed=passed,
            bundle_time_control_id=time_control_id,
            bundle_rating_band=rating_band,
        )
        refreshed_track_state, _updated_contract = self.smart_profile.current_track_state()
        new_level = int(refreshed_track_state.current_level)
        self._apply_settings(self.settings)
        if new_level != previous_level:
            self._pending_smart_level_change = (previous_level, new_level)

    def consume_pending_smart_level_change(self) -> tuple[int, int] | None:
        pending = self._pending_smart_level_change
        self._pending_smart_level_change = None
        return pending

    def get_best_continuation_from_board(self, board: chess.Board, max_plies: int = 15) -> list[tuple[str, str, str]]:
        if max_plies <= 0:
            return []
        engine_authority = getattr(self.evaluator, 'engine_authority', None)
        best_continuation = getattr(engine_authority, 'best_continuation', None)
        if best_continuation is None:
            best_reply = getattr(engine_authority, 'best_reply', None)
            if best_reply is None:
                return []
            try:
                reply_uci, reply_san = best_reply(board)
            except Exception:
                return []
            if not reply_uci or not reply_san:
                return []
            move = chess.Move.from_uci(reply_uci)
            if move not in board.legal_moves:
                return []
            board.push(move)
            return [(reply_uci, reply_san, board.fen())]
        try:
            line = list(best_continuation(board, plies=max_plies))
        except Exception:
            line = []
        if line:
            return line
        best_reply = getattr(engine_authority, 'best_reply', None)
        if best_reply is None:
            return []
        try:
            reply_uci, reply_san = best_reply(board)
        except Exception:
            return []
        if not reply_uci or not reply_san:
            return []
        move = chess.Move.from_uci(reply_uci)
        if move not in board.legal_moves:
            return []
        board.push(move)
        return [(reply_uci, reply_san, board.fen())]

    def get_corrective_continuation(
        self,
        board_before_fail: chess.Board,
        preferred_move_uci: str | None,
        max_plies: int = 15,
    ) -> list[tuple[str, str, str]]:
        if (
            self.last_evaluation is not None
            and self.last_evaluation.reason_code == ReasonCode.OPENING_EXIT_BEFORE_OPPONENT
            and self.opening_locked_provider is not None
            and self.opening_locked_state.selected_opening_name
        ):
            canonical = self.opening_locked_provider.canonical_continuation(
                position_key=normalize_builder_position_key(board_before_fail),
                selected_opening_name=str(self.opening_locked_state.selected_opening_name),
                max_plies=max_plies,
            )
            board_for_line = board_before_fail.copy(stack=True)
            line: list[tuple[str, str, str]] = []
            for move_uci in canonical.line:
                try:
                    move = chess.Move.from_uci(move_uci)
                except ValueError:
                    break
                if move not in board_for_line.legal_moves:
                    break
                san = board_for_line.san(move)
                board_for_line.push(move)
                line.append((move_uci, san, board_for_line.fen()))
            if line:
                return line
        if max_plies <= 0 or not preferred_move_uci:
            return []
        board_after_correction = board_before_fail.copy(stack=True)
        try:
            corrective_move = chess.Move.from_uci(preferred_move_uci)
        except ValueError:
            return []
        if corrective_move not in board_after_correction.legal_moves:
            return []
        corrective_san = board_after_correction.san(corrective_move)
        board_after_correction.push(corrective_move)
        continuation = [(preferred_move_uci, corrective_san, board_after_correction.fen())]
        continuation.extend(self.get_best_continuation_from_board(board_after_correction, max_plies=max_plies - 1))
        return continuation[:max_plies]

    def _lookup_recommended_alternatives(
        self,
        board_before_move: chess.Board,
        evaluation: EvaluationResult,
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        if not evaluation.preferred_move_uci:
            return [], []
        engine_authority = getattr(self.evaluator, 'engine_authority', None)
        ranked_candidate_moves = getattr(engine_authority, 'ranked_candidate_moves', None)
        if ranked_candidate_moves is None:
            return [], []
        try:
            ranked = list(ranked_candidate_moves(board_before_move, max_moves=8))
        except Exception:
            return [], []
        excellent: list[tuple[str, str]] = []
        good: list[tuple[str, str]] = []
        for move_uci, move_san, cp_loss in ranked:
            if move_uci == evaluation.preferred_move_uci:
                continue
            if cp_loss is None:
                continue
            if cp_loss <= self.config.overlay_excellent_max_cp_loss:
                excellent.append((move_uci, move_san))
            elif self.config.good_moves_acceptable and cp_loss <= self.config.overlay_good_max_cp_loss:
                good.append((move_uci, move_san))
        return excellent[:3], good[:3]

    def _capture_failure(self, board_before_move: chess.Board, evaluation: EvaluationResult):
        items = self._items()
        position_key = normalize_builder_position_key(board_before_move)
        side = 'white' if board_before_move.turn == chess.WHITE else 'black'
        existing = next((item for item in items if item.position_key == position_key and item.side_to_move == side), None)
        previous_frequency_state = existing.frequency_state if existing is not None else None
        accepted = list(evaluation.metadata.get('candidate_moves', [])) if isinstance(evaluation.metadata, dict) else []
        line_preview = ' '.join(move.san for move in self.run_path[-6:])
        inherited_manual_metadata = self._manual_inherited_failure_metadata(board_before_move, items)
        if existing is None:
            item = ReviewItem.create(self.active_profile_id, position_key, board_before_move.fen(), side, evaluation.reason_text, evaluation.preferred_move_uci, accepted, self.run_path)
            item.d_failure_metadata = self._d_failure_metadata(evaluation)
            if inherited_manual_metadata is not None:
                item.origin_kind = ReviewItemOrigin.MANUAL_TARGET.value
                item.allow_below_threshold_reach = inherited_manual_metadata['allow_below_threshold_reach']
                item.predecessor_line_uci = inherited_manual_metadata['predecessor_line_uci']
                item.predecessor_line_notation_kind = 'uci' if item.predecessor_line_uci else None
                item.manual_parent_review_item_id = inherited_manual_metadata['manual_parent_review_item_id']
                item.manual_reach_policy_inherited = True
                item.manual_presentation_mode = ManualPresentationMode.PLAY_TO_POSITION.value
                item.manual_forced_player_color = inherited_manual_metadata['manual_forced_player_color']
            items.append(item)
            impact_summary = 'Created new review item and scheduled immediate retry.'
        else:
            item = apply_failure(existing, evaluation.reason_text, evaluation.preferred_move_uci, [asdict(move) for move in self.run_path], line_preview, self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play')
            item.d_failure_metadata = self._d_failure_metadata(evaluation)
            if inherited_manual_metadata is not None:
                item.origin_kind = ReviewItemOrigin.MANUAL_TARGET.value
                item.allow_below_threshold_reach = inherited_manual_metadata['allow_below_threshold_reach']
                item.predecessor_line_uci = inherited_manual_metadata['predecessor_line_uci']
                item.predecessor_line_notation_kind = 'uci' if item.predecessor_line_uci else None
                item.manual_parent_review_item_id = inherited_manual_metadata['manual_parent_review_item_id']
                item.manual_reach_policy_inherited = True
                item.manual_presentation_mode = ManualPresentationMode.PLAY_TO_POSITION.value
                item.manual_forced_player_color = inherited_manual_metadata['manual_forced_player_color']
            impact_summary = f'Updated review item; urgency is now {item.urgency_tier}.'
        decision = self.router.stubborn_extreme_repeat(self.active_profile_id, item) if item.pending_forced_stubborn_repeat else self.router.immediate_retry(self.active_profile_id, item)
        self.router.record_review_result(self.active_profile_id, self.current_routing.routing_source if self.current_routing else '', was_miss=True)
        if self.current_routing and self.current_routing.selected_review_item_id:
            self._notify_review_deck_observers(
                'training_outcome_recorded',
                review_item_id=self.current_routing.selected_review_item_id,
                result='FAIL',
            )
        item.pending_forced_stubborn_repeat = False
        self._save_items(items)
        self.review_storage.save_router_state(self.active_profile_id, self.router.export_profile_state(self.active_profile_id))
        if previous_frequency_state is not None and previous_frequency_state != item.frequency_state:
            self._notify_review_deck_observers(
                'item_frequency_state_changed',
                review_item_id=item.review_item_id,
                previous_frequency_state=previous_frequency_state,
                new_frequency_state=item.frequency_state,
            )
        self.review_storage.append_history(
            self.active_profile_id,
            event_to_dict(
                build_event(
                    'failure',
                    review_item_id=item.review_item_id,
                    routing=decision.routing_source,
                    reason=evaluation.reason_text,
                    outcome_channel=self._outcome_channel(decision.routing_source),
                )
            ),
        )
        return item, impact_summary, decision.routing_source


    def _d_failure_metadata(self, evaluation: EvaluationResult) -> dict[str, object]:
        metadata = evaluation.metadata if isinstance(evaluation.metadata, dict) else {}
        reconciled = metadata.get("reconciled") if isinstance(metadata.get("reconciled"), dict) else {}
        failure = reconciled.get("failure_explanation") if isinstance(reconciled.get("failure_explanation"), dict) else {}
        opening_locked = metadata.get("opening_locked") if isinstance(metadata.get("opening_locked"), dict) else {}
        return {
            "reason_code": failure.get("reason_code") or getattr(evaluation.reason_code, "value", str(evaluation.reason_code)),
            "template_id": failure.get("template_id"),
            "family_label": failure.get("family_label"),
            "max_practical_band_id": failure.get("max_practical_band_id"),
            "first_failure_band_id": failure.get("first_failure_band_id"),
            "toggle_state_required": failure.get("toggle_state_required"),
            "resolved_band_id": reconciled.get("resolved_band_id"),
            "would_pass_with_sharp_enabled": bool(reconciled.get("would_pass_with_sharp_enabled", False)),
            "opening_transition_classification": opening_locked.get("transition_classification"),
            "opening_selected_name": opening_locked.get("selected_opening_name"),
        }

    def _manual_inherited_failure_metadata(self, board_before_move: chess.Board, items: list[ReviewItem]) -> dict[str, object] | None:
        if not self.current_routing or self.current_routing.routing_source != RoutingSource.MANUAL_TARGET.value:
            return None
        if not self.active_review_plan or len(board_before_move.move_stack) >= len(self.active_review_plan.predecessor_path):
            return None
        if not self.current_review_item_id:
            return None
        parent = next((item for item in items if item.review_item_id == self.current_review_item_id), None)
        if parent is None or not parent.allow_below_threshold_reach:
            return None
        log_line(
            f"Manual reach-policy inheritance applied; parent={parent.review_item_id}; reason=route_construction_failure",
            tag='review',
        )
        return {
            'allow_below_threshold_reach': True,
            'predecessor_line_uci': parent.predecessor_line_uci,
            'manual_parent_review_item_id': parent.review_item_id,
            'manual_forced_player_color': parent.manual_forced_player_color,
        }

    def _capture_success_if_needed(self):
        if not self.current_review_item_id:
            return 'No review item changed; ordinary corpus pass.', 'ordinary_corpus_play'
        items = self._items()
        item = next((item for item in items if item.review_item_id == self.current_review_item_id), None)
        if item is None:
            return 'No review item changed; routed item no longer exists.', 'ordinary_corpus_play'
        previous_frequency_state = item.frequency_state
        apply_success(item, self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play')
        self.router.record_review_result(self.active_profile_id, self.current_routing.routing_source if self.current_routing else '', was_miss=False)
        if self.current_routing and self.current_routing.selected_review_item_id:
            self._notify_review_deck_observers(
                'training_outcome_recorded',
                review_item_id=self.current_routing.selected_review_item_id,
                result='PASS',
            )
        self._save_items(items)
        next_decision = self._select_routing_with_inspector(items, reason="post_success_routing")
        self.review_storage.save_router_state(self.active_profile_id, self.router.export_profile_state(self.active_profile_id))
        if previous_frequency_state != item.frequency_state:
            self._notify_review_deck_observers(
                'item_frequency_state_changed',
                review_item_id=item.review_item_id,
                previous_frequency_state=previous_frequency_state,
                new_frequency_state=item.frequency_state,
            )
        routed_by = self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play'
        self.review_storage.append_history(
            self.active_profile_id,
            event_to_dict(
                build_event(
                    'success',
                    review_item_id=item.review_item_id,
                    routing=routed_by,
                    outcome_channel=self._outcome_channel(routed_by),
                )
            ),
        )
        return f'Review item improved; next due at {item.due_at_utc}.', next_decision.routing_source

    @staticmethod
    def _outcome_channel(routing_reason: str) -> str:
        if routing_reason == 'ordinary_corpus_play':
            return 'ordinary_corpus_play'
        if routing_reason == 'srs_due_review':
            return 'spaced_repetition_review'
        if routing_reason == 'manual_target':
            return 'manual_target'
        return 'review_or_practice'

    def _manual_target_setup_override_feedback(self, board_before_move: chess.Board, move: chess.Move):
        if not self.current_routing or self.current_routing.routing_source != 'manual_target':
            return None
        if not self.active_review_plan or len(board_before_move.move_stack) >= len(self.active_review_plan.predecessor_path):
            return None
        if not self.current_review_item_id:
            return None
        items = self._items()
        item = next((candidate for candidate in items if candidate.review_item_id == self.current_review_item_id), None)
        if item is None or not item.allow_below_threshold_reach:
            return None
        expected = self.active_review_plan.predecessor_path[len(board_before_move.move_stack)]
        expected_side = 'white' if board_before_move.turn == chess.WHITE else 'black'
        if expected.get('side_to_move') != expected_side:
            return None
        if expected.get('move_uci') != move.uci():
            return None
        from .evaluation import AuthoritySource, CanonicalJudgment, OverlayLabel, ReasonCode
        return EvaluationResult(
            accepted=True,
            canonical_judgment=CanonicalJudgment.BETTER,
            overlay_label=OverlayLabel.GOOD,
            reason_code=ReasonCode.ENGINE_PASS,
            reason_text='Permitted reach move (manual target mode).',
            authority_source=AuthoritySource.NONE,
            move_uci=move.uci(),
            legal_move_confirmed=True,
            preferred_move_uci=expected.get('move_uci'),
            preferred_move_san=expected.get('san'),
            metadata={
                'manual_target_authorized_setup_move': True,
                'manual_target_reason': 'authorized_reach_path',
            },
        )

    def _handle_opponent_turn(self) -> None:
        pending = self.prepare_pending_opponent_action()
        if pending is None:
            return
        if pending.visible_delay_seconds > 0:
            time.sleep(pending.visible_delay_seconds)
        self.commit_pending_opponent_action()

    def prepare_pending_opponent_action(self) -> PendingOpponentAction | None:
        if self.state != SessionState.OPPONENT_TURN:
            return None
        if self.pending_opponent_action is not None:
            return self.pending_opponent_action
        board_before = self.board.board.copy(stack=True)
        scripted = self._planned_opponent_move(board_before)
        review_predecessor_bypassed = False
        if scripted is not None and self.developer_timing_overrides.enabled and self.developer_timing_overrides.force_ordinary_corpus_play:
            scripted = None
            review_predecessor_bypassed = True
        timing_context, native_components, adjusted_components = self._build_opponent_timing_context()
        effective_key = self._effective_timing_context_key(adjusted_components)
        fallback_keys_attempted = self._fallback_keys_for_components(adjusted_components)
        if scripted is not None:
            choice = scripted
        elif timing_context is None:
            choice = self.opponent.choose_move_with_context(self.board.board)
        else:
            choice = self.opponent.choose_move_with_runtime_context(self.board.board, timing_context=timing_context)
        choice = self._opening_locked_filter_opponent_choice(board_before, choice)
        visible_delay_seconds, visible_delay_reason = self._visible_opponent_delay_seconds(choice.sampled_think_time_seconds, choice=choice)
        choice = replace(
            choice,
            visible_delay_applied=visible_delay_seconds > 0,
            visible_delay_seconds=visible_delay_seconds if visible_delay_seconds > 0 else None,
            visible_delay_reason=visible_delay_reason,
        )
        self.pending_opponent_action = PendingOpponentAction(
            board_before=board_before,
            choice=choice,
            native_components=native_components,
            adjusted_components=adjusted_components,
            effective_key=effective_key,
            fallback_keys_attempted=fallback_keys_attempted,
            review_predecessor_bypassed=review_predecessor_bypassed,
            visible_delay_seconds=visible_delay_seconds,
        )
        return self.pending_opponent_action

    def commit_pending_opponent_action(self) -> bool:
        pending = self.pending_opponent_action
        if pending is None:
            return False
        self.pending_opponent_action = None
        choice = pending.choice
        self._consume_opponent_think_time(choice.sampled_think_time_seconds)
        move = choice.move
        self.last_opponent_choice = choice
        san = self.board.board.san(move)
        self.board.board.push(move)
        if self._opening_lock_active():
            transition = self._classify_opening_transition(self.board.board)
            if transition == OpeningTransitionClassification.LEFT_TO_UNNAMED:
                self.opening_locked_state.lock_released_by_opponent = True
                self.opening_locked_state.current_transition_state = OpeningLockedModeState.RELEASED_BY_OPPONENT
        self._record_path_move(pending.board_before, move)
        self._refresh_opening_name_state(reason='position_refresh')
        self._update_timing_diagnostics(
            choice,
            native_components=pending.native_components,
            adjusted_components=pending.adjusted_components,
            effective_key=pending.effective_key,
            fallback_keys_attempted=pending.fallback_keys_attempted,
            review_predecessor_bypassed=pending.review_predecessor_bypassed,
        )
        log_line(
            "timing_debug: "
            f"override_enabled={self.developer_timing_overrides.enabled}; "
            f"raw_native={pending.native_components}; "
            f"override_adjusted={pending.adjusted_components}; "
            f"lookup_mode={choice.timing_lookup_mode}; "
            f"bundle_invariant_time_control={choice.timing_bundle_invariant_time_control_id or 'n/a'}; "
            f"bundle_invariant_rating_band={choice.timing_bundle_invariant_rating_band or 'n/a'}; "
            f"effective_context={pending.effective_key or 'n/a'}; "
            f"fallback_keys={list(choice.timing_fallback_keys_attempted) or list(pending.fallback_keys_attempted)}; "
            f"overlay_matched={choice.timing_overlay_active}; "
            f"invariants_ignored_for_match={choice.timing_invariants_ignored_for_match}; "
            f"move_profile={choice.move_pressure_profile_id or 'n/a'}; "
            f"think_profile={choice.think_time_profile_id or 'n/a'}; "
            f"sampled_think={choice.sampled_think_time_seconds if choice.sampled_think_time_seconds is not None else 'n/a'}; "
            f"visible_delay={choice.visible_delay_seconds if choice.visible_delay_seconds is not None else 'none'}; "
            f"visible_delay_reason={choice.visible_delay_reason or 'none'}; "
            f"review_predecessor_bypassed={pending.review_predecessor_bypassed}",
            tag="corpus",
        )
        log_line(f'Opponent plays: {san}{self._format_opponent_choice_detail(choice)}', tag='corpus')
        if self._resolve_terminal_board_state():
            return True
        self.state = SessionState.PLAYER_TURN if self.board.turn() == self.player_color else SessionState.OPPONENT_TURN
        if self.state == SessionState.PLAYER_TURN:
            self._player_turn_started_at = time.monotonic()
        return True

    def _opening_locked_filter_opponent_choice(self, board_before: chess.Board, choice):
        if not self._opening_lock_active():
            return choice
        move = choice.move
        test_board = board_before.copy(stack=True)
        test_board.push(move)
        move_classification = self._classify_opening_transition(test_board)
        if move_classification != OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING:
            return choice
        valid_moves: list[tuple[chess.Move, float]] = []
        for candidate in getattr(choice, "candidate_summaries", ()) or ():
            uci = str(candidate.get("uci") or "").strip()
            if not uci:
                continue
            try:
                candidate_move = chess.Move.from_uci(uci)
            except ValueError:
                continue
            if candidate_move not in board_before.legal_moves:
                continue
            board_after = board_before.copy(stack=True)
            board_after.push(candidate_move)
            candidate_classification = self._classify_opening_transition(board_after)
            if candidate_classification == OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING:
                continue
            valid_moves.append((candidate_move, float(candidate.get("effective_weight") or candidate.get("raw_count") or 1.0)))
        if not valid_moves:
            if self.opening_locked_provider is not None and self.opening_locked_state.selected_opening_name:
                canonical = self.opening_locked_provider.canonical_continuation(
                    position_key=normalize_builder_position_key(board_before),
                    selected_opening_name=str(self.opening_locked_state.selected_opening_name),
                    max_plies=1,
                )
                if canonical.next_move_uci:
                    try:
                        fallback_move = chess.Move.from_uci(canonical.next_move_uci)
                    except ValueError:
                        fallback_move = None
                    if fallback_move is not None and fallback_move in board_before.legal_moves:
                        board_after = board_before.copy(stack=True)
                        board_after.push(fallback_move)
                        if self._classify_opening_transition(board_after) != OpeningTransitionClassification.LEFT_TO_OTHER_NAMED_OPENING:
                            return replace(
                                choice,
                                move=fallback_move,
                                selected_via="opening_locked_canonical_fallback",
                                corpus_lookup_reason_code="opening_locked_filtered_to_canonical",
                            )
            return choice
        legal_moves = [entry[0] for entry in valid_moves]
        weights = [max(0.0, entry[1]) for entry in valid_moves]
        filtered_move = random.choices(legal_moves, weights=weights, k=1)[0]
        return replace(
            choice,
            move=filtered_move,
            selected_via="opening_locked_filtered_candidate",
            corpus_lookup_reason_code="opening_locked_filtered_out_other_named",
        )

    def cancel_pending_opponent_action(self) -> None:
        self.pending_opponent_action = None

    def _planned_opponent_move(self, board: chess.Board):
        if not self.active_review_plan:
            return None
        if len(board.move_stack) >= len(self.active_review_plan.predecessor_path):
            return None
        expected = self.active_review_plan.predecessor_path[len(board.move_stack)]
        if expected['side_to_move'] != ('white' if board.turn == chess.WHITE else 'black'):
            return None
        move = chess.Move.from_uci(expected['move_uci'])
        if move not in board.legal_moves:
            return None
        from .opponent import OpponentMoveChoice
        return OpponentMoveChoice(move, expected.get('fen_before', board.fen()), 'review_predecessor_path', 'review_plan_reentry', expected.get('fen_before', board.fen()), 1, 1, 1, 1.0, 1, False, None, False, ({'uci': move.uci(), 'raw_count': 1, 'effective_weight': 1.0},))

    def _format_opponent_choice_detail(self, choice) -> str:
        parts = [f'via {choice.selected_via}', f'reason={choice.corpus_lookup_reason_code}', f'position={choice.normalized_position_key}', f'candidate_rows={choice.candidate_row_count}', f'legal_candidates={choice.legal_candidate_count}']
        if choice.timing_attempted_context_key:
            parts.append(f"timing_attempt={choice.timing_attempted_context_key}")
            parts.append(f"timing_fallback_keys={list(choice.timing_fallback_keys_attempted)}")
            parts.append(f"timing_lookup_mode={choice.timing_lookup_mode}")
            parts.append(f"timing_bundle_time_control={choice.timing_bundle_invariant_time_control_id or 'n/a'}")
            parts.append(f"timing_bundle_rating_band={choice.timing_bundle_invariant_rating_band or 'n/a'}")
        if choice.timing_overlay_active:
            parts.extend(
                [
                    f"timing_overlay=active",
                    f"context={choice.timing_context_key}",
                    f"fallback={choice.timing_fallback_used}",
                    f"invariants_ignored={choice.timing_invariants_ignored_for_match}",
                    f"move_profile={choice.move_pressure_profile_id}",
                    f"think_profile={choice.think_time_profile_id}",
                    f"sampled_think={choice.sampled_think_time_seconds:.2f}s" if choice.sampled_think_time_seconds is not None else "sampled_think=n/a",
                ]
            )
        elif choice.timing_overlay_available:
            parts.append("timing_overlay=available_unmatched")
        parts.extend(
            [
                f"overlay_source={choice.timing_overlay_source or 'absent'}",
                f"bundle_kind={choice.bundle_kind or 'unknown'}",
                f"exact_payload={choice.exact_payload_path or 'n/a'}",
                f"visible_delay={choice.visible_delay_seconds:.2f}s" if choice.visible_delay_applied and choice.visible_delay_seconds is not None else "visible_delay=none",
                f"visible_delay_reason={choice.visible_delay_reason or 'none'}",
            ]
        )
        if choice.selected_via == "cross_bundle_human_fallback":
            parts.extend(
                [
                    f"cross_bundle_mode={choice.cross_bundle_mode or 'n/a'}",
                    f"cross_bundle_queried={len(choice.cross_bundle_bundles_queried)}",
                    f"cross_bundle_matched={len(choice.cross_bundle_bundles_matched)}",
                    f"cross_bundle_candidate_rows={choice.cross_bundle_candidate_row_count}",
                    f"cross_bundle_merged_legal={choice.cross_bundle_merged_candidate_count}",
                    f"cross_bundle_selected_bundle={choice.cross_bundle_selected_bundle or 'unknown'}",
                ]
            )
        elif getattr(choice, "cross_bundle_mode", None):
            parts.append(f"cross_bundle_mode={choice.cross_bundle_mode}")
        return ' [' + ' | '.join(parts) + ']'

    def _visible_opponent_delay_seconds(self, sampled_seconds: float | None, *, choice=None):
        if sampled_seconds is None:
            return (0.0, "sampled_think_time_missing") if choice is not None else 0.0
        if choice is not None and not getattr(choice, "timing_overlay_active", False):
            if getattr(choice, "selected_via", None) == "review_predecessor_path":
                return 0.0, "review_predecessor_path"
            return 0.0, "no_overlay_match"
        scale = self.opponent_visible_delay_speed_multiplier
        min_seconds = self.opponent_visible_delay_min_seconds
        max_seconds = self.opponent_visible_delay_max_seconds
        overrides = self.developer_timing_overrides
        if overrides.enabled:
            scale *= overrides.visible_delay_scale
            if overrides.visible_delay_min_seconds is not None:
                min_seconds = overrides.visible_delay_min_seconds
            if overrides.visible_delay_max_seconds is not None:
                max_seconds = overrides.visible_delay_max_seconds
        scaled = max(0.0, sampled_seconds) * max(0.0, scale)
        if max_seconds <= 0:
            return 0.0, "delay_suppressed_by_dev_setting"
        result = max(min_seconds, min(max_seconds, scaled))
        if choice is None:
            return result
        return result, "applied"

    def _build_timed_state_from_bundle(self) -> TimedSessionState | None:
        provider = getattr(self.opponent, "bundle_provider", None)
        manifest = getattr(getattr(provider, "bundle", None), "manifest", None)
        if not isinstance(manifest, dict):
            return None
        is_final_bundle = self._is_final_canonical_bundle(manifest)
        time_control_id, _rating_band = self._timing_contract_metadata()
        initial_raw = manifest.get("initial_time_seconds", manifest.get("initial_seconds"))
        increment_raw = manifest.get("increment_seconds")
        if is_final_bundle and (not time_control_id or initial_raw is None or increment_raw is None):
            missing = [
                name
                for name, value in (
                    ("time_control_id", time_control_id),
                    ("initial_time_seconds", initial_raw),
                    ("increment_seconds", increment_raw),
                )
                if value is None or (isinstance(value, str) and not value.strip())
            ]
            raise ValueError(f"Final corpus bundle is missing required timing contract fields: {', '.join(missing)}")
        if not time_control_id:
            return None
        if initial_raw is None:
            return None
        if increment_raw is None:
            increment_raw = 0.0
        initial_seconds = float(initial_raw)
        increment_seconds = float(increment_raw)
        return TimedSessionState(
            time_control_id=time_control_id,
            initial_seconds=initial_seconds,
            increment_seconds=increment_seconds,
            white_remaining_ms=int(initial_seconds * 1000),
            black_remaining_ms=int(initial_seconds * 1000),
        )

    def _consume_player_think_time(self, *, additional_seconds: float = 0.0) -> None:
        if self.timed_state is None or self._player_turn_started_at is None:
            return
        elapsed = max(0.0, time.monotonic() - self._player_turn_started_at) + max(0.0, additional_seconds)
        is_white = self.player_color == chess.WHITE
        if is_white:
            self.timed_state.white_remaining_ms = max(0, self.timed_state.white_remaining_ms - int(elapsed * 1000))
        else:
            self.timed_state.black_remaining_ms = max(0, self.timed_state.black_remaining_ms - int(elapsed * 1000))
        self.timed_state.previous_opponent_think_seconds = elapsed
        if self.timed_state.increment_seconds > 0:
            if is_white:
                self.timed_state.white_remaining_ms += int(self.timed_state.increment_seconds * 1000)
            else:
                self.timed_state.black_remaining_ms += int(self.timed_state.increment_seconds * 1000)

    def displayed_clock_seconds(self, now: float | None = None) -> tuple[float | None, float | None]:
        if self.timed_state is None:
            return None, None
        white_remaining_ms = self.timed_state.white_remaining_ms
        black_remaining_ms = self.timed_state.black_remaining_ms
        if self.state == SessionState.PLAYER_TURN and self._player_turn_started_at is not None:
            timestamp = time.monotonic() if now is None else now
            elapsed_ms = int(max(0.0, timestamp - self._player_turn_started_at) * 1000)
            if self.player_color == chess.WHITE:
                white_remaining_ms = max(0, white_remaining_ms - elapsed_ms)
            else:
                black_remaining_ms = max(0, black_remaining_ms - elapsed_ms)
        return white_remaining_ms / 1000.0, black_remaining_ms / 1000.0

    def _consume_opponent_think_time(self, sampled_seconds: float | None) -> None:
        if self.timed_state is None:
            return
        think_seconds = 0.2 if sampled_seconds is None else max(0.0, sampled_seconds)
        is_opponent_white = self.player_color == chess.BLACK
        if is_opponent_white:
            self.timed_state.white_remaining_ms = max(0, self.timed_state.white_remaining_ms - int(think_seconds * 1000))
            if self.timed_state.increment_seconds > 0:
                self.timed_state.white_remaining_ms += int(self.timed_state.increment_seconds * 1000)
        else:
            self.timed_state.black_remaining_ms = max(0, self.timed_state.black_remaining_ms - int(think_seconds * 1000))
            if self.timed_state.increment_seconds > 0:
                self.timed_state.black_remaining_ms += int(self.timed_state.increment_seconds * 1000)
        self.timed_state.previous_player_think_seconds = think_seconds

    def _build_native_opponent_timing_context_components(self) -> dict[str, object] | None:
        if self.timed_state is None:
            return None
        opponent_remaining_ms = self.timed_state.white_remaining_ms if self.player_color == chess.BLACK else self.timed_state.black_remaining_ms
        remaining_seconds = opponent_remaining_ms / 1000.0
        _time_control_id, rating_band = self._timing_contract_metadata()
        return {
            "time_control_id": self.timed_state.time_control_id,
            "mover_elo_band": rating_band or "unknown",
            "remaining_ratio": remaining_seconds / max(1.0, self.timed_state.initial_seconds),
            "remaining_seconds": remaining_seconds,
            "prev_opp_think_seconds": self.timed_state.previous_opponent_think_seconds,
            "opening_ply": len(self.board.board.move_stack) + 1,
        }

    def _apply_timing_override_components(self, native_components: dict[str, object] | None) -> dict[str, object] | None:
        if native_components is None:
            return None
        overrides = self.developer_timing_overrides
        if not overrides.enabled:
            return dict(native_components)
        context = dict(native_components)
        if overrides.force_time_control_id != "Auto":
            context["time_control_id"] = overrides.force_time_control_id
        if overrides.force_mover_elo_band != "Auto":
            context["mover_elo_band"] = overrides.force_mover_elo_band
        if overrides.force_clock_pressure_bucket != "Auto":
            context["clock_pressure_bucket_override"] = overrides.force_clock_pressure_bucket
        if overrides.force_prev_opp_think_bucket != "Auto":
            context["prev_opp_think_bucket_override"] = overrides.force_prev_opp_think_bucket
        if overrides.force_opening_ply_band != "Auto":
            context["opening_ply_band_override"] = overrides.force_opening_ply_band
        return context

    def _build_opponent_timing_context(self) -> tuple[dict[str, object] | None, dict[str, object] | None, dict[str, object] | None]:
        native_components = self._build_native_opponent_timing_context_components()
        adjusted_components = self._apply_timing_override_components(native_components)
        return adjusted_components, native_components, adjusted_components

    def overlay_key_dimensions(self) -> dict[str, list[str]]:
        provider = getattr(self.opponent, "bundle_provider", None)
        overlay = getattr(getattr(provider, "bundle", None), "overlay", None)
        context_map = getattr(overlay, "context_profile_map", {})
        if not isinstance(context_map, dict):
            return parse_overlay_key_dimensions([])
        return parse_overlay_key_dimensions(list(context_map.keys()))

    def update_developer_timing_overrides(self, settings: DeveloperTimingOverrideState) -> DeveloperTimingOverrideState:
        self.developer_timing_overrides = self.developer_timing_store.save(settings)
        return self.developer_timing_overrides

    def reset_developer_timing_overrides(self) -> DeveloperTimingOverrideState:
        return self.update_developer_timing_overrides(DeveloperTimingOverrideState.disabled_defaults())

    def _effective_timing_context_key(self, components: dict[str, object] | None) -> str | None:
        if components is None:
            return None
        bundle = getattr(getattr(self.opponent, "bundle_provider", None), "bundle", None)
        lookup_mode = getattr(bundle, "timing_lookup_mode", "full_key")
        dynamic_context = DynamicTimingContext(
            clock_pressure_bucket=str(components.get("clock_pressure_bucket_override") or bucket_clock_pressure(float(components.get("remaining_ratio", 1.0)))),
            prev_opp_think_bucket=str(components.get("prev_opp_think_bucket_override") or bucket_prev_opp_think(components.get("prev_opp_think_seconds"))),
            opening_ply_band=str(components.get("opening_ply_band_override") or bucket_opening_ply_band(int(components.get("opening_ply", 1)))),
        )
        if lookup_mode == "reduced_dynamic":
            return dynamic_context.key()
        context = TimingContext(
            time_control_id=str(components.get("time_control_id", "unknown")),
            mover_elo_band=str(components.get("mover_elo_band", "unknown")),
            clock_pressure_bucket=dynamic_context.clock_pressure_bucket,
            prev_opp_think_bucket=dynamic_context.prev_opp_think_bucket,
            opening_ply_band=dynamic_context.opening_ply_band,
        )
        return context.key()

    def _fallback_keys_for_components(self, components: dict[str, object] | None) -> tuple[str, ...]:
        if components is None:
            return ()
        bundle = getattr(getattr(self.opponent, "bundle_provider", None), "bundle", None)
        lookup_mode = getattr(bundle, "timing_lookup_mode", "full_key")
        dynamic_context = DynamicTimingContext(
            clock_pressure_bucket=str(components.get("clock_pressure_bucket_override") or bucket_clock_pressure(float(components.get("remaining_ratio", 1.0)))),
            prev_opp_think_bucket=str(components.get("prev_opp_think_bucket_override") or bucket_prev_opp_think(components.get("prev_opp_think_seconds"))),
            opening_ply_band=str(components.get("opening_ply_band_override") or bucket_opening_ply_band(int(components.get("opening_ply", 1)))),
        )
        if lookup_mode == "reduced_dynamic":
            return tuple(fallback_keys_for_dynamic_context(dynamic_context))
        context = TimingContext(
            time_control_id=str(components.get("time_control_id", "unknown")),
            mover_elo_band=str(components.get("mover_elo_band", "unknown")),
            clock_pressure_bucket=dynamic_context.clock_pressure_bucket,
            prev_opp_think_bucket=dynamic_context.prev_opp_think_bucket,
            opening_ply_band=dynamic_context.opening_ply_band,
        )
        return tuple(fallback_keys_for_context(context))

    def _normalize_overlay_source(self, source: str | None) -> str:
        mapping = {"inline_json": "inline manifest", "json_file": "json_file", "behavioral_profile_set_sqlite": "behavioral_profile_set_sqlite", "absent": "absent"}
        return mapping.get(source or "absent", source or "absent")

    def _update_timing_diagnostics(self, choice, *, native_components: dict[str, object] | None, adjusted_components: dict[str, object] | None, effective_key: str | None, fallback_keys_attempted: tuple[str, ...], review_predecessor_bypassed: bool) -> None:
        source_map = {
            "review_predecessor_path": "review predecessor path",
            "stockfish_fallback": "stockfish fallback",
            "cross_bundle_human_fallback": "cross-bundle human fallback",
            "random_legal_fallback": "random fallback",
        }
        bundle = getattr(getattr(self.opponent, "bundle_provider", None), "bundle", None)
        inferred_lookup_mode = getattr(bundle, "timing_lookup_mode", "full_key")
        runtime_lookup_mode = choice.timing_lookup_mode
        if not choice.timing_attempted_context_key and not choice.timing_fallback_keys_attempted:
            runtime_lookup_mode = inferred_lookup_mode
        self.live_timing_debug_state = LiveTimingDebugState(
            bundle_path=str(self.runtime_context.config.corpus_bundle_dir) if self.runtime_context.config.corpus_bundle_dir else None,
            overlay_source=self._normalize_overlay_source(choice.timing_overlay_source),
            overlay_available=bool(choice.timing_overlay_available),
            raw_runtime_context_components={"native": native_components, "override_adjusted": adjusted_components},
            effective_context_key=choice.timing_attempted_context_key or effective_key,
            fallback_keys_attempted=tuple(choice.timing_fallback_keys_attempted) if choice.timing_fallback_keys_attempted else tuple(fallback_keys_attempted),
            matched_context_key=choice.timing_context_key if choice.timing_overlay_active else None,
            lookup_mode=runtime_lookup_mode,
            bundle_invariant_time_control_id=choice.timing_bundle_invariant_time_control_id,
            bundle_invariant_rating_band=choice.timing_bundle_invariant_rating_band,
            invariants_ignored_for_match=choice.timing_invariants_ignored_for_match,
            fallback_used=bool(choice.timing_fallback_used),
            move_pressure_profile_id=choice.move_pressure_profile_id,
            think_time_profile_id=choice.think_time_profile_id,
            sampled_think_time_seconds=choice.sampled_think_time_seconds,
            visible_delay_applied_seconds=choice.visible_delay_seconds if choice.visible_delay_applied else None,
            visible_delay_reason=choice.visible_delay_reason or "none",
            last_opponent_source=source_map.get(choice.selected_via, "ordinary corpus"),
            review_predecessor_bypassed=review_predecessor_bypassed,
        )

    def _timing_summary_text(self) -> str:
        if self.timed_state is None:
            return " | Opponent timing: off"
        white = self.timed_state.white_remaining_ms / 1000.0
        black = self.timed_state.black_remaining_ms / 1000.0
        debug_state = self.live_timing_debug_state
        if not debug_state.overlay_available:
            timing_status = "timed"
        elif debug_state.matched_context_key is None:
            timing_status = "timed"
        elif debug_state.fallback_used:
            timing_status = "active"
        else:
            timing_status = "active"
        sampled = debug_state.sampled_think_time_seconds
        think_text = f"{sampled:.2f}s" if isinstance(sampled, float) else "n/a"
        return (
            f" | Opponent timing: {timing_status}"
            f" | Opponent think: {think_text}"
            f" | Clocks W/B: {white:.1f}s/{black:.1f}s"
        )

    def _resolve_fail(self) -> None:
        log_line('FAIL', tag='evaluation')
        if self.last_outcome is not None:
            log_line(self.last_outcome.reason, tag='evaluation')
            if self.last_outcome.preferred_move:
                log_line(f'Preferred move: {self.last_outcome.preferred_move}', tag='evaluation')
            if self.last_outcome.punishing_reply_san or self.last_outcome.punishing_reply_uci:
                log_line(f'Punishing reply: {self.last_outcome.punishing_reply_san or self.last_outcome.punishing_reply_uci}', tag='evaluation')
            log_line(f'Routing reason: {self.last_outcome.routing_reason}', tag='review')
            log_line(f'Next run: {self.last_outcome.next_routing_reason}', tag='review')
        log_line('Restarting training game after acknowledgement in GUI or caller control.', tag='startup')
        self.state = SessionState.RESTART_PENDING

    def _resolve_success(self) -> None:
        log_line('SUCCESS', tag='evaluation')
        if self.last_outcome is not None:
            log_line(self.last_outcome.reason, tag='evaluation')
            log_line(f'Routing reason: {self.last_outcome.routing_reason}', tag='review')
            log_line(f'Profile: {self.last_outcome.profile_name}', tag='review')
        log_line('Opening window cleared. Restarting training game after acknowledgement in GUI or caller control.', tag='startup')
        self.state = SessionState.RESTART_PENDING

    def _resolve_authority_unavailable(self) -> None:
        log_line('AUTHORITY UNAVAILABLE', tag='error')
        if self.last_outcome is not None:
            log_line(self.last_outcome.reason, tag='evaluation')
        log_line('Run paused explicitly because engine authority is unavailable; no fail was recorded.', tag='error')
        self.state = SessionState.RESTART_PENDING

    def _print_new_game_banner(self) -> None:
        log_line('=== New Training Game ===', tag='startup')
        log_line('You are WHITE' if self.player_color == chess.WHITE else 'You are BLACK', tag='startup')

    def _print_startup_summary(self) -> None:
        color_name = 'WHITE' if self.player_color == chess.WHITE else 'BLACK'
        for line in self.runtime_context.startup_status(mode=self.mode.upper(), user_color=color_name).lines:
            log_line(line, tag='evaluation')

    def _print_evaluation_feedback(self, evaluation: EvaluationResult) -> None:
        for line in format_evaluation_feedback(evaluation):
            log_line(line, tag='evaluation')

    def close(self) -> None:
        self.cancel_pending_opponent_action()
        self.opponent.close()
        self.evaluator.engine_authority.close()
