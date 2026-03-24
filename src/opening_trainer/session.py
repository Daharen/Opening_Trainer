from __future__ import annotations

import random
from dataclasses import asdict
from pathlib import Path

import chess

from .board import GameBoard
from .bundle_corpus import normalize_builder_position_key
from .corpus import load_artifact
from .evaluation import CanonicalJudgment, EngineAuthority, EvaluatorConfig, OpeningBookAuthority, format_evaluation_feedback
from .evaluator import MoveEvaluator
from .models import EvaluationResult, MoveHistoryEntry, SessionOutcome, SessionState, SessionView
from .opponent import OpponentProvider
from .review.models import ReviewItem, ReviewPathMove, RoutingDecision
from .review.profile_service import ProfileService
from .review.router import ReviewRouter
from .review.scheduler import apply_failure, apply_success
from .review.storage import ReviewStorage
from .runtime import (
    RuntimeContext,
    RuntimeOverrides,
    bundle_retained_ply_depth_from_metadata,
    inspect_corpus_bundle,
    load_runtime_config,
    max_supported_player_moves_from_retained_plies,
)
from .settings import CONSERVATIVE_FALLBACK_MAX_DEPTH, TrainerSettings, TrainerSettingsStore
from .session_events import build_event, event_to_dict
from .session_logging import log_line


class TrainingSession:
    restart_delay_ms = 900

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
        )
        self.evaluator = MoveEvaluator(
            config=self.config,
            book_authority=OpeningBookAuthority(self.runtime_context.book.path if self.runtime_context.book.available else None),
            engine_authority=EngineAuthority(self.config),
        )
        self.required_player_moves = self.config.active_envelope_player_moves
        self.review_storage = review_storage or ReviewStorage()
        self.profile_service = ProfileService(self.review_storage)
        self.router = ReviewRouter()
        self.active_profile_id = self.profile_service.get_active_profile_id()
        self.settings_store = TrainerSettingsStore(self.review_storage.root)
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
        self._apply_settings(self.settings)

    def max_supported_training_depth(self) -> int:
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
            manifest = getattr(getattr(provider, 'bundle', None), 'metadata', None)
            manifest = getattr(manifest, 'manifest', None)
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
        self.required_player_moves = self.settings.active_training_ply_depth
        self.config = type(self.config)(**{**self.config.snapshot(), 'active_envelope_player_moves': self.required_player_moves, 'good_moves_acceptable': self.settings.good_moves_acceptable})
        self.evaluator.config = self.config
        self.evaluator.overlay_classifier.config = self.config
        self.evaluator.engine_authority.config = self.config

    def _profile_name(self) -> str:
        return self.review_storage.load_profile_meta(self.active_profile_id).display_name

    def _items(self):
        return self.review_storage.load_items(self.active_profile_id)

    def _save_items(self, items):
        self.review_storage.save_items(self.active_profile_id, items)

    def start_new_game(self) -> SessionView:
        self.state = SessionState.STARTING_GAME
        self.board.reset()
        self.player_color = random.choice([chess.WHITE, chess.BLACK])
        self.player_move_count = 0
        self.last_evaluation = None
        self.last_outcome = None
        self.last_opponent_choice = None
        self.run_path = []
        items = self._items()
        self.current_routing = self.router.select(self.active_profile_id, items)
        self.current_review_item_id = self.current_routing.selected_review_item_id
        self.active_review_plan = self.current_routing.review_plan
        self._print_new_game_banner()
        log_line(self.opponent.status_message, tag='startup')
        self._print_startup_summary()
        log_line(f'Routing: {self.current_routing.selection_explanation}', tag='review')
        if self.board.turn() == self.player_color:
            self.state = SessionState.PLAYER_TURN
        else:
            self.state = SessionState.OPPONENT_TURN
            self.advance_until_user_turn()
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

    def submit_user_move_uci(self, move_uci: str) -> SessionView:
        return self._submit_user_move(move_uci.strip())

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
        bundle_dir = self.runtime_context.config.corpus_bundle_dir
        if bundle_dir:
            provider = getattr(self.opponent, 'bundle_provider', None)
            manifest = getattr(getattr(provider, 'bundle', None), 'metadata', None)
            manifest = getattr(manifest, 'manifest', None)
            if isinstance(manifest, dict):
                band = manifest.get('target_rating_band') or manifest.get('rating_band') or manifest.get('elo_band')
                retained = manifest.get('retained_ply_depth')
                band_text = self._format_rating_band(band) or self._bundle_name_fallback(bundle_dir)
                retained_text = f' | Retained depth: {retained}' if retained is not None else ''
                return f'Corpus: {band_text}{retained_text}'
            return f'Corpus: {self._bundle_name_fallback(bundle_dir)}'
        artifact_path = self.runtime_context.config.corpus_artifact_path
        if artifact_path:
            try:
                artifact = load_artifact(artifact_path)
                band_text = self._format_rating_band(getattr(artifact, 'target_rating_band', None)) or 'artifact'
                return f'Corpus: {band_text} | Retained depth: {artifact.retained_ply_depth}'
            except Exception:
                return 'Corpus: legacy artifact'
        return 'Corpus: fallback / no bundle metadata'

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

    def _submit_user_move(self, move_str: str) -> SessionView:
        if self.state != SessionState.PLAYER_TURN:
            raise RuntimeError('Cannot submit a user move when the session is not awaiting player input.')
        if not self.board.is_legal(move_str):
            log_line('Illegal move. Try again.', tag='evaluation')
            return self.get_view()
        board_before_move = self.board.board.copy(stack=True)
        pre_fail_fen = board_before_move.fen()
        move = self.board.push(move_str)
        self._record_path_move(board_before_move, move)
        self.player_move_count += 1
        evaluation = self.evaluator.evaluate(board_before_move, move, self.player_move_count)
        self.last_evaluation = evaluation
        self._print_evaluation_feedback(evaluation)
        if evaluation.canonical_judgment == CanonicalJudgment.AUTHORITY_UNAVAILABLE:
            self.last_outcome = SessionOutcome(False, evaluation.reason_text, None, evaluation, 'authority_unavailable', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', 'ordinary_corpus_play', self._profile_name(), 'No review item recorded because the authority was unavailable.')
            self.state = SessionState.AUTHORITY_UNAVAILABLE_RESOLUTION
            self._resolve_authority_unavailable()
            return self.get_view()
        if not evaluation.accepted:
            post_fail_fen = self.board.board.fen()
            punishing_reply_uci, punishing_reply_san = self._lookup_punishing_reply()
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
                player_color=self.player_color,
            )
            self.state = SessionState.FAIL_RESOLUTION
            self._resolve_fail()
            return self.get_view()
        if self._resolve_terminal_board_state():
            return self.get_view()
        if self.player_move_count >= self.required_player_moves:
            impact_summary, next_reason = self._capture_success_if_needed()
            self.last_outcome = SessionOutcome(True, f'Completed {self.required_player_moves} accepted player moves inside the opening window.', None, evaluation, 'pass', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', next_reason, self._profile_name(), impact_summary)
            self.state = SessionState.SUCCESS_RESOLUTION
            self._resolve_success()
            return self.get_view()
        self.state = SessionState.OPPONENT_TURN
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
            self._resolve_success()
            return True
        impact_summary = 'Terminal game state reached; no additional review item recorded.'
        next_reason = self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play'
        self.last_outcome = SessionOutcome(False, reason, None, self.last_evaluation, 'fail', self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play', next_reason, self._profile_name(), impact_summary, player_color=self.player_color)
        self.state = SessionState.FAIL_RESOLUTION
        self._resolve_fail()
        return True

    def _lookup_punishing_reply(self) -> tuple[str | None, str | None]:
        board_after_fail = self.board.board.copy(stack=True)
        engine_authority = getattr(self.evaluator, 'engine_authority', None)
        best_reply = getattr(engine_authority, 'best_reply', None)
        if best_reply is None:
            return None, None
        try:
            return best_reply(board_after_fail)
        except Exception:
            return None, None

    def _capture_failure(self, board_before_move: chess.Board, evaluation: EvaluationResult):
        items = self._items()
        position_key = normalize_builder_position_key(board_before_move)
        side = 'white' if board_before_move.turn == chess.WHITE else 'black'
        existing = next((item for item in items if item.position_key == position_key and item.side_to_move == side), None)
        accepted = list(evaluation.metadata.get('candidate_moves', [])) if isinstance(evaluation.metadata, dict) else []
        line_preview = ' '.join(move.san for move in self.run_path[-6:])
        if existing is None:
            item = ReviewItem.create(self.active_profile_id, position_key, board_before_move.fen(), side, evaluation.reason_text, evaluation.preferred_move_uci, accepted, self.run_path)
            items.append(item)
            impact_summary = 'Created new review item and scheduled immediate retry.'
        else:
            item = apply_failure(existing, evaluation.reason_text, evaluation.preferred_move_uci, [asdict(move) for move in self.run_path], line_preview, self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play')
            impact_summary = f'Updated review item; urgency is now {item.urgency_tier}.'
        self._save_items(items)
        decision = self.router.immediate_retry(self.active_profile_id, item)
        self.review_storage.append_history(self.active_profile_id, event_to_dict(build_event('failure', review_item_id=item.review_item_id, routing=decision.routing_source, reason=evaluation.reason_text)))
        return item, impact_summary, decision.routing_source

    def _capture_success_if_needed(self):
        if not self.current_review_item_id:
            return 'No review item changed; ordinary corpus pass.', 'ordinary_corpus_play'
        items = self._items()
        item = next((item for item in items if item.review_item_id == self.current_review_item_id), None)
        if item is None:
            return 'No review item changed; routed item no longer exists.', 'ordinary_corpus_play'
        apply_success(item, self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play')
        self._save_items(items)
        next_decision = self.router.select(self.active_profile_id, items)
        self.review_storage.append_history(self.active_profile_id, event_to_dict(build_event('success', review_item_id=item.review_item_id, routing=self.current_routing.routing_source if self.current_routing else 'ordinary_corpus_play')))
        return f'Review item improved; next due at {item.due_at_utc}.', next_decision.routing_source

    def _handle_opponent_turn(self) -> None:
        board_before = self.board.board.copy(stack=True)
        scripted = self._planned_opponent_move(board_before)
        choice = scripted or self.opponent.choose_move_with_context(self.board.board)
        move = choice.move
        self.last_opponent_choice = choice
        san = self.board.board.san(move)
        self.board.board.push(move)
        self._record_path_move(board_before, move)
        log_line(f'Opponent plays: {san}{self._format_opponent_choice_detail(choice)}', tag='corpus')
        if self._resolve_terminal_board_state():
            return
        self.state = SessionState.PLAYER_TURN if self.board.turn() == self.player_color else SessionState.OPPONENT_TURN

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
        return ' [' + ' | '.join([f'via {choice.selected_via}', f'reason={choice.corpus_lookup_reason_code}', f'position={choice.normalized_position_key}', f'candidate_rows={choice.candidate_row_count}', f'legal_candidates={choice.legal_candidate_count}']) + ']'

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
