from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import chess
import chess.engine

from .bundle_corpus import (
    BuilderAggregateParseError,
    normalize_builder_position_key,
)
from .corpus import DEFAULT_ARTIFACT_PATH, load_artifact, normalize_position_key
from .evaluation import EvaluatorConfig
from .evaluation.engine_process import launch_engine, shutdown_engine
from .runtime import corpus_status_detail
from .timing import (
    DynamicTimingContext,
    TimingConditionedCorpusBundleLoader,
    TimingContext,
    apply_move_pressure_modulation,
    bucket_clock_pressure,
    bucket_opening_ply_band,
    bucket_prev_opp_think,
    fallback_keys_for_context,
    fallback_keys_for_dynamic_context,
    sample_think_time_seconds,
)


@dataclass(frozen=True)
class OpponentMoveChoice:
    move: chess.Move
    position_key: str
    selected_via: str
    corpus_lookup_reason_code: str
    normalized_position_key: str
    candidate_row_count: int
    legal_candidate_count: int
    raw_count: int
    effective_weight: float
    total_observed_count: int
    sparse: bool
    sparse_reason: str | None
    fallback_applied: bool
    candidate_summaries: tuple[dict[str, object], ...]
    timing_overlay_active: bool = False
    timing_context_key: str | None = None
    timing_fallback_used: bool = False
    move_pressure_profile_id: str | None = None
    think_time_profile_id: str | None = None
    sampled_think_time_seconds: float | None = None
    modulation_summary: dict[str, object] | None = None
    timing_overlay_available: bool = False
    timing_overlay_source: str | None = None
    bundle_kind: str | None = None
    exact_payload_path: str | None = None
    visible_delay_applied: bool = False
    visible_delay_seconds: float | None = None
    timing_attempted_context_key: str | None = None
    timing_fallback_keys_attempted: tuple[str, ...] = ()
    visible_delay_reason: str | None = None
    timing_lookup_mode: str = "full_key"
    timing_bundle_invariant_time_control_id: str | None = None
    timing_bundle_invariant_rating_band: str | None = None
    timing_invariants_ignored_for_match: bool = False


class OpponentMoveProvider(Protocol):
    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        ...


class RandomOpponentProvider:
    def __init__(self, rng=None):
        self.rng = rng or random

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        legal_moves = list(board.legal_moves)
        if not legal_moves:
            raise ValueError("RandomOpponentProvider received a position with no legal moves.")
        move = self.rng.choice(legal_moves)
        return OpponentMoveChoice(
            move=move,
            position_key=normalize_position_key(board),
            selected_via="random_legal_move",
            corpus_lookup_reason_code="random_fallback_used_after_all_failures",
            normalized_position_key=normalize_position_key(board),
            candidate_row_count=0,
            legal_candidate_count=len(legal_moves),
            raw_count=0,
            effective_weight=1.0,
            total_observed_count=0,
            sparse=False,
            sparse_reason=None,
            fallback_applied=False,
            candidate_summaries=tuple({"uci": legal.uci(), "raw_count": 0, "effective_weight": 1.0} for legal in legal_moves),
        )


class StockfishOpponentProvider:
    def __init__(self, config: EvaluatorConfig):
        self.config = config
        self._engine: chess.engine.SimpleEngine | None = None

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        engine = self._ensure_engine()
        info = engine.play(
            board,
            chess.engine.Limit(depth=self.config.engine_depth, time=self.config.engine_time_limit_seconds),
        )
        move = info.move
        if move is None or move not in board.legal_moves:
            raise LookupError("Stockfish fallback returned no legal move.")
        return OpponentMoveChoice(
            move=move,
            position_key=normalize_position_key(board),
            selected_via="stockfish_fallback",
            corpus_lookup_reason_code="stockfish_fallback_used_after_corpus_miss",
            normalized_position_key=normalize_position_key(board),
            candidate_row_count=0,
            legal_candidate_count=1,
            raw_count=0,
            effective_weight=1.0,
            total_observed_count=0,
            sparse=False,
            sparse_reason=None,
            fallback_applied=True,
            candidate_summaries=({"uci": move.uci(), "raw_count": 0, "effective_weight": 1.0},),
        )

    def _ensure_engine(self) -> chess.engine.SimpleEngine:
        if self._engine is None:
            self._engine = launch_engine(self.config)
        return self._engine

    def _close_engine(self) -> None:
        if self._engine is None:
            return
        shutdown_engine(self._engine)
        self._engine = None

    def close(self) -> None:
        self._close_engine()


class CorpusBackedOpponentProvider:
    def __init__(self, artifact_path: str | Path = DEFAULT_ARTIFACT_PATH, rng=None):
        self.artifact_path = Path(artifact_path)
        self.rng = rng or random
        self.artifact = load_artifact(self.artifact_path)
        self.position_index = {position.position_key: position for position in self.artifact.positions}

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        lookup_chain = self._lookup_chain(board)
        for selected_via, position_key in lookup_chain:
            position = self.position_index.get(position_key)
            if position is None:
                continue
            legal_candidates = [candidate for candidate in position.candidate_moves if chess.Move.from_uci(candidate.uci) in board.legal_moves]
            if not legal_candidates:
                continue
            weights = [candidate.effective_weight for candidate in legal_candidates]
            selected = self.rng.choices(legal_candidates, weights=weights, k=1)[0]
            return OpponentMoveChoice(
                move=chess.Move.from_uci(selected.uci),
                position_key=position.position_key,
                selected_via=selected_via,
                corpus_lookup_reason_code="legacy_corpus_hit",
                normalized_position_key=normalize_position_key(board),
                candidate_row_count=len(position.candidate_moves),
                legal_candidate_count=len(legal_candidates),
                raw_count=selected.raw_count,
                effective_weight=selected.effective_weight,
                total_observed_count=position.total_observed_count,
                sparse=position.sparse,
                sparse_reason=position.sparse_reason,
                fallback_applied=selected_via != "exact_position",
                candidate_summaries=tuple(
                    {
                        "uci": candidate.uci,
                        "raw_count": candidate.raw_count,
                        "effective_weight": candidate.effective_weight,
                    }
                    for candidate in legal_candidates
                ),
            )
        raise LookupError(
            f"No corpus-backed move available for position {normalize_position_key(board)} using artifact {self.artifact_path}."
        )

    def _lookup_chain(self, board: chess.Board) -> list[tuple[str, str]]:
        positions: list[tuple[str, str]] = [("exact_position", normalize_position_key(board))]
        rewind_board = board.copy(stack=True)
        fallback_index = 0
        while rewind_board.move_stack:
            rewind_board.pop()
            fallback_index += 1
            positions.append((f"prefix_backoff_{fallback_index}", normalize_position_key(rewind_board)))
        return positions


class BuilderAggregateOpponentProvider:
    def __init__(self, bundle_dir: str | Path, rng=None):
        self.bundle_dir = Path(bundle_dir)
        self.rng = rng or random
        self.last_lookup_diagnostic: str | None = None
        try:
            self.bundle = TimingConditionedCorpusBundleLoader().load(self.bundle_dir, rng=self.rng)
        except BuilderAggregateParseError as exc:
            self.last_lookup_diagnostic = f"reason_code={exc.reason_code}; detail={exc.detail}"
            raise

    def choose_move(self, board: chess.Board, timing_context: dict[str, object] | None = None) -> OpponentMoveChoice:
        position_key = normalize_builder_position_key(board)
        position = self.bundle.lookup_position(position_key)
        if position is None:
            diagnostic = f"reason_code=position_key_not_found; position_key={position_key}"
            self.last_lookup_diagnostic = diagnostic
            raise LookupError(diagnostic)

        legal_candidates: list[tuple[chess.Move, object]] = []
        rejected_uci: list[str] = []
        for candidate in position.candidates:
            try:
                move = chess.Move.from_uci(candidate.uci)
            except ValueError:
                rejected_uci.append(candidate.uci)
                continue
            if move not in board.legal_moves:
                rejected_uci.append(candidate.uci)
                continue
            legal_candidates.append((move, candidate))

        if position.candidate_row_count > 0 and not position.candidates:
            diagnostic = (
                "reason_code=position_row_found_but_no_supported_candidate_moves; "
                f"position_key={position_key}; candidate_rows_loaded={position.candidate_row_count}; legal_candidates=0"
            )
            self.last_lookup_diagnostic = diagnostic
            raise LookupError(diagnostic)

        if not legal_candidates:
            diagnostic = (
                "reason_code=position_row_found_but_all_candidate_moves_illegal; "
                f"position_key={position_key}; candidate_rows_loaded={position.candidate_row_count}; legal_candidates=0; "
                f"rejected_candidates={rejected_uci}"
            )
            self.last_lookup_diagnostic = diagnostic
            raise LookupError(diagnostic)

        weighted_candidates = [candidate for _, candidate in legal_candidates]
        base_weights = [(candidate.uci, float(max(0, candidate.raw_count))) for candidate in weighted_candidates]
        sampling_weights = [weight for _uci, weight in base_weights]
        timing_overlay_active = False
        timing_fallback_used = False
        timing_context_key = None
        move_pressure_profile_id = None
        think_time_profile_id = None
        sampled_think_time_seconds = None
        modulation_summary: dict[str, object] | None = None
        attempted_context_key: str | None = None
        fallback_keys_attempted: tuple[str, ...] = ()
        timing_lookup_mode = self.bundle.timing_lookup_mode
        timing_bundle_invariant_time_control_id = self.bundle.bundle_invariant_time_control_id
        timing_bundle_invariant_rating_band = self.bundle.bundle_invariant_rating_band
        timing_invariants_ignored_for_match = False
        if timing_context and self.bundle.timing_overlay_available:
            clock_pressure_bucket = str(timing_context.get("clock_pressure_bucket_override")) if timing_context.get("clock_pressure_bucket_override") else bucket_clock_pressure(float(timing_context.get("remaining_ratio", 1.0)))
            prev_opp_think_bucket = str(timing_context.get("prev_opp_think_bucket_override")) if timing_context.get("prev_opp_think_bucket_override") else bucket_prev_opp_think(timing_context.get("prev_opp_think_seconds"))
            opening_ply_band = str(timing_context.get("opening_ply_band_override")) if timing_context.get("opening_ply_band_override") else bucket_opening_ply_band(int(timing_context.get("opening_ply", 1)))
            context = TimingContext(
                time_control_id=str(timing_context.get("time_control_id", "unknown")),
                mover_elo_band=str(timing_context.get("mover_elo_band", "unknown")),
                clock_pressure_bucket=clock_pressure_bucket,
                prev_opp_think_bucket=prev_opp_think_bucket,
                opening_ply_band=opening_ply_band,
            )
            timing_context_key = context.key()
            if timing_lookup_mode == "reduced_dynamic":
                dynamic_context = DynamicTimingContext(
                    clock_pressure_bucket=context.clock_pressure_bucket,
                    prev_opp_think_bucket=context.prev_opp_think_bucket,
                    opening_ply_band=context.opening_ply_band,
                )
                attempted_context_key = dynamic_context.key()
                fallback_keys_attempted = tuple(fallback_keys_for_dynamic_context(dynamic_context))
            else:
                attempted_context_key = context.key()
                fallback_keys_attempted = tuple(fallback_keys_for_context(context))
            overlay = self.bundle.resolve_overlay(context)
            if overlay is not None:
                adjusted_weights, summary = apply_move_pressure_modulation(base_weights, overlay.move_pressure_profile, context.clock_pressure_bucket)
                if adjusted_weights:
                    sampling_weights = [weight for _uci, weight in adjusted_weights]
                timing_overlay_active = True
                timing_fallback_used = overlay.fallback_used
                timing_context_key = overlay.matched_key
                attempted_context_key = overlay.attempted_key
                fallback_keys_attempted = overlay.fallback_keys
                timing_lookup_mode = overlay.lookup_mode
                timing_invariants_ignored_for_match = overlay.invariants_ignored
                move_pressure_profile_id = overlay.move_pressure_profile.profile_id
                think_time_profile_id = overlay.think_time_profile.profile_id
                sampled_think_time_seconds = sample_think_time_seconds(
                    overlay.think_time_profile,
                    float(timing_context.get("remaining_seconds", 0.0)),
                    rng=self.rng,
                )
                modulation_summary = {
                    **summary,
                    "pre_top_moves": [f"{uci}:{weight:.4f}" for uci, weight in sorted(base_weights, key=lambda item: item[1], reverse=True)[:3]],
                    "post_top_moves": [f"{uci}:{weight:.4f}" for uci, weight in sorted(adjusted_weights, key=lambda item: item[1], reverse=True)[:3]],
                }
        selected = self.rng.choices(weighted_candidates, weights=sampling_weights, k=1)[0]
        selected_move = next(move for move, candidate in legal_candidates if candidate == selected)
        self.last_lookup_diagnostic = (
            "reason_code=corpus_hit; "
            f"position_key={position_key}; candidate_rows_loaded={position.candidate_row_count}; legal_candidates={len(legal_candidates)}"
        )
        return OpponentMoveChoice(
            move=selected_move,
            position_key=position_key,
            selected_via=("corpus_aggregate_bundle" if self.bundle.exact_corpus.metadata.payload_format == "jsonl" else self.bundle.exact_corpus.metadata.provider_label),
            corpus_lookup_reason_code="corpus_hit",
            normalized_position_key=position_key,
            candidate_row_count=position.candidate_row_count,
            legal_candidate_count=len(legal_candidates),
            raw_count=selected.raw_count,
            effective_weight=float(selected.raw_count),
            total_observed_count=position.total_observed_count,
            sparse=False,
            sparse_reason=None,
            fallback_applied=False,
            candidate_summaries=tuple(
                {
                    "uci": candidate.uci,
                    "raw_count": candidate.raw_count,
                    "effective_weight": float(candidate.raw_count),
                }
                for _, candidate in legal_candidates
            ),
            timing_overlay_active=timing_overlay_active,
            timing_context_key=timing_context_key,
            timing_fallback_used=timing_fallback_used,
            move_pressure_profile_id=move_pressure_profile_id,
            think_time_profile_id=think_time_profile_id,
            sampled_think_time_seconds=sampled_think_time_seconds,
            modulation_summary=modulation_summary,
            timing_overlay_available=self.bundle.timing_overlay_available,
            timing_overlay_source=self.bundle.overlay_source,
            bundle_kind=self.bundle.bundle_kind,
            exact_payload_path=str(self.bundle.exact_payload_path) if self.bundle.exact_payload_path is not None else None,
            timing_attempted_context_key=attempted_context_key,
            timing_fallback_keys_attempted=fallback_keys_attempted,
            timing_lookup_mode=timing_lookup_mode,
            timing_bundle_invariant_time_control_id=timing_bundle_invariant_time_control_id,
            timing_bundle_invariant_rating_band=timing_bundle_invariant_rating_band,
            timing_invariants_ignored_for_match=timing_invariants_ignored_for_match,
        )


class OpponentProvider:
    def __init__(self, artifact_path: str | Path | None = DEFAULT_ARTIFACT_PATH, bundle_dir: str | Path | None = None, evaluator_config: EvaluatorConfig | None = None, rng=None):
        self.rng = rng or random
        self.artifact_path = Path(artifact_path) if artifact_path is not None else None
        self.bundle_dir = Path(bundle_dir) if bundle_dir is not None else None
        self.evaluator_config = evaluator_config or EvaluatorConfig()
        self.random_provider = RandomOpponentProvider(rng=self.rng)
        self.stockfish_provider = StockfishOpponentProvider(self.evaluator_config)
        self.bundle_provider: BuilderAggregateOpponentProvider | None = None
        self.corpus_provider: CorpusBackedOpponentProvider | None = None
        self.mode = "random_fallback"
        self.status_message = "No compatible corpus source loaded; opponent provider will use Stockfish fallback before random legal fallback."
        self.last_choice: OpponentMoveChoice | None = None
        self.last_failure_reason: str | None = None

        if self.bundle_dir is not None:
            try:
                self.bundle_provider = BuilderAggregateOpponentProvider(self.bundle_dir, rng=self.rng)
                self.mode = "bundle"
                self.status_message = corpus_status_detail(self.bundle_dir)
            except Exception as exc:
                self.last_failure_reason = str(exc)
                self.status_message = f"{corpus_status_detail(self.bundle_dir)}; runtime will attempt legacy corpus, then Stockfish, then random legal fallback."

        if self.bundle_provider is None and self.artifact_path is not None and self.artifact_path.is_file():
            self.corpus_provider = CorpusBackedOpponentProvider(self.artifact_path, rng=self.rng)
            self.mode = "corpus"
            self.status_message = corpus_status_detail(self.artifact_path)

    def choose_move(self, board: chess.Board) -> chess.Move:
        self.last_choice = self.choose_move_with_context(board)
        return self.last_choice.move

    def choose_move_with_context(self, board: chess.Board) -> OpponentMoveChoice:
        return self.choose_move_with_runtime_context(board, timing_context=None)

    def choose_move_with_runtime_context(self, board: chess.Board, timing_context: dict[str, object] | None = None) -> OpponentMoveChoice:
        failures: list[str] = []
        normalized_position_key = normalize_builder_position_key(board)
        bundle = getattr(self.bundle_provider, "bundle", None)
        bundle_overlay_available = bool(getattr(bundle, "timing_overlay_available", False))
        bundle_overlay_source = getattr(bundle, "overlay_source", None)
        bundle_kind = getattr(bundle, "bundle_kind", None)
        bundle_exact_payload = str(getattr(bundle, "exact_payload_path", None)) if getattr(bundle, "exact_payload_path", None) is not None else None
        bundle_lookup_mode = str(getattr(bundle, "timing_lookup_mode", "full_key"))
        bundle_time_control = getattr(bundle, "bundle_invariant_time_control_id", None)
        bundle_rating_band = getattr(bundle, "bundle_invariant_rating_band", None)
        if self.bundle_provider is not None:
            try:
                choice = self.bundle_provider.choose_move(board, timing_context=timing_context)
                self.last_choice = choice
                return choice
            except LookupError as exc:
                failures.append(f"bundle lookup failed: {exc}")
            except BuilderAggregateParseError as exc:
                failures.append(
                    "bundle lookup failed: "
                    f"reason_code={exc.reason_code}; detail={exc.detail}; position_key={normalize_builder_position_key(board)}"
                )
        if self.corpus_provider is not None:
            try:
                choice = self.corpus_provider.choose_move(board)
                self.last_choice = choice
                return choice
            except LookupError as exc:
                failures.append(f"legacy corpus lookup failed: {exc}")
        try:
            choice = self.stockfish_provider.choose_move(board)
            choice = OpponentMoveChoice(
                move=choice.move,
                position_key=choice.position_key,
                selected_via=choice.selected_via,
                corpus_lookup_reason_code=choice.corpus_lookup_reason_code,
                normalized_position_key=normalized_position_key,
                candidate_row_count=self._extract_candidate_row_count(),
                legal_candidate_count=self._extract_legal_candidate_count(),
                raw_count=choice.raw_count,
                effective_weight=choice.effective_weight,
                total_observed_count=choice.total_observed_count,
                sparse=choice.sparse,
                sparse_reason="; ".join(failures) if failures else choice.sparse_reason,
                fallback_applied=choice.fallback_applied,
                candidate_summaries=choice.candidate_summaries,
                timing_overlay_available=bundle_overlay_available,
                timing_overlay_source=bundle_overlay_source,
                bundle_kind=bundle_kind,
                exact_payload_path=bundle_exact_payload,
                timing_lookup_mode=bundle_lookup_mode,
                timing_bundle_invariant_time_control_id=bundle_time_control,
                timing_bundle_invariant_rating_band=bundle_rating_band,
            )
            self.last_choice = choice
            return choice
        except (LookupError, FileNotFoundError, chess.engine.EngineError, OSError) as exc:
            failures.append(f"Stockfish fallback failed: {exc}")
        choice = self.random_provider.choose_move(board)
        if failures:
            choice = OpponentMoveChoice(
                move=choice.move,
                position_key=choice.position_key,
                selected_via=choice.selected_via,
                corpus_lookup_reason_code=choice.corpus_lookup_reason_code,
                normalized_position_key=normalized_position_key,
                candidate_row_count=self._extract_candidate_row_count(),
                legal_candidate_count=self._extract_legal_candidate_count(default=choice.legal_candidate_count),
                raw_count=choice.raw_count,
                effective_weight=choice.effective_weight,
                total_observed_count=choice.total_observed_count,
                sparse=choice.sparse,
                sparse_reason="; ".join(failures),
                fallback_applied=True,
                candidate_summaries=choice.candidate_summaries,
                timing_overlay_available=bundle_overlay_available,
                timing_overlay_source=bundle_overlay_source,
                bundle_kind=bundle_kind,
                exact_payload_path=bundle_exact_payload,
                timing_lookup_mode=bundle_lookup_mode,
                timing_bundle_invariant_time_control_id=bundle_time_control,
                timing_bundle_invariant_rating_band=bundle_rating_band,
            )
        self.last_choice = choice
        return choice

    def close(self) -> None:
        self.stockfish_provider.close()

    def _extract_candidate_row_count(self) -> int:
        diagnostic = getattr(self.bundle_provider, "last_lookup_diagnostic", None)
        return self._extract_int_from_diagnostic(diagnostic, "candidate_rows_loaded", default=0)

    def _extract_legal_candidate_count(self, default: int = 0) -> int:
        diagnostic = getattr(self.bundle_provider, "last_lookup_diagnostic", None)
        return self._extract_int_from_diagnostic(diagnostic, "legal_candidates", default=default)

    @staticmethod
    def _extract_int_from_diagnostic(diagnostic: str | None, key: str, default: int) -> int:
        if not diagnostic:
            return default
        prefix = f"{key}="
        for part in diagnostic.split(";"):
            token = part.strip()
            if not token.startswith(prefix):
                continue
            value = token.removeprefix(prefix)
            try:
                return int(value)
            except ValueError:
                return default
        return default
