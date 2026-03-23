from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import chess
import chess.engine

from .bundle_corpus import (
    BuilderAggregateCorpusProvider,
    BuilderAggregateParseError,
    normalize_builder_position_key,
)
from .corpus import DEFAULT_ARTIFACT_PATH, load_artifact, normalize_position_key
from .evaluation import EvaluatorConfig
from .runtime import corpus_status_detail


@dataclass(frozen=True)
class OpponentMoveChoice:
    move: chess.Move
    position_key: str
    selected_via: str
    raw_count: int
    effective_weight: float
    total_observed_count: int
    sparse: bool
    sparse_reason: str | None
    fallback_applied: bool
    candidate_summaries: tuple[dict[str, object], ...]


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

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        with chess.engine.SimpleEngine.popen_uci(self.config.engine_path) as engine:
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
            raw_count=0,
            effective_weight=1.0,
            total_observed_count=0,
            sparse=False,
            sparse_reason=None,
            fallback_applied=True,
            candidate_summaries=({"uci": move.uci(), "raw_count": 0, "effective_weight": 1.0},),
        )


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
            self.bundle = BuilderAggregateCorpusProvider(self.bundle_dir, rng=self.rng)
        except BuilderAggregateParseError as exc:
            self.last_lookup_diagnostic = f"reason_code={exc.reason_code}; detail={exc.detail}"
            raise

    def choose_move(self, board: chess.Board) -> OpponentMoveChoice:
        position_key = normalize_builder_position_key(board)
        position = self.bundle.position_index.get(position_key)
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
        selected = self.rng.choices(weighted_candidates, weights=[max(0, candidate.raw_count) for candidate in weighted_candidates], k=1)[0]
        selected_move = next(move for move, candidate in legal_candidates if candidate == selected)
        self.last_lookup_diagnostic = (
            "reason_code=corpus_hit; "
            f"position_key={position_key}; candidate_rows_loaded={position.candidate_row_count}; legal_candidates={len(legal_candidates)}"
        )
        return OpponentMoveChoice(
            move=selected_move,
            position_key=position_key,
            selected_via="corpus_aggregate_bundle",
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
        failures: list[str] = []
        if self.bundle_provider is not None:
            try:
                return self.bundle_provider.choose_move(board)
            except LookupError as exc:
                failures.append(f"bundle lookup failed: {exc}")
            except BuilderAggregateParseError as exc:
                failures.append(
                    "bundle lookup failed: "
                    f"reason_code={exc.reason_code}; detail={exc.detail}; position_key={normalize_builder_position_key(board)}"
                )
        if self.corpus_provider is not None:
            try:
                return self.corpus_provider.choose_move(board)
            except LookupError as exc:
                failures.append(f"legacy corpus lookup failed: {exc}")
        try:
            return self.stockfish_provider.choose_move(board)
        except (LookupError, FileNotFoundError, chess.engine.EngineError, OSError) as exc:
            failures.append(f"Stockfish fallback failed: {exc}")
        choice = self.random_provider.choose_move(board)
        if failures:
            choice = OpponentMoveChoice(
                move=choice.move,
                position_key=choice.position_key,
                selected_via=choice.selected_via,
                raw_count=choice.raw_count,
                effective_weight=choice.effective_weight,
                total_observed_count=choice.total_observed_count,
                sparse=choice.sparse,
                sparse_reason="; ".join(failures),
                fallback_applied=True,
                candidate_summaries=choice.candidate_summaries,
            )
        return choice
