from __future__ import annotations

import hashlib
from dataclasses import asdict

import chess

from ..bundle_corpus import normalize_builder_position_key
from .models import (
    ManualForcedPlayerColor,
    ManualPresentationMode,
    ReviewItem,
    ReviewItemOrigin,
    ReviewPathMove,
    UrgencyTier,
    utc_now_iso,
)


def normalize_fen_for_target(fen: str) -> str:
    board = chess.Board(fen)
    return board.fen()


def _position_identity(board: chess.Board) -> str:
    return normalize_builder_position_key(board)


def _parse_predecessor_path_uci(predecessor_line_uci: str) -> tuple[list[ReviewPathMove], chess.Board]:
    board = chess.Board()
    predecessor_path: list[ReviewPathMove] = []
    for token in [part.strip() for part in predecessor_line_uci.split() if part.strip()]:
        try:
            move = chess.Move.from_uci(token)
        except ValueError as exc:
            raise ValueError(f"Invalid UCI move in predecessor line: {token}") from exc
        if move not in board.legal_moves:
            raise ValueError(f"Illegal predecessor move from current position: {token}")
        predecessor_path.append(
            ReviewPathMove(
                ply_index=len(board.move_stack),
                side_to_move='white' if board.turn == chess.WHITE else 'black',
                move_uci=move.uci(),
                san=board.san(move),
                fen_before=board.fen(),
            )
        )
        board.push(move)
    return predecessor_path, board


def _canonical_predecessor_path_from_start(target_board: chess.Board) -> list[ReviewPathMove]:
    target_identity = _position_identity(target_board)
    start = chess.Board()
    if _position_identity(start) == target_identity:
        return []
    queue: list[tuple[chess.Board, list[chess.Move]]] = [(start, [])]
    seen = {_position_identity(start)}
    max_depth = 24
    max_nodes = 50000
    visited = 0
    while queue:
        board, path = queue.pop(0)
        if len(path) >= max_depth:
            continue
        legal_moves = sorted((move.uci(), move) for move in board.legal_moves)
        for _uci, move in legal_moves:
            probe = board.copy(stack=True)
            probe.push(move)
            visited += 1
            identity = _position_identity(probe)
            if identity in seen:
                continue
            new_path = [*path, move]
            if identity == target_identity:
                material_board = chess.Board()
                resolved: list[ReviewPathMove] = []
                for resolved_move in new_path:
                    resolved.append(
                        ReviewPathMove(
                            ply_index=len(material_board.move_stack),
                            side_to_move='white' if material_board.turn == chess.WHITE else 'black',
                            move_uci=resolved_move.uci(),
                            san=material_board.san(resolved_move),
                            fen_before=material_board.fen(),
                        )
                    )
                    material_board.push(resolved_move)
                return resolved
            if visited >= max_nodes:
                raise ValueError('No valid predecessor route could be deterministically resolved from the start position.')
            seen.add(identity)
            queue.append((probe, new_path))
    raise ValueError('No valid predecessor route could be deterministically resolved from the start position.')


def validate_manual_target(
    *,
    target_fen: str,
    predecessor_line_uci: str | None,
    presentation_mode: str = ManualPresentationMode.PLAY_TO_POSITION.value,
    auto_resolve_predecessor: bool = False,
) -> tuple[chess.Board, list[ReviewPathMove], str | None]:
    try:
        target_board = chess.Board(target_fen)
    except ValueError as exc:
        raise ValueError(f"Invalid target FEN: {exc}") from exc
    if not target_board.is_valid():
        raise ValueError('Target FEN does not describe a valid chess position.')

    cleaned_line = (predecessor_line_uci or '').strip()
    normalized_presentation_mode = (presentation_mode or ManualPresentationMode.PLAY_TO_POSITION.value).strip()
    auto_resolve_allowed = auto_resolve_predecessor and normalized_presentation_mode == ManualPresentationMode.PLAY_TO_POSITION.value
    predecessor_path: list[ReviewPathMove] = []
    if cleaned_line:
        try:
            predecessor_path, reached = _parse_predecessor_path_uci(cleaned_line)
        except ValueError:
            if not auto_resolve_allowed:
                raise
            predecessor_path = _canonical_predecessor_path_from_start(target_board)
            cleaned_line = ' '.join(move.move_uci for move in predecessor_path)
            reached = target_board
        if _position_identity(reached) != _position_identity(target_board):
            if not auto_resolve_allowed:
                raise ValueError('Predecessor line does not reach the target position identity.')
            predecessor_path = _canonical_predecessor_path_from_start(target_board)
            cleaned_line = ' '.join(move.move_uci for move in predecessor_path)
    if normalized_presentation_mode == ManualPresentationMode.PLAY_TO_POSITION.value and not predecessor_path:
        if not auto_resolve_allowed:
            raise ValueError('Play-to-position mode requires a predecessor line that reaches the target.')
        predecessor_path = _canonical_predecessor_path_from_start(target_board)
        cleaned_line = ' '.join(move.move_uci for move in predecessor_path)
    return target_board, predecessor_path, cleaned_line or None


def create_manual_target_item(
    *,
    profile_id: str,
    target_board: chess.Board,
    predecessor_path: list[ReviewPathMove],
    predecessor_line_uci: str | None,
    urgency_tier: str,
    allow_below_threshold_reach: bool,
    manual_presentation_mode: str,
    manual_forced_player_color: str,
    operator_note: str | None,
    manual_parent_review_item_id: str | None = None,
    manual_reach_policy_inherited: bool = False,
) -> ReviewItem:
    now = utc_now_iso()
    position_key = _position_identity(target_board)
    side_to_move = 'white' if target_board.turn == chess.WHITE else 'black'
    stable_id_material = (
        f"{profile_id}|manual_target|{position_key}|{side_to_move}|"
        f"{predecessor_line_uci or ''}|{manual_presentation_mode}|{manual_forced_player_color}"
    )
    review_item_id = hashlib.sha256(stable_id_material.encode('utf-8')).hexdigest()[:16]
    return ReviewItem(
        review_item_id=review_item_id,
        position_key=position_key,
        position_fen_normalized=target_board.fen(),
        side_to_move=side_to_move,
        created_at_utc=now,
        updated_at_utc=now,
        last_seen_at_utc=now,
        last_failed_at_utc=None,
        last_passed_at_utc=None,
        times_seen=0,
        times_failed=0,
        times_passed=0,
        consecutive_failures=0,
        consecutive_successes=0,
        success_streak=0,
        mastery_score=0.0,
        stability_score=0.0,
        urgency_tier=urgency_tier,
        urgency_multiplier=2.5 if urgency_tier == UrgencyTier.EXTREME.value else (1.5 if urgency_tier == UrgencyTier.BOOSTED.value else 1.0),
        due_at_utc=now,
        last_routing_reason='manual_target_created',
        failure_reason='Manual target item',
        preferred_move_uci=None,
        accepted_move_set=[],
        predecessor_path=[asdict(move) for move in predecessor_path],
        line_preview_san=' '.join(move.san for move in predecessor_path[-6:]),
        profile_id=profile_id,
        frequency_retired_for_current_due_cycle=False,
        stubborn_extreme_state='none',
        stubborn_extra_repeat_consumed_until_success=False,
        skipped_review_slots=0,
        was_due_previous_check=True,
        pending_forced_stubborn_repeat=False,
        canonical_predecessor_path_id='manual_target_v1',
        canonical_predecessor_path_metadata={'path_count': 1 if predecessor_path else 0, 'selection_rule': 'manual_target_predecessor_line'},
        canonical_anchor_positions=[move.fen_before for move in predecessor_path[-2:]] if predecessor_path else [],
        hijack_stage='none',
        hijack_pass_ticker=0,
        dormant=False,
        avoidance_count=0,
        last_hijack_routing_source='',
        last_anchor_seen_at=None,
        frequency_state=urgency_tier,
        frequency_state_entered_at_utc=now,
        srs_stage_index=0,
        srs_next_due_at_utc=now,
        srs_last_reviewed_at_utc=None,
        srs_last_result='none',
        srs_lapse_count=0,
        origin_kind=ReviewItemOrigin.MANUAL_TARGET.value,
        manual_target_fen=target_board.fen(),
        predecessor_line_uci=predecessor_line_uci,
        predecessor_line_notation_kind='uci' if predecessor_line_uci else None,
        allow_below_threshold_reach=allow_below_threshold_reach,
        manual_presentation_mode=manual_presentation_mode or ManualPresentationMode.PLAY_TO_POSITION.value,
        manual_forced_player_color=manual_forced_player_color or ManualForcedPlayerColor.AUTO.value,
        manual_initial_urgency_tier=urgency_tier,
        operator_note=operator_note.strip() if operator_note and operator_note.strip() else None,
        manual_parent_review_item_id=manual_parent_review_item_id,
        manual_reach_policy_inherited=manual_reach_policy_inherited,
    )
