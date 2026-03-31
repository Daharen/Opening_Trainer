from __future__ import annotations

import sqlite3

import chess

from opening_trainer.bundle_corpus import normalize_builder_position_key
from opening_trainer.review.predecessor_lookup import find_predecessor_route_for_fen


def _create_predecessor_db(path, rows):
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE predecessor_master (position_key TEXT PRIMARY KEY, parent_position_key TEXT, incoming_move_uci TEXT)"
        )
        connection.executemany(
            "INSERT INTO predecessor_master(position_key, parent_position_key, incoming_move_uci) VALUES (?, ?, ?)",
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def test_route_lookup_found_reconstructs_forward_line(tmp_path):
    board = chess.Board()
    key_start = normalize_builder_position_key(board)
    board.push_uci("e2e4")
    key_after_e4 = normalize_builder_position_key(board)
    board.push_uci("e7e5")
    key_target = normalize_builder_position_key(board)
    db_path = tmp_path / "predecessor.sqlite"
    _create_predecessor_db(
        db_path,
        [
            (key_start, None, None),
            (key_after_e4, key_start, "e2e4"),
            (key_target, key_after_e4, "e7e5"),
        ],
    )

    result = find_predecessor_route_for_fen(board.fen(), predecessor_master_db_path=str(db_path))

    assert result.success is True
    assert result.predecessor_line_uci == "e2e4 e7e5"
    assert result.ply_count == 2


def test_route_lookup_not_found_reports_explicit_reason(tmp_path):
    db_path = tmp_path / "predecessor.sqlite"
    _create_predecessor_db(db_path, [])

    result = find_predecessor_route_for_fen(chess.STARTING_FEN, predecessor_master_db_path=str(db_path))

    assert result.success is False
    assert result.failure_reason == "target_not_found"


def test_route_lookup_malformed_parent_chain_fails_safely(tmp_path):
    board = chess.Board()
    board.push_uci("e2e4")
    key_target = normalize_builder_position_key(board)
    db_path = tmp_path / "predecessor.sqlite"
    _create_predecessor_db(db_path, [(key_target, "missing_parent", "e2e4")])

    result = find_predecessor_route_for_fen(board.fen(), predecessor_master_db_path=str(db_path))

    assert result.success is False
    assert result.failure_reason == "chain_reconstruction_failed"


def test_route_lookup_normalizes_input_fen_identity(tmp_path):
    board = chess.Board()
    board.push_uci("e2e4")
    normalized_key = normalize_builder_position_key(board)
    db_path = tmp_path / "predecessor.sqlite"
    _create_predecessor_db(
        db_path,
        [
            (normalize_builder_position_key(chess.Board()), None, None),
            (normalized_key, normalize_builder_position_key(chess.Board()), "e2e4"),
        ],
    )

    fen_with_different_counters = board.board_fen() + " b KQkq - 9 99"
    result = find_predecessor_route_for_fen(fen_with_different_counters, predecessor_master_db_path=str(db_path))

    assert result.success is True
    assert result.normalized_position_key == normalized_key
