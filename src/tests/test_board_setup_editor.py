from __future__ import annotations

import pytest

from opening_trainer.ui.board_setup_editor import build_setup_fen


def test_build_setup_fen_accepts_valid_setup() -> None:
    fen = build_setup_fen(board_fen='4k3/8/8/8/8/8/8/4K3', turn='w', castling='')
    assert fen.startswith('4k3/8/8/8/8/8/8/4K3 w -')


def test_build_setup_fen_rejects_missing_king() -> None:
    with pytest.raises(ValueError, match='exactly one white king'):
        build_setup_fen(board_fen='4k3/8/8/8/8/8/8/8', turn='w', castling='')


def test_build_setup_fen_rejects_illegal_startup_position() -> None:
    with pytest.raises(ValueError, match='legal enough'):
        build_setup_fen(board_fen='4k3/8/8/8/8/8/4K3/8', turn='b', castling='KQkq')
