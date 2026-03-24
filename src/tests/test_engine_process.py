from __future__ import annotations

import os

import chess

from opening_trainer.evaluation import EvaluatorConfig
from opening_trainer.evaluation.engine import EngineAuthority
from opening_trainer.evaluation.engine_process import engine_popen_kwargs
from opening_trainer.opponent import StockfishOpponentProvider


def test_engine_popen_kwargs_no_window_policy_is_windows_only():
    kwargs = engine_popen_kwargs()
    if os.name == "nt":
        assert "creationflags" in kwargs
    else:
        assert kwargs == {}


def test_engine_authority_reuses_long_lived_engine(monkeypatch):
    launches = {"count": 0}

    class FakeScore:
        def pov(self, _side):
            return self

        def is_mate(self):
            return False

        def score(self, mate_score=100000):
            return 15

    class FakeEngine:
        def analyse(self, board, limit):
            move = next(iter(board.legal_moves))
            return {"pv": [move], "score": FakeScore()}

        def quit(self):
            return None

    def fake_popen_uci(path, **kwargs):
        launches["count"] += 1
        return FakeEngine()

    monkeypatch.setattr("chess.engine.SimpleEngine.popen_uci", fake_popen_uci)
    authority = EngineAuthority(EvaluatorConfig(engine_path="fake-stockfish"))
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")

    first = authority.evaluate(board, move)
    second = authority.best_reply(board)

    assert first.available is True
    assert second[0] is not None
    assert launches["count"] == 1


def test_stockfish_fallback_reuses_long_lived_engine(monkeypatch):
    launches = {"count": 0}

    class PlayResult:
        def __init__(self, move):
            self.move = move

    class FakeEngine:
        def play(self, board, limit):
            return PlayResult(next(iter(board.legal_moves)))

        def quit(self):
            return None

    def fake_popen_uci(path, **kwargs):
        launches["count"] += 1
        return FakeEngine()

    monkeypatch.setattr("chess.engine.SimpleEngine.popen_uci", fake_popen_uci)
    provider = StockfishOpponentProvider(EvaluatorConfig(engine_path="fake-stockfish"))
    board = chess.Board()

    provider.choose_move(board)
    provider.choose_move(board)

    assert launches["count"] == 1
