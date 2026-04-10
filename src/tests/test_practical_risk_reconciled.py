from __future__ import annotations

import sqlite3

import chess

from opening_trainer.evaluation import (
    BookAuthorityResult,
    EngineAuthorityResult,
    ReasonCode,
)
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.practical_risk_reconciled import PracticalRiskReconciledService, render_failure_explanation


class StubBookAuthority:
    def evaluate(self, board_before_move, played_move):
        return BookAuthorityResult(
            accepted=False,
            available=False,
            reason_code=ReasonCode.BOOK_UNAVAILABLE,
            reason_text="book unavailable",
            metadata={},
        )


class StubEngineAuthority:
    def __init__(self, accepted: bool = False):
        self.accepted = accepted

    def evaluate(self, board_before_move, played_move):
        return EngineAuthorityResult(
            accepted=self.accepted,
            available=True,
            reason_code=ReasonCode.ENGINE_PASS if self.accepted else ReasonCode.ENGINE_FAIL,
            reason_text="engine",
            best_move_uci="d2d4",
            best_move_san="d4",
            played_move_uci=played_move.uci(),
            played_move_san=board_before_move.san(played_move),
            cp_loss=120,
            metadata={},
        )


def _make_db(path, *, bands=("1400", "1800")):
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("CREATE TABLE artifact_metadata (key TEXT, value TEXT)")
        conn.execute("CREATE TABLE reconciled_move_admissions (position_key TEXT, band_id TEXT, move_uci TEXT, admitted_good_inclusive INTEGER, admitted_good_exclusive INTEGER, admission_origin TEXT, engine_quality_class TEXT, local_reason TEXT, reconciled_local_distinction TEXT)")
        conn.execute("CREATE TABLE failure_explanations (position_key TEXT, band_id TEXT, move_uci TEXT, mode_id TEXT, reason_code TEXT, template_id TEXT, family_label TEXT, max_practical_band_id TEXT, first_failure_band_id TEXT, toggle_state_required TEXT, rendered_preview TEXT)")
        conn.execute("CREATE TABLE reconciled_root_summaries (position_key TEXT, band_id TEXT, summary TEXT)")
        conn.executemany(
            "INSERT INTO artifact_metadata (key, value) VALUES (?, ?)",
            [
                ("artifact_role", "stage_d_reconciled"),
                ("time_control_id", "600+0"),
                ("artifact_family_id", "family-x"),
                ("included_band_ids", ",".join(bands)),
            ],
        )
    conn.close()


def test_exact_and_fallback_band_resolution(tmp_path):
    db = tmp_path / "r.sqlite"
    _make_db(db, bands=("1400", "1800", "2200"))
    service = PracticalRiskReconciledService(path=db, compatible_time_control_id="600+0")
    service._load()

    assert service.resolve_band_id("1800").resolved_band_id == "1800"
    assert service.resolve_band_id("1700").resolved_band_id == "1800"
    assert service.resolve_band_id("2500").resolved_band_id == "2200"


def test_strict_mode_toggle_off_fails_and_toggle_on_passes(tmp_path):
    db = tmp_path / "r.sqlite"
    _make_db(db)
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")
    position_key = board.fen()
    conn = sqlite3.connect(db)
    with conn:
        conn.execute(
            "INSERT INTO reconciled_move_admissions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (position_key, "1800", "e2e4", 1, 0, "reconciled", "sharp", "strict reject", "reconciled"),
        )
        conn.execute(
            "INSERT INTO failure_explanations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (position_key, "1800", "e2e4", "good_exclusive", "would_pass_if_sharp_toggle_enabled", "tpl", "gambit line", "1800", None, "sharp_on", None),
        )
    conn.close()
    service = PracticalRiskReconciledService(path=db, compatible_time_control_id="600+0")
    service._load()

    ev = MoveEvaluator(book_authority=StubBookAuthority(), engine_authority=StubEngineAuthority(False))
    ev.practical_risk_reconciled = service
    ev.practical_risk_context = {
        "position_key": position_key,
        "requested_band_id": "1800",
        "good_moves_acceptable": False,
        "allow_sharp_gambit_lines": False,
    }
    failed = ev.evaluate(board, move, 1)
    assert failed.accepted is False
    assert failed.canonical_judgment.value == "Fail"

    ev.practical_risk_context["allow_sharp_gambit_lines"] = True
    passed = ev.evaluate(board, move, 1)
    assert passed.accepted is True
    assert passed.canonical_judgment.value == "Better"


def test_outgrown_reason_never_overridden(tmp_path):
    db = tmp_path / "r.sqlite"
    _make_db(db)
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")
    position_key = board.fen()
    conn = sqlite3.connect(db)
    with conn:
        conn.execute(
            "INSERT INTO reconciled_move_admissions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (position_key, "1800", "e2e4", 1, 0, "reconciled", "sharp", "strict reject", "reconciled"),
        )
        conn.execute(
            "INSERT INTO failure_explanations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (position_key, "1800", "e2e4", "good_exclusive", "outgrown_above_band", "tpl", "sharp line", "1800", "2000", "sharp_on", None),
        )
    conn.close()
    service = PracticalRiskReconciledService(path=db, compatible_time_control_id="600+0")
    service._load()
    ev = MoveEvaluator(book_authority=StubBookAuthority(), engine_authority=StubEngineAuthority(False))
    ev.practical_risk_reconciled = service
    ev.practical_risk_context = {
        "position_key": position_key,
        "requested_band_id": "1800",
        "good_moves_acceptable": False,
        "allow_sharp_gambit_lines": True,
    }
    result = ev.evaluate(board, move, 1)
    assert result.accepted is False
    assert "outgrown" in result.reason_text.lower()


def test_missing_row_and_invalid_artifact_fallback(tmp_path):
    db = tmp_path / "r.sqlite"
    _make_db(db)
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")
    service = PracticalRiskReconciledService(path=db, compatible_time_control_id="wrong")
    service._load()
    assert service.active is False

    ev = MoveEvaluator(book_authority=StubBookAuthority(), engine_authority=StubEngineAuthority(False))
    ev.practical_risk_reconciled = service
    ev.practical_risk_context = {"position_key": board.fen(), "requested_band_id": "1800", "good_moves_acceptable": True, "allow_sharp_gambit_lines": False}
    result = ev.evaluate(board, move, 1)
    assert result.accepted is False


def test_renderer_structured_without_preview():
    text = render_failure_explanation(
        {
            "reason_code": "would_pass_if_sharp_toggle_enabled",
            "family_label": "gambit line",
            "max_practical_band_id": "1800",
            "rendered_preview": None,
        },
        requested_band_id="1700",
        resolved_band_id="1800",
    )
    assert "Enable sharp/gambit lines" in text
    assert "resolved band" in text
