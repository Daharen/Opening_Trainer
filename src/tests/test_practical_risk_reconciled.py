from __future__ import annotations

import sqlite3

import chess

from opening_trainer.evaluation import BookAuthorityResult, EngineAuthorityResult, ReasonCode
from opening_trainer.evaluator import MoveEvaluator
from opening_trainer.practical_risk_reconciled import PracticalRiskReconciledService, ReconciledFailureRenderer


class StubBookAuthority:
    def evaluate(self, board_before_move, played_move):
        return BookAuthorityResult(
            accepted=False,
            available=False,
            reason_code=ReasonCode.BOOK_UNAVAILABLE,
            reason_text="Book unavailable",
            metadata={"book_available": False},
        )


class StubEngineAuthority:
    def evaluate(self, board_before_move, played_move):
        return EngineAuthorityResult(
            accepted=False,
            available=True,
            reason_code=ReasonCode.ENGINE_FAIL,
            reason_text="Rejected by engine",
            best_move_uci="d2d4",
            best_move_san="d4",
            cp_loss=180,
            metadata={"engine_available": True},
        )


class StubBookPassAuthority:
    def evaluate(self, board_before_move, played_move):
        return BookAuthorityResult(
            accepted=True,
            available=True,
            reason_code=ReasonCode.BOOK_HIT,
            reason_text="Accepted via book membership.",
            candidate_move_uci=played_move.uci(),
            metadata={"book_available": True},
        )


class StubEnginePassAuthority:
    def evaluate(self, board_before_move, played_move):
        return EngineAuthorityResult(
            accepted=True,
            available=True,
            reason_code=ReasonCode.ENGINE_PASS,
            reason_text="Accepted by engine",
            best_move_uci=played_move.uci(),
            best_move_san=board_before_move.san(played_move),
            played_move_uci=played_move.uci(),
            played_move_san=board_before_move.san(played_move),
            cp_loss=0,
            metadata={"engine_available": True},
        )


def _make_db(path, *, bands=("1000-1200", "1400-1600"), reason_code="would_pass_if_sharp_toggle_enabled"):
    board = chess.Board()
    position_key = board.fen().rsplit(" ", 2)[0]
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE artifact_metadata(key TEXT, value TEXT)")
    conn.execute("CREATE TABLE reconciled_move_admissions(position_key TEXT, band_id TEXT, move_uci TEXT, admitted_good_inclusive INTEGER, admitted_good_exclusive INTEGER, admission_origin TEXT, engine_quality_class TEXT, local_reason TEXT)")
    conn.execute("CREATE TABLE failure_explanations(position_key TEXT, band_id TEXT, move_uci TEXT, mode_id TEXT, reason_code TEXT, template_id TEXT, family_label TEXT, max_practical_band_id TEXT, first_failure_band_id TEXT, toggle_state_required TEXT, rendered_preview TEXT)")
    conn.execute("CREATE TABLE reconciled_root_summaries(position_key TEXT, band_id TEXT, summary_json TEXT)")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('artifact_role','practical_risk_reconciled')")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('time_control_id','600+0')")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('included_band_order',?)", ("[\"%s\"]" % "\",\"".join(bands),))
    conn.execute("INSERT INTO reconciled_move_admissions VALUES(?,?, 'e2e4',0,0,'reconciled','good','sharp line')", (position_key, "1400-1600"))
    conn.execute(
        "INSERT INTO failure_explanations VALUES(?,?,'e2e4','good_exclusive',?,?,?,?,?,?,?)",
        (position_key, "1400-1600", reason_code, "tmpl", "sharp line", "1400-1600", "1800-2000", "sharp_on", None),
    )
    conn.execute("INSERT INTO reconciled_root_summaries VALUES(?,?,'{}')", (position_key, "1400-1600"))
    conn.commit()
    conn.close()


def _eval(service, *, sharp=False):
    evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(),
        engine_authority=StubEngineAuthority(),
        reconciled_service=service,
    )
    evaluator.config = type(evaluator.config)(**{**evaluator.config.snapshot(), "good_moves_acceptable": False})
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")
    return evaluator.evaluate(board, move, 1, requested_band_id="1200-1400", allow_sharp_gambit_lines=sharp)


def test_exact_band_lookup_and_fallback_order(tmp_path):
    db = tmp_path / "reconciled.sqlite"
    _make_db(db)
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    exact = service.resolve_band_id("1400-1600")
    assert exact.resolved_band_id == "1400-1600"
    assert exact.provenance == "exact"

    higher = service.resolve_band_id("1200-1400")
    assert higher.resolved_band_id == "1400-1600"
    assert higher.provenance == "fallback_nearest_higher"

    lower = service.resolve_band_id("1800-2000")
    assert lower.resolved_band_id == "1400-1600"
    assert lower.provenance == "fallback_nearest_lower"


def test_strict_mode_sharp_off_fails_and_sharp_on_overrides(tmp_path):
    db = tmp_path / "reconciled.sqlite"
    _make_db(db)
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    failed = _eval(service, sharp=False)
    assert failed.accepted is False
    assert "Enable sharp/gambit lines" in failed.reason_text

    passed = _eval(service, sharp=True)
    assert passed.accepted is True
    assert passed.metadata["reconciled"]["decision_source"] == "sharp_toggle_override_from_failure_explanation"


def test_outgrown_reason_not_overridden_by_sharp_toggle(tmp_path):
    db = tmp_path / "reconciled.sqlite"
    _make_db(db, reason_code="outgrown_above_band")
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    result = _eval(service, sharp=True)
    assert result.accepted is False
    assert "outgrown" in result.reason_text.lower()


def test_missing_row_and_invalid_artifact_fallback_cleanly(tmp_path):
    db = tmp_path / "reconciled.sqlite"
    _make_db(db)
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    board = chess.Board()
    move = chess.Move.from_uci("d2d4")
    evaluator = MoveEvaluator(book_authority=StubBookAuthority(), engine_authority=StubEngineAuthority(), reconciled_service=service)
    result = evaluator.evaluate(board, move, 1, requested_band_id="1200-1400", allow_sharp_gambit_lines=False)
    assert result.accepted is False
    assert result.metadata["reconciled"]["decision_source"] == "reconciled_artifact_no_row"

    bad = PracticalRiskReconciledService(db, expected_time_control_id="300+0")
    assert bad.active is False
    assert "time_control_id mismatch" in (bad.activation_error or "")


def test_failure_renderer_stable_without_rendered_preview():
    rendered = ReconciledFailureRenderer.render(
        {
            "reason_code": "strict_mode_rejects_good",
            "family_label": None,
            "max_practical_band_id": None,
            "first_failure_band_id": None,
            "rendered_preview": None,
        },
        requested_band_id="1200-1400",
        resolved_band_id="1400-1600",
    )
    assert "Good moves are enabled" in rendered
    assert "requested band" in rendered


def test_reconciled_not_consulted_after_book_or_engine_pass(tmp_path):
    db = tmp_path / "reconciled.sqlite"
    _make_db(db)
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    board = chess.Board()
    move = chess.Move.from_uci("e2e4")

    book_pass = MoveEvaluator(
        book_authority=StubBookPassAuthority(),
        engine_authority=StubEngineAuthority(),
        reconciled_service=service,
    ).evaluate(board, move, 1, requested_band_id="1200-1400", allow_sharp_gambit_lines=True)
    assert book_pass.accepted is True
    assert book_pass.canonical_judgment.value == "Book"
    assert book_pass.metadata["reconciled"]["decision_source"] == "legacy_engine_book"

    engine_pass = MoveEvaluator(
        book_authority=StubBookAuthority(),
        engine_authority=StubEnginePassAuthority(),
        reconciled_service=service,
    ).evaluate(board, move, 1, requested_band_id="1200-1400", allow_sharp_gambit_lines=True)
    assert engine_pass.accepted is True
    assert engine_pass.canonical_judgment.value == "Better"
    assert engine_pass.metadata["reconciled"]["decision_source"] == "legacy_engine_book"
