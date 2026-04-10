from __future__ import annotations

import sqlite3

import chess
import opening_trainer.evaluator as evaluator_module

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


def _position_key(board: chess.Board) -> str:
    return board.fen().rsplit(" ", 2)[0]


def _create_real_schema_db(
    path,
    *,
    bands=("1000-1200", "1400-1600"),
    admissions=(),
    explanations=(),
    include_root_summary_counts=True,
):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE artifact_metadata(key TEXT, value TEXT)")
    conn.execute(
        """
        CREATE TABLE reconciled_move_admissions(
            position_key TEXT,
            band_id TEXT,
            move_uci TEXT,
            local_admitted_if_good_accepted INTEGER,
            local_admitted_if_good_rejected INTEGER,
            reconciled_admitted_if_good_accepted INTEGER,
            reconciled_admitted_if_good_rejected INTEGER,
            local_admission_origin_if_good_accepted TEXT,
            local_admission_origin_if_good_rejected TEXT,
            reconciled_admission_origin_if_good_accepted TEXT,
            reconciled_admission_origin_if_good_rejected TEXT,
            engine_quality_class TEXT,
            local_reason TEXT,
            practical_ceiling_band_id TEXT,
            family_label TEXT,
            failure_reason_code TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE failure_explanations(
            position_key TEXT,
            band_id TEXT,
            move_uci TEXT,
            mode_id TEXT,
            reason_code TEXT,
            template_id TEXT,
            family_label TEXT,
            max_practical_band_id TEXT,
            first_failure_band_id TEXT,
            toggle_state_required TEXT,
            rendered_preview TEXT
        )
        """
    )
    if include_root_summary_counts:
        conn.execute(
            """
            CREATE TABLE reconciled_root_summaries(
                position_key TEXT,
                band_id TEXT,
                local_admitted_if_good_accepted_count INTEGER,
                local_admitted_if_good_rejected_count INTEGER,
                reconciled_admitted_if_good_accepted_count INTEGER,
                reconciled_admitted_if_good_rejected_count INTEGER
            )
            """
        )
    else:
        conn.execute(
            """
            CREATE TABLE reconciled_root_summaries(
                position_key TEXT,
                band_id TEXT
            )
            """
        )
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('artifact_role','practical_risk_reconciled')")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('time_control_id','600+0')")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('included_band_order',?)", ("[\"%s\"]" % "\",\"".join(bands),))

    conn.executemany(
        """
        INSERT INTO reconciled_move_admissions(
            position_key, band_id, move_uci,
            local_admitted_if_good_accepted, local_admitted_if_good_rejected,
            reconciled_admitted_if_good_accepted, reconciled_admitted_if_good_rejected,
            local_admission_origin_if_good_accepted, local_admission_origin_if_good_rejected,
            reconciled_admission_origin_if_good_accepted, reconciled_admission_origin_if_good_rejected,
            engine_quality_class, local_reason, practical_ceiling_band_id
        ) VALUES(
            ?,?,?,?,?,?,?,
            ?,?,?,?,
            ?,?,?
        )
        """,
        admissions,
    )
    conn.executemany(
        "INSERT INTO failure_explanations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        explanations,
    )
    for admission in admissions:
        if include_root_summary_counts:
            conn.execute(
                "INSERT INTO reconciled_root_summaries VALUES(?,?,?,?,?,?)",
                (admission[0], admission[1], 1, 1, 1, 1),
            )
        else:
            conn.execute(
                "INSERT INTO reconciled_root_summaries VALUES(?,?)",
                (admission[0], admission[1]),
            )
    conn.commit()
    conn.close()


def _eval(service, *, move_uci="e2e4", board=None, requested_band_id="1200-1400", sharp=False, good_moves_acceptable=False):
    evaluator = MoveEvaluator(
        book_authority=StubBookAuthority(),
        engine_authority=StubEngineAuthority(),
        reconciled_service=service,
    )
    evaluator.config = type(evaluator.config)(**{**evaluator.config.snapshot(), "good_moves_acceptable": good_moves_acceptable})
    board = board or chess.Board()
    move = chess.Move.from_uci(move_uci)
    return evaluator.evaluate(board, move, 1, requested_band_id=requested_band_id, allow_sharp_gambit_lines=sharp)


def test_real_schema_exact_band_lookup_and_fallback_order(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "reconciled.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 0, 0, "local", "local", "reconciled", "reconciled", "good", "sharp line", "1400-1600")
        ],
    )
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


def test_engine_fail_reconciled_admitted_rescues_on_real_schema(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "reconciled.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford line", "1400-1600")
        ],
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    passed = _eval(service, requested_band_id="1200-1400")
    assert passed.accepted is True
    assert passed.metadata["reconciled"]["decision_source"] == "reconciled_admission"
    assert passed.reason_text == "Accepted via practical-risk reconciliation for the current training band."


def test_strict_mode_sharp_override_only_when_enabled(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "reconciled.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 0, 0, "local", "local", "reconciled", "reconciled", "good", "sharp line", "1400-1600")
        ],
        explanations=[
            (position_key, "1400-1600", "e2e4", "good_exclusive", "would_pass_if_sharp_toggle_enabled", "tmpl", "sharp line", "1400-1600", "1800-2000", "sharp_on", None)
        ],
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    failed = _eval(service, sharp=False)
    assert failed.accepted is False
    assert "Enable sharp/gambit lines" in failed.reason_text

    passed = _eval(service, sharp=True)
    assert passed.accepted is True
    assert passed.metadata["reconciled"]["decision_source"] == "sharp_toggle_override_from_failure_explanation"


def test_outgrown_above_band_remains_fail(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "reconciled.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 0, 0, "local", "local", "reconciled", "reconciled", "good", "sharp line", "1400-1600")
        ],
        explanations=[
            (position_key, "1400-1600", "e2e4", "good_exclusive", "outgrown_above_band", "tmpl", "stafford", "1400-1600", "1800-2000", "sharp_on", None)
        ],
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    result = _eval(service, sharp=True)
    assert result.accepted is False
    assert "outgrown" in result.reason_text.lower()


def test_incompatible_schema_fails_activation_cleanly(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "broken.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE artifact_metadata(key TEXT, value TEXT)")
    conn.execute("CREATE TABLE reconciled_move_admissions(position_key TEXT, band_id TEXT, move_uci TEXT, admitted_good_inclusive INTEGER)")
    conn.execute("CREATE TABLE failure_explanations(position_key TEXT, band_id TEXT, move_uci TEXT)")
    conn.execute(
        """
        CREATE TABLE reconciled_root_summaries(
            position_key TEXT,
            band_id TEXT,
            local_admitted_if_good_accepted_count INTEGER,
            local_admitted_if_good_rejected_count INTEGER,
            reconciled_admitted_if_good_accepted_count INTEGER,
            reconciled_admitted_if_good_rejected_count INTEGER
        )
        """
    )
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('artifact_role','practical_risk_reconciled')")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('time_control_id','600+0')")
    conn.execute("INSERT INTO reconciled_move_admissions VALUES(?,?,?,1)", (position_key, "1400-1600", "e2e4"))
    conn.commit()
    conn.close()

    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    assert service.active is False
    assert "artifact schema mismatch in reconciled_move_admissions" in (service.activation_error or "")


def test_incompatible_root_summary_schema_is_non_fatal_and_inspectable(tmp_path):
    db = tmp_path / "broken_root_summary.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE artifact_metadata(key TEXT, value TEXT)")
    conn.execute(
        """
        CREATE TABLE reconciled_move_admissions(
            position_key TEXT,
            band_id TEXT,
            move_uci TEXT,
            local_admitted_if_good_accepted INTEGER,
            local_admitted_if_good_rejected INTEGER,
            reconciled_admitted_if_good_accepted INTEGER,
            reconciled_admitted_if_good_rejected INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE failure_explanations(
            position_key TEXT,
            band_id TEXT,
            move_uci TEXT,
            mode_id TEXT,
            reason_code TEXT,
            template_id TEXT,
            family_label TEXT,
            max_practical_band_id TEXT,
            first_failure_band_id TEXT,
            toggle_state_required TEXT,
            rendered_preview TEXT
        )
        """
    )
    conn.execute("CREATE TABLE reconciled_root_summaries(position_key TEXT, band_id TEXT)")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('artifact_role','practical_risk_reconciled')")
    conn.execute("INSERT INTO artifact_metadata(key,value) VALUES('time_control_id','600+0')")
    conn.commit()
    conn.close()

    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    assert service.active is True
    assert service.activation_error is None
    assert service.root_summary_status == "optional_schema_mismatch_ignored"
    assert "missing columns" in (service.root_summary_activation_warning or "")


def test_root_summary_optional_schema_still_allows_rescue(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "root_summary_optional.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford line", "1400-1600")
        ],
        include_root_summary_counts=False,
    )

    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    result = _eval(service, requested_band_id="1200-1400")

    assert service.active is True
    assert service.root_summary_status == "optional_schema_mismatch_ignored"
    assert result.accepted is True
    assert result.metadata["reconciled"]["decision_source"] == "reconciled_admission"


def test_root_summary_uses_structured_counts_not_legacy_summary_blob(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "root_summary.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford line", "1400-1600")
        ],
    )

    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    summary = service.get_root_summary(position_key, "1400-1600")

    assert service.active is True
    assert summary is not None
    assert summary["local_admitted_if_good_accepted_count"] == 1
    assert summary["local_admitted_if_good_rejected_count"] == 1
    assert summary["reconciled_admitted_if_good_accepted_count"] == 1
    assert summary["reconciled_admitted_if_good_rejected_count"] == 1
    assert "summary_json" not in summary


def test_stafford_b8c6_rescue_uses_real_schema_columns(tmp_path):
    board = chess.Board()
    for uci in ("e2e4", "e7e5", "g1f3", "g8f6", "f3e5"):
        board.push_uci(uci)
    position_key = _position_key(board)
    db = tmp_path / "stafford.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "b8c6", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford gambit", "1400-1600")
        ],
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    result = _eval(service, board=board, move_uci="b8c6", requested_band_id="1200-1400", good_moves_acceptable=False)
    assert result.accepted is True
    assert result.metadata["reconciled"]["decision_source"] == "reconciled_admission"


def test_manual_target_play_to_position_tested_move_rescues_real_schema(tmp_path):
    board = chess.Board()
    for uci in ("d2d4", "g8f6", "c2c4", "e7e6"):
        board.push_uci(uci)
    position_key = _position_key(board)
    db = tmp_path / "manual_target.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1000-1200", "g1f3", 1, 1, 1, 1, "local", "local", "reconciled", "reconciled", "good", "manual_target_test", "1000-1200")
        ],
        bands=("1000-1200",),
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    result = _eval(service, board=board, move_uci="g1f3", requested_band_id="1000-1200")
    assert result.accepted is True
    assert result.metadata["reconciled"]["decision_source"] == "reconciled_admission"


def test_manual_target_play_to_position_rescue_with_degraded_root_summary(tmp_path):
    board = chess.Board()
    for uci in ("d2d4", "g8f6", "c2c4", "e7e6"):
        board.push_uci(uci)
    position_key = _position_key(board)
    db = tmp_path / "manual_target_degraded.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1000-1200", "g1f3", 1, 1, 1, 1, "local", "local", "reconciled", "reconciled", "good", "manual_target_test", "1000-1200")
        ],
        bands=("1000-1200",),
        include_root_summary_counts=False,
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    result = _eval(service, board=board, move_uci="g1f3", requested_band_id="1000-1200")
    assert service.active is True
    assert result.accepted is True
    assert result.metadata["reconciled"]["decision_source"] == "reconciled_admission"


def test_rescued_outcome_clears_legacy_engine_fail_wording(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "stale_reason_fix.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford line", "1400-1600")
        ],
        include_root_summary_counts=False,
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    rescued = _eval(service, requested_band_id="1200-1400")
    assert rescued.accepted is True
    assert "Rejected as an inaccuracy outside engine tolerance." not in rescued.reason_text


def test_fail_outcome_retains_fail_wording_when_not_rescued(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "fail_reason_kept.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 0, 0, "local", "local", "reconciled", "reconciled", "good", "stafford line", "1400-1600")
        ],
        include_root_summary_counts=False,
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    failed = _eval(service, requested_band_id="1200-1400")
    assert failed.accepted is False
    assert "Rejected as a " in failed.reason_text
    assert "outside engine tolerance." in failed.reason_text


def test_missing_row_and_invalid_artifact_fallback_cleanly(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "reconciled.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 0, 0, "local", "local", "reconciled", "reconciled", "good", "sharp line", "1400-1600")
        ],
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
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
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "reconciled.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 1, 1, 1, 1, "local", "local", "reconciled", "reconciled", "good", "manual", "1400-1600")
        ],
    )
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
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


def test_sharp_admitted_line_obeys_runtime_toggle_gate(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "sharp_gate.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "e2e4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "quiet practical", "1400-1600"),
            (position_key, "1400-1600", "d2d4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford gambit", "1400-1600"),
        ],
        explanations=[
            (position_key, "1400-1600", "d2d4", "good_exclusive", "would_pass_if_sharp_toggle_enabled", "tmpl_sharp", "sharp/gambit", "1400-1600", "1800-2000", "sharp_on", None)
        ],
    )
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE reconciled_move_admissions SET family_label='sharp/gambit', failure_reason_code='would_pass_if_sharp_toggle_enabled' WHERE move_uci='d2d4'"
    )
    conn.commit()
    conn.close()
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    sharp_blocked = _eval(service, move_uci="d2d4", requested_band_id="1200-1400", sharp=False)
    assert sharp_blocked.accepted is False
    assert "Enable sharp/gambit lines" in sharp_blocked.reason_text
    assert sharp_blocked.metadata["reconciled"]["decision_source"] == "sharp_toggle_policy_blocked_admission"

    sharp_allowed = _eval(service, move_uci="d2d4", requested_band_id="1200-1400", sharp=True)
    assert sharp_allowed.accepted is True
    assert sharp_allowed.reason_text == "Accepted via practical-risk reconciliation for the current training band."

    non_sharp = _eval(service, move_uci="e2e4", requested_band_id="1200-1400", sharp=False)
    assert non_sharp.accepted is True
    assert non_sharp.reason_text == "Accepted via practical-risk reconciliation for the current training band."


def test_sharp_admission_policy_block_uses_runtime_explanation_without_failure_row(tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "sharp_gate_runtime_reason.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "d2d4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "sharp/gambit practical", "1400-1600"),
        ],
    )
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE reconciled_move_admissions SET family_label='sharp/gambit' WHERE move_uci='d2d4'"
    )
    conn.commit()
    conn.close()
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")

    result = _eval(service, move_uci="d2d4", requested_band_id="1200-1400", sharp=False)
    assert result.accepted is False
    assert "Enable sharp/gambit lines" in result.reason_text
    assert "Rejected by engine" not in result.reason_text


def test_sharp_toggle_off_no_longer_logs_admitted_rescue(monkeypatch, tmp_path):
    board = chess.Board()
    position_key = _position_key(board)
    db = tmp_path / "sharp_gate_log_regression.sqlite"
    _create_real_schema_db(
        db,
        admissions=[
            (position_key, "1400-1600", "d2d4", 0, 0, 1, 1, "local", "local", "reconciled", "reconciled", "good", "stafford gambit", "1400-1600"),
        ],
        explanations=[
            (position_key, "1400-1600", "d2d4", "good_exclusive", "would_pass_if_sharp_toggle_enabled", "tmpl_sharp", "sharp/gambit", "1400-1600", "1800-2000", "sharp_on", None)
        ],
    )
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE reconciled_move_admissions SET family_label='sharp/gambit', failure_reason_code='would_pass_if_sharp_toggle_enabled' WHERE move_uci='d2d4'"
    )
    conn.commit()
    conn.close()
    service = PracticalRiskReconciledService(db, expected_time_control_id="600+0")
    logs: list[str] = []
    monkeypatch.setattr(evaluator_module, "log_line", lambda message, tag=None: logs.append(message))

    result = _eval(service, move_uci="d2d4", requested_band_id="1200-1400", sharp=False)

    assert result.accepted is False
    assert any("PRACTICAL_RISK_FAIL_INTERCEPT_BEGIN" in line and "sharp_toggle=off" in line for line in logs)
    assert any("PRACTICAL_RISK_FAIL_CONFIRMED" in line and "reason_code=would_pass_if_sharp_toggle_enabled" in line for line in logs)
    assert not any("PRACTICAL_RISK_FAIL_RESCUED" in line and "reason=admitted" in line for line in logs)
