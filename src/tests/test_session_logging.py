from pathlib import Path

import opening_trainer.session_logging as session_logging


def test_session_logger_caps_visible_and_file_lines(tmp_path):
    log_path = tmp_path / "session.log"
    logger = session_logging.SessionLogger(session_id="test", log_path=log_path, mirror_to_console=False)

    for i in range(10_050):
        logger.append(f"line {i}", tag="startup")

    visible = logger.visible_lines()
    persisted = logger.bootstrap_lines()
    assert len(visible) == session_logging.RING_BUFFER_MAX_LINES
    assert len(persisted) == session_logging.SESSION_FILE_MAX_LINES
    assert "line 50" in persisted[0]
    assert "line 10049" in persisted[-1]


def test_session_logger_subscribe_and_clear_visible_buffer(tmp_path):
    log_path = tmp_path / "session.log"
    logger = session_logging.SessionLogger(session_id="test", log_path=log_path, mirror_to_console=False)

    captured: list[str] = []
    unsubscribe = logger.subscribe(captured.append)
    logger.append("boot", tag="startup")
    logger.append("fail", tag="error")
    unsubscribe()
    logger.append("post", tag="startup")

    assert len(captured) == 2
    assert "[startup] boot" in captured[0]
    assert "[error] fail" in captured[1]

    logger.clear_visible_buffer()
    assert logger.visible_lines() == ()
    assert len(logger.bootstrap_lines()) == 3


def test_prune_old_session_files_keeps_latest_five(tmp_path, monkeypatch):
    monkeypatch.setenv(session_logging.SESSION_LOG_DIR_ENV, str(tmp_path))
    monkeypatch.setenv(session_logging.SESSION_ID_ENV, "active")
    monkeypatch.delenv(session_logging.SESSION_LOG_PATH_ENV, raising=False)
    session_logging.reset_logger_for_tests()

    for idx in range(7):
        path = tmp_path / f"session_old_{idx}.log"
        path.write_text("x\n", encoding="utf-8")

    logger = session_logging.get_session_logger()
    assert logger.log_path.exists()
    kept = sorted(p.name for p in tmp_path.glob("session_*.log"))
    assert len(kept) == session_logging.MAX_SESSION_FILES


def test_initialize_session_logging_creates_directory_and_binds_logger(tmp_path, monkeypatch):
    consumer_sessions = tmp_path / "LocalAppData" / "OpeningTrainer" / "logs" / "sessions"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(session_logging.SESSION_LOG_PATH_ENV, raising=False)
    monkeypatch.delenv(session_logging.SESSION_ID_ENV, raising=False)
    session_logging.reset_logger_for_tests()

    session_logging.initialize_session_logging(consumer_sessions)
    logger = session_logging.get_session_logger()

    assert consumer_sessions.exists()
    assert logger.log_path.parent == consumer_sessions
    assert not (tmp_path / "logs" / "sessions").exists()


def test_logger_reinitialization_replaces_stale_singleton_path(tmp_path, monkeypatch):
    dev_sessions = tmp_path / "runtime" / "logs" / "sessions"
    consumer_sessions = tmp_path / "LocalAppData" / "OpeningTrainer" / "logs" / "sessions"
    monkeypatch.delenv(session_logging.SESSION_LOG_PATH_ENV, raising=False)
    monkeypatch.delenv(session_logging.SESSION_ID_ENV, raising=False)
    session_logging.reset_logger_for_tests()

    session_logging.initialize_session_logging(dev_sessions)
    first_logger = session_logging.get_session_logger()

    session_logging.initialize_session_logging(consumer_sessions)
    second_logger = session_logging.get_session_logger()

    assert first_logger.log_path.parent == dev_sessions
    assert second_logger.log_path.parent == consumer_sessions
    assert first_logger is not second_logger


def test_logger_reports_binding_source_for_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(session_logging.SESSION_LOG_DIR_ENV, raising=False)
    monkeypatch.delenv(session_logging.SESSION_LOG_PATH_ENV, raising=False)
    monkeypatch.delenv(session_logging.SESSION_ID_ENV, raising=False)
    session_logging.reset_logger_for_tests()

    logger = session_logging.get_session_logger()

    assert logger.log_path.parent.resolve() == (tmp_path / "logs" / "sessions").resolve()
    assert any("source=fallback-relative" in line for line in logger.visible_lines())
