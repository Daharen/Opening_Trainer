from __future__ import annotations

from types import SimpleNamespace

from opening_trainer.single_instance import cleanup_stale_instance_diagnostics
from opening_trainer.ui.gui_app import OpeningTrainerGUI, launch_gui


class FakeRoot:
    def __init__(self):
        self.destroy_calls = 0
        self.cancelled: list[str] = []
        self.children = []

    def after_cancel(self, handle: str) -> None:
        self.cancelled.append(handle)

    def winfo_children(self):
        return list(self.children)

    def destroy(self) -> None:
        self.destroy_calls += 1


class FakeDevConsole:
    def __init__(self):
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


def test_shutdown_coordinator_is_idempotent(monkeypatch):
    events: list[str] = []
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = {"a1", "a2"}
    gui.dev_console = FakeDevConsole()
    gui.session = SimpleNamespace(close=lambda: events.append("session_close"))
    gui._child_windows = []
    gui.root = FakeRoot()
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda message, tag="startup": events.append(message))
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: events.append("remove_diag"))
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: events.append("release_guard"))

    gui._shutdown_coordinator(reason="test")
    gui._shutdown_coordinator(reason="test_again")

    assert gui.root.destroy_calls == 1
    assert events.count("session_close") == 1
    assert events.count("remove_diag") == 1
    assert events.count("release_guard") == 1


def test_launch_gui_logs_duplicate_owner_info_when_available(monkeypatch):
    lines: list[str] = []
    owner = SimpleNamespace(
        pid=4242,
        startup_utc="2026-03-25T01:02:03Z",
        session_log_path="logs/sessions/session_abc.log",
        session_id="abc",
    )
    monkeypatch.setattr("opening_trainer.ui.gui_app.acquire_single_instance_guard", lambda: False)
    monkeypatch.setattr("opening_trainer.ui.gui_app.read_instance_diagnostics", lambda: owner)
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda message, tag="startup": lines.append(message))

    launch_gui()

    assert any(line.startswith("APP_DUPLICATE_BLOCKED") for line in lines)
    assert any("APP_DUPLICATE_OWNER_INFO_AVAILABLE" in line and "pid=4242" in line for line in lines)


def test_cleanup_stale_instance_diagnostics_when_mutex_is_free(monkeypatch, tmp_path):
    diagnostics_path = tmp_path / "instance.json"
    diagnostics_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("OPENING_TRAINER_INSTANCE_DIAGNOSTICS_PATH", str(diagnostics_path))

    removed = cleanup_stale_instance_diagnostics()

    assert removed is True
    assert diagnostics_path.exists() is False
