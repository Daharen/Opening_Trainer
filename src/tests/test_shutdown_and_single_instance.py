from __future__ import annotations

from types import SimpleNamespace

from opening_trainer.single_instance import cleanup_stale_instance_diagnostics
from opening_trainer.ui.gui_app import ANIMATION_IMPL_MARKER, OpeningTrainerGUI, launch_gui


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


class FakeDialog:
    def __init__(self):
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class FakeWindow:
    def __init__(self):
        self.destroy_calls = 0

    def winfo_exists(self):
        return True

    def destroy(self):
        self.destroy_calls += 1


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


def test_shutdown_coordinator_tolerates_missing_timing_override_dialog(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = set()
    gui.dev_console = FakeDevConsole()
    gui.session = SimpleNamespace(close=lambda: None)
    gui._child_windows = []
    gui.root = FakeRoot()
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)

    gui._shutdown_coordinator(reason="missing_dialog")

    assert gui.root.destroy_calls == 1


def test_shutdown_coordinator_tolerates_none_timing_override_dialog(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = set()
    gui.dev_console = FakeDevConsole()
    gui.timing_override_dialog = None
    gui.session = SimpleNamespace(close=lambda: None)
    gui._child_windows = []
    gui.root = FakeRoot()
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)

    gui._shutdown_coordinator(reason="none_dialog")

    assert gui.root.destroy_calls == 1


def test_shutdown_coordinator_closes_timing_override_dialog_when_present(monkeypatch):
    dialog = FakeDialog()
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = set()
    gui.dev_console = FakeDevConsole()
    gui.timing_override_dialog = dialog
    gui.session = SimpleNamespace(close=lambda: None)
    gui._child_windows = []
    gui.root = FakeRoot()
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)

    gui._shutdown_coordinator(reason="dialog_present")

    assert dialog.close_calls == 1


def test_shutdown_coordinator_cancels_after_handles_and_clears_them(monkeypatch):
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = {"after_1", "after_2"}
    gui.dev_console = FakeDevConsole()
    gui.session = SimpleNamespace(close=lambda: None)
    gui._child_windows = []
    gui.root = FakeRoot()
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)

    gui._shutdown_coordinator(reason="cancel_after")

    assert sorted(gui.root.cancelled) == ["after_1", "after_2"]
    assert gui._after_handles == set()


def test_shutdown_coordinator_closes_child_windows(monkeypatch):
    tracked_window = FakeWindow()
    toplevel_window = FakeWindow()
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = set()
    gui.dev_console = FakeDevConsole()
    gui.session = SimpleNamespace(close=lambda: None)
    gui._child_windows = [tracked_window]
    gui.root = FakeRoot()
    gui.root.children = [toplevel_window]
    monkeypatch.setattr("opening_trainer.ui.gui_app.tk.Toplevel", FakeWindow)
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)

    gui._shutdown_coordinator(reason="child_windows")

    assert tracked_window.destroy_calls == 1
    assert toplevel_window.destroy_calls == 1
    assert gui._child_windows == []


def test_shutdown_coordinator_invokes_session_close(monkeypatch):
    calls: list[str] = []
    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui._shutdown_started = False
    gui._is_shutting_down = False
    gui._after_handles = set()
    gui.dev_console = FakeDevConsole()
    gui.session = SimpleNamespace(close=lambda: calls.append("session_close"))
    gui._child_windows = []
    gui.root = FakeRoot()
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)

    gui._shutdown_coordinator(reason="session_close")

    assert calls == ["session_close"]


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


def test_launch_gui_logs_animation_implementation_marker(monkeypatch):
    lines: list[str] = []
    monkeypatch.setattr("opening_trainer.ui.gui_app.acquire_single_instance_guard", lambda: True)
    monkeypatch.setattr("opening_trainer.ui.gui_app.write_instance_diagnostics", lambda **kwargs: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.remove_instance_diagnostics", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.release_single_instance_guard", lambda: None)
    monkeypatch.setattr("opening_trainer.ui.gui_app.log_line", lambda message, tag="startup": lines.append(message))

    class FakeApp:
        def __init__(self, runtime_context=None):
            self.runtime_context = runtime_context

        def run(self):
            return None

    monkeypatch.setattr("opening_trainer.ui.gui_app.OpeningTrainerGUI", FakeApp)

    launch_gui()

    assert any(line.startswith("GUI_ANIM_IMPL_VERSION:") and f"marker={ANIMATION_IMPL_MARKER}" in line for line in lines)


def test_cleanup_stale_instance_diagnostics_when_mutex_is_free(monkeypatch, tmp_path):
    diagnostics_path = tmp_path / "instance.json"
    diagnostics_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("OPENING_TRAINER_INSTANCE_DIAGNOSTICS_PATH", str(diagnostics_path))

    removed = cleanup_stale_instance_diagnostics()

    assert removed is True
    assert diagnostics_path.exists() is False
