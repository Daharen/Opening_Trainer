from pathlib import Path
import builtins

from opening_trainer.main import _startup_failure_log_path, run
from opening_trainer.ui.gui_app import DuplicateInstanceLaunchBlockedError


class CaptureCalls:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))



def test_default_run_launches_gui_without_cli_flag(monkeypatch):
    launched = CaptureCalls()
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_mode": type("Mode", (), {"value": "dev"})(),
            "runtime_mode_source": "default",
            "runtime_mode_reason": "test",
        },
    )()
    monkeypatch.setattr('opening_trainer.main.load_runtime_config', lambda overrides: runtime_context)
    monkeypatch.setattr('opening_trainer.ui.gui_app.launch_gui', lambda runtime_context=None: launched(runtime_context))

    run([])

    assert launched.calls == [((runtime_context,), {})]


def test_probe_gui_bootstrap_flag_runs_and_returns(monkeypatch):
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_mode": type("Mode", (), {"value": "dev"})(),
            "runtime_mode_source": "default",
            "runtime_mode_reason": "test",
        },
    )()
    probe_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main._probe_gui_bootstrap", lambda context: probe_calls.append(context.runtime_mode.value))

    run(["--probe-gui-bootstrap"])

    assert probe_calls == ["dev"]


def test_probe_real_gui_startup_flag_runs_and_returns(monkeypatch):
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_mode": type("Mode", (), {"value": "consumer"})(),
            "runtime_mode_source": "cli",
            "runtime_mode_reason": "test",
        },
    )()
    probe_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main._probe_real_gui_startup", lambda context: probe_calls.append(context.runtime_mode.value))

    run(["--probe-real-gui-startup", "--runtime-mode", "consumer"])

    assert probe_calls == ["consumer"]


def test_probe_real_gui_startup_runs_from_temp_cwd(monkeypatch, tmp_path):
    runtime_context = object()
    original_cwd = Path.cwd()
    observed_cwd: list[Path] = []
    monkeypatch.chdir(tmp_path)

    def _fake_launch_gui(runtime_context=None, probe_real_startup=False):
        observed_cwd.append(Path.cwd())
        assert probe_real_startup is True

    monkeypatch.setattr("opening_trainer.ui.gui_app.launch_gui", _fake_launch_gui)
    monkeypatch.setattr("opening_trainer.main.log_line", lambda *args, **kwargs: None)

    from opening_trainer.main import _probe_real_gui_startup

    _probe_real_gui_startup(runtime_context)

    assert len(observed_cwd) == 1
    assert observed_cwd[0] != tmp_path
    assert Path.cwd() == tmp_path
    monkeypatch.chdir(original_cwd)


def test_frozen_consumer_gui_failure_writes_artifact_and_exits(monkeypatch, tmp_path):
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_mode": type("Mode", (), {"value": "consumer"})(),
            "runtime_mode_source": "auto-consumer",
            "runtime_mode_reason": "test",
            "runtime_paths": type(
                "RuntimePaths",
                (),
                {
                    "app_state_root": tmp_path / "OpeningTrainer",
                    "log_root": tmp_path / "OpeningTrainer" / "logs",
                },
            )(),
        },
    )()
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main.run_cli", lambda overrides=None: (_ for _ in ()).throw(AssertionError("run_cli must not be used")))
    monkeypatch.setattr("opening_trainer.main.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.main._show_startup_failure_dialog", lambda stage, exc, path: None)
    monkeypatch.setattr("opening_trainer.main.sys.frozen", True, raising=False)

    try:
        run([])
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("SystemExit expected")

    artifact = (tmp_path / "OpeningTrainer" / "startup_failure.log").read_text(encoding="utf-8")
    assert "stage=gui_" in artifact
    assert "runtime_mode=consumer" in artifact
    assert "traceback:" in artifact


def test_dev_gui_import_failure_still_falls_back_to_cli(monkeypatch):
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_mode": type("Mode", (), {"value": "dev"})(),
            "runtime_mode_source": "default",
            "runtime_mode_reason": "test",
        },
    )()
    cli_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main.run_cli", lambda overrides=None: cli_calls.append("cli"))
    monkeypatch.setattr("opening_trainer.main.log_line", lambda *args, **kwargs: None)
    original_import = builtins.__import__

    def _failing_import(name, *args, **kwargs):
        if name == "opening_trainer.ui.gui_app":
            raise RuntimeError("gui import failed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _failing_import)
    monkeypatch.setattr("opening_trainer.main.sys.frozen", False, raising=False)

    run([])

    assert cli_calls == ["cli"]


def test_duplicate_instance_launch_exits_without_cli_fallback(monkeypatch):
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_mode": type("Mode", (), {"value": "consumer"})(),
            "runtime_mode_source": "cli",
            "runtime_mode_reason": "test",
        },
    )()
    cli_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main.run_cli", lambda overrides=None: cli_calls.append("cli"))
    monkeypatch.setattr(
        "opening_trainer.ui.gui_app.launch_gui",
        lambda runtime_context=None: (_ for _ in ()).throw(DuplicateInstanceLaunchBlockedError("blocked")),
    )

    try:
        run(["--runtime-mode", "consumer"])
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("SystemExit expected on duplicate instance launch block.")

    assert cli_calls == []


def test_startup_failure_path_uses_local_app_data(monkeypatch, tmp_path):
    runtime_context = type("RuntimeContext", (), {})()
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Local"))

    path = _startup_failure_log_path(runtime_context)

    assert path == tmp_path / "Local" / "OpeningTrainer" / "startup_failure.log"


def test_run_binds_session_logging_before_logger_creation(monkeypatch, tmp_path):
    calls: list[str] = []
    runtime_context = type(
        "RuntimeContext",
        (),
        {
            "config": type("Config", (), {"strict_assets": False})(),
            "runtime_paths": type("RuntimePaths", (), {"log_root": tmp_path / "runtime" / "logs"})(),
            "config_source": "test",
            "corpus": type("Corpus", (), {"detail": "corpus"})(),
            "book": type("Book", (), {"detail": "book"})(),
            "engine": type("Engine", (), {"detail": "engine"})(),
            "runtime_mode": type("Mode", (), {"value": "dev"})(),
            "runtime_mode_source": "default",
            "runtime_mode_reason": "test",
        },
    )()

    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main.initialize_session_logging", lambda path: calls.append(f"init:{path}"))
    monkeypatch.setattr("opening_trainer.main.get_session_logger", lambda: calls.append("get"))
    monkeypatch.setattr("opening_trainer.main.log_line", lambda *args, **kwargs: calls.append("log"))
    monkeypatch.setattr("opening_trainer.ui.gui_app.launch_gui", lambda runtime_context=None: calls.append("launch_gui"))

    run(["--show-runtime"])

    assert calls[0] == f"init:{tmp_path / 'runtime' / 'logs' / 'sessions'}"
    assert calls[1] == "get"



def test_powershell_runner_auto_mode_is_non_interactive_and_skips_console_bundle_selection():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert '"Auto" {' in script
    run_block = script.split('"Auto" {', 1)[1].split('"Run" {', 1)[0]
    assert 'Select-CorpusBundleDirectory' not in run_block
    assert 'non-interactive path (AutoSafe validate + run, corpus skip)' in run_block
    assert 'Invoke-ValidationProfile -ProfileName "AutoSafe"' in run_block
    dev_block = script.split('"DevRun" {', 1)[1].split('"Test" {', 1)[0]
    assert 'Select-CorpusBundleDirectory' in dev_block


def test_powershell_runner_has_split_validation_profiles_and_timeout_markers():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert '"AutoSafe"' in script
    assert '"DevFast"' in script
    assert '"DevFull"' in script
    assert 'VALIDATION_PROFILE_$($ProfileName.ToUpperInvariant())_BEGIN' in script
    assert 'VALIDATION_PROFILE_TIMEOUT' in script
    assert 'VALIDATION_CHILD_PROCESS_CLEANUP_BEGIN' in script
    assert 'VALIDATION_CHILD_PROCESS_CLEANUP_COMPLETE' in script
    assert 'src/tests/test_launch_paths.py' in script
    assert 'src/tests/test_gui_app.py' in script
    assert 'src/tests/test_shutdown_and_single_instance.py' in script
    assert 'src/tests/test_session_logging.py' in script
    assert 'src/tests/test_engine_process.py' not in script


def test_powershell_runner_bundle_validation_recognizes_sqlite_and_legacy_fallback_order():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'manifest payload_format=sqlite but sqlite payload is missing' in script
    assert "if ($payloadFormat -eq \"sqlite\")" in script
    assert "if ($payloadFormat -eq \"jsonl\")" in script
    assert "if (Test-Path $sqlitePath -PathType Leaf)" in script
    assert "if (Test-Path $aggregatePath -PathType Leaf)" in script
    assert "manifest payload_format -> data/corpus.sqlite -> data/aggregated_position_move_counts.jsonl (legacy)" in script


def test_ordinary_and_developer_launchers_are_split():
    ordinary = Path('Launch_Opening_Trainer.vbs').read_text(encoding='utf-8')
    developer = Path('Launch_Opening_Trainer_Dev.cmd').read_text(encoding='utf-8')

    assert "-Action Auto" in ordinary
    assert "shell.Run cmd, 0" in ordinary
    assert "-Action Menu" in developer


def test_powershell_ordinary_launch_includes_splash_and_single_instance_guards():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'function Show-StartupSplash' in script
    assert 'Opening Trainer is starting' in script
    assert 'Initializing environment' in script
    assert 'Validating runtime' in script
    assert 'Launching trainer' in script
    assert 'Opening GUI' in script
    assert 'Try-OpenExistingMutex -Name $BootMutexName' in script
    assert 'Try-OpenExistingMutex -Name $AppMutexName' in script
    assert 'Wait-ForStartupHandoff' in script
    assert 'GUI_READY:' in script
    assert 'GUI_STARTUP_FAILED:' in script
    assert 'APP_DUPLICATE_OWNER_INFO_AVAILABLE:' in script


def test_powershell_ordinary_failure_message_points_to_session_log_and_dev_launcher():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'Startup failed' in script
    assert 'See session log:' in script
    assert 'Launch_Opening_Trainer_Dev.cmd' in script
