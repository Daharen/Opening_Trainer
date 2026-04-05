from pathlib import Path
import sys
from types import SimpleNamespace

from opening_trainer.main import _startup_failure_log_path, run
from opening_trainer.ui.gui_app import DuplicateInstanceLaunchBlockedError


class CaptureCalls:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))



def test_default_run_launches_gui_without_cli_flag(monkeypatch, runtime_context_factory):
    launched = CaptureCalls()
    runtime_context = runtime_context_factory(runtime_mode="dev")
    monkeypatch.setattr('opening_trainer.main.load_runtime_config', lambda overrides: runtime_context)
    monkeypatch.setattr('opening_trainer.ui.gui_app.launch_gui', lambda runtime_context=None: launched(runtime_context))

    run([])

    assert launched.calls == [((runtime_context,), {})]


def test_probe_gui_bootstrap_flag_runs_and_returns(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="dev")
    probe_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main._probe_gui_bootstrap", lambda context: probe_calls.append(context.runtime_mode.value))

    run(["--probe-gui-bootstrap"])

    assert probe_calls == ["dev"]


def test_probe_real_gui_startup_flag_runs_and_returns(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="consumer", runtime_mode_source="cli")
    probe_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main._probe_real_gui_startup", lambda context: probe_calls.append(context.runtime_mode.value))

    run(["--probe-real-gui-startup", "--runtime-mode", "consumer"])

    assert probe_calls == ["consumer"]


def test_probe_real_gui_startup_runs_from_temp_cwd(monkeypatch, tmp_path):
    runtime_context = object()
    original_cwd = Path.cwd()
    observed_cwd: list[Path] = []
    logs: list[str] = []
    monkeypatch.chdir(tmp_path)

    def _fake_launch_gui(runtime_context=None, probe_real_startup=False):
        observed_cwd.append(Path.cwd())
        assert probe_real_startup is True

    monkeypatch.setattr("opening_trainer.ui.gui_app.launch_gui", _fake_launch_gui)
    monkeypatch.setattr("opening_trainer.main.log_line", lambda message, **kwargs: logs.append(message))

    from opening_trainer.main import _probe_real_gui_startup

    _probe_real_gui_startup(runtime_context)

    assert len(observed_cwd) == 1
    assert observed_cwd[0] != tmp_path
    assert Path.cwd() == tmp_path
    assert any("GUI_PROBE_TEMP_CWD_CREATED" in message for message in logs)
    assert any("GUI_PROBE_TEMP_CWD_RESTORED" in message for message in logs)
    assert "GUI_PROBE_REAL_STARTUP_OK" in logs
    monkeypatch.chdir(original_cwd)


def test_probe_real_gui_startup_tolerates_temp_dir_cleanup_permission_error(monkeypatch, tmp_path):
    runtime_context = object()
    log_messages: list[str] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("opening_trainer.main.time.sleep", lambda _: None)

    def _fake_launch_gui(runtime_context=None, probe_real_startup=False):
        assert probe_real_startup is True

    monkeypatch.setattr("opening_trainer.ui.gui_app.launch_gui", _fake_launch_gui)
    monkeypatch.setattr("opening_trainer.main.shutil.rmtree", lambda path: (_ for _ in ()).throw(PermissionError("locked")))
    monkeypatch.setattr("opening_trainer.main.log_line", lambda message, **kwargs: log_messages.append(message))

    from opening_trainer.main import _probe_real_gui_startup

    _probe_real_gui_startup(runtime_context)

    assert Path.cwd() == tmp_path
    assert any("GUI_PROBE_TEMP_CWD_CLEANUP_RETRY" in message for message in log_messages)
    assert any("GUI_PROBE_TEMP_CWD_CLEANUP_DEFERRED" in message for message in log_messages)
    assert "GUI_PROBE_REAL_STARTUP_OK" in log_messages


def test_frozen_consumer_gui_import_failure_writes_artifact_and_exits(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="consumer", runtime_mode_source="auto-consumer")
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main.run_cli", lambda overrides=None: (_ for _ in ()).throw(AssertionError("run_cli must not be used")))
    monkeypatch.setattr("opening_trainer.main.log_line", lambda *args, **kwargs: None)
    monkeypatch.setattr("opening_trainer.main._show_startup_failure_dialog", lambda stage, exc, path: None)
    monkeypatch.setattr("opening_trainer.main.sys.frozen", True, raising=False)
    monkeypatch.setitem(sys.modules, "opening_trainer.ui.gui_app", None)

    try:
        run([])
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("SystemExit expected")

    artifact = (runtime_context.runtime_paths.app_state_root / "startup_failure.log").read_text(encoding="utf-8")
    assert "stage=gui_import" in artifact
    assert "runtime_mode=consumer" in artifact
    assert "traceback:" in artifact


def test_dev_gui_import_failure_still_falls_back_to_cli(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="dev")
    cli_calls: list[str] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.main.run_cli", lambda overrides=None: cli_calls.append("cli"))
    monkeypatch.setattr("opening_trainer.main.log_line", lambda *args, **kwargs: None)
    monkeypatch.setitem(sys.modules, "opening_trainer.ui.gui_app", None)
    monkeypatch.setattr("opening_trainer.main.sys.frozen", False, raising=False)

    run([])

    assert cli_calls == ["cli"]


def test_duplicate_instance_launch_exits_without_cli_fallback(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="consumer", runtime_mode_source="cli")
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


def test_apply_update_flag_uses_updater_helper(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="consumer", runtime_mode_source="cli")
    helper_calls: list[dict] = []
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr(
        "opening_trainer.main.launch_updater_helper",
        lambda manifest_path_or_url, **kwargs: helper_calls.append({"manifest": manifest_path_or_url, **kwargs}),
    )

    run(["--runtime-mode", "consumer", "--apply-update", "https://example.invalid/manifest.json"])

    assert len(helper_calls) == 1
    assert helper_calls[0]["manifest"] == "https://example.invalid/manifest.json"


def test_startup_failure_path_uses_local_app_data(monkeypatch, tmp_path):
    runtime_context = type("RuntimeContext", (), {})()
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Local"))

    path = _startup_failure_log_path(runtime_context)

    assert path == tmp_path / "Local" / "OpeningTrainer" / "startup_failure.log"


def test_run_binds_session_logging_before_logger_creation(monkeypatch, tmp_path, runtime_context_factory):
    calls: list[str] = []
    runtime_context = runtime_context_factory(runtime_mode="dev")
    runtime_context.runtime_paths.log_root = tmp_path / "runtime" / "logs"
    runtime_context.config_source = "test"

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


def test_powershell_runner_bootstraps_pytest_for_validation_profiles():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'function Ensure-Pytest' in script
    assert 'Validation tool bootstrap: checking pytest availability in repo-local virtual environment...' in script
    assert 'Validation tool bootstrap: pytest already available in repo-local virtual environment.' in script
    assert 'Validation tool bootstrap: pytest missing in repo-local virtual environment; installing pytest...' in script
    assert 'Validation tool bootstrap: pytest install completed.' in script
    assert 'Ensure-Pytest -VenvPython $VenvPython -VenvPip $venv.VenvPip' in script


def test_powershell_runner_validation_commands_resolve_pytest_entrypoint_with_module_fallback():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'function Resolve-PytestRunner' in script
    assert '(Join-Path $VenvDir "Scripts\\py.test.exe")' in script
    assert '(Join-Path $VenvDir "Scripts\\pytest.exe")' in script
    assert 'ArgumentsPrefix = @("-m", "pytest")' in script
    assert 'Invoke-ValidationCommand -Name "autosafe_pytest_subset" -FilePath $pytestRunner.FilePath' in script
    assert 'Invoke-ValidationCommand -Name "devfast_pytest_subset" -FilePath $pytestRunner.FilePath' in script
    assert 'Invoke-ValidationCommand -Name "pytest_full" -FilePath $pytestRunner.FilePath' in script
    assert 'pytest entrypoint reused' in script
    assert 'python -m pytest fallback used' in script
    assert 'chosen executable path' in script


def test_powershell_runner_bundle_validation_recognizes_sqlite_and_legacy_fallback_order():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'manifest payload_format=sqlite but sqlite payload is missing (.sqlite or .sqlite.zst)' in script
    assert "if ($payloadFormat -eq \"sqlite\")" in script
    assert "if ($payloadFormat -eq \"jsonl\")" in script
    assert '$sqliteZstPath = "$sqlitePath.zst"' in script
    assert "if ((Test-Path $sqlitePath -PathType Leaf) -or (Test-Path $sqliteZstPath -PathType Leaf))" in script
    assert "if (Test-Path $aggregatePath -PathType Leaf)" in script
    assert "manifest payload_format -> data/corpus.sqlite(.zst) -> data/aggregated_position_move_counts.jsonl (legacy)" in script


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


def test_strict_assets_does_not_exit_when_only_corpus_bundle_is_unavailable(monkeypatch, runtime_context_factory):
    runtime_context = runtime_context_factory(runtime_mode="consumer")
    runtime_context.config = SimpleNamespace(strict_assets=True)
    runtime_context.corpus = SimpleNamespace(label="corpus bundle directory", path=Path("/missing/catalog-root"), available=False)
    runtime_context.engine = SimpleNamespace(label="engine", path=Path("/engine"), available=True)
    runtime_context.book = SimpleNamespace(label="opening book", path=Path("/book"), available=True)

    launched = CaptureCalls()
    monkeypatch.setattr("opening_trainer.main.load_runtime_config", lambda overrides: runtime_context)
    monkeypatch.setattr("opening_trainer.ui.gui_app.launch_gui", lambda runtime_context=None: launched(runtime_context))

    run(["--runtime-mode", "consumer"])

    assert launched.calls == [((runtime_context,), {})]
