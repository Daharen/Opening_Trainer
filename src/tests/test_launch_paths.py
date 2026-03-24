from pathlib import Path

from opening_trainer.main import run


class CaptureCalls:
    def __init__(self):
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))



def test_default_run_launches_gui_without_cli_flag(monkeypatch):
    launched = CaptureCalls()
    runtime_context = type('RuntimeContext', (), {'config': type('Config', (), {'strict_assets': False})()})()
    monkeypatch.setattr('opening_trainer.main.load_runtime_config', lambda overrides: runtime_context)
    monkeypatch.setattr('opening_trainer.ui.gui_app.launch_gui', lambda runtime_context=None: launched(runtime_context))

    run([])

    assert launched.calls == [((runtime_context,), {})]



def test_powershell_runner_auto_mode_is_non_interactive_and_skips_console_bundle_selection():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert '"Auto" {' in script
    run_block = script.split('"Auto" {', 1)[1].split('"Run" {', 1)[0]
    assert 'Select-CorpusBundleDirectory' not in run_block
    assert 'non-interactive path (validate + run, corpus skip)' in run_block
    dev_block = script.split('"DevRun" {', 1)[1].split('"Test" {', 1)[0]
    assert 'Select-CorpusBundleDirectory' in dev_block


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


def test_powershell_ordinary_failure_message_points_to_session_log_and_dev_launcher():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert 'Startup failed' in script
    assert 'See session log:' in script
    assert 'Launch_Opening_Trainer_Dev.cmd' in script
