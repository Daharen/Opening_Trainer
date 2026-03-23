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



def test_powershell_runner_run_mode_skips_console_bundle_selection():
    script = Path('run.ps1').read_text(encoding='utf-8')

    assert '"Run" {' in script
    run_block = script.split('"Run" {', 1)[1].split('"DevRun" {', 1)[0]
    assert 'Select-CorpusBundleDirectory' not in run_block
    assert 'desktop-first trainer without pre-GUI console corpus selection' in run_block
    dev_block = script.split('"DevRun" {', 1)[1].split('"Test" {', 1)[0]
    assert 'Select-CorpusBundleDirectory' in dev_block
