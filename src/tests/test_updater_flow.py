from __future__ import annotations

import base64
import json
from pathlib import Path

from opening_trainer.install_layout import choose_mutable_app_root, write_installed_app_manifest
import pytest

from opening_trainer.updater import (
    INSTALL_DIAGNOSTIC_MARKER,
    UpdaterInstallStateError,
    UpdaterUnsupportedInRuntimeError,
    check_for_update,
    evaluate_updater_runtime_support,
    launch_updater_helper,
    log_install_runtime_diagnostics,
    resolve_manifest_path_or_url,
)


def test_apply_helper_avoids_reserved_pid_variable_name():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert "param([int]$Pid" not in script
    assert "Wait-ForProcessExit -Pid" not in script


def test_apply_helper_relaunch_trampoline_includes_popup_suppressor_contract():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert "$suppressorTimeoutSeconds = 25" in script
    assert "$suppressorPollMilliseconds = 100" in script
    assert "popup_suppressor_" in script
    assert "EnumWindows" in script
    assert "if ($title -cne 'Error') { continue }" in script
    assert "popup_signature=title=Error_any" in script
    assert "SendMessage($hWnd, $wmCommand, [IntPtr]$idOk" in script
    assert "SetForegroundWindow($hWnd)" in script
    assert "POPUP_SUPPRESSOR_ENTER_SENT" in script
    assert "POPUP_SUPPRESSOR_SPACE_SENT" in script
    assert "POPUP_SUPPRESSOR_WM_CLOSE_SENT" in script
    assert "Stop-Process -Id ([int]$ownerPid) -Force" in script
    assert "POPUP_SUPPRESSOR_STARTED" in script
    assert "POPUP_SUPPRESSOR_MATCH_DETECTED" in script
    assert "POPUP_SUPPRESSOR_OWNER_KILLED" in script
    assert "POPUP_SUPPRESSOR_EXITED_AFTER_TIMEOUT" in script
    assert "POPUP_SUPPRESSOR_LAUNCH result=started mode=hidden_detached" in script
    assert "Start-ErrorPopupSuppressor -SuppressorScriptPath" in script
    assert "Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -ArgumentList @(" in script


def test_mutable_app_root_fallback_order(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Local"))
    monkeypatch.setenv("USERPROFILE", str(tmp_path / "User"))

    chosen, probes = choose_mutable_app_root()

    assert chosen == tmp_path / "Local" / "OpeningTrainer" / "App"
    assert probes[0].ok is True


def test_installed_manifest_persists_mutable_root(tmp_path):
    app_state = tmp_path / "OpeningTrainer"
    target = tmp_path / "Local" / "OpeningTrainer" / "App"

    path = write_installed_app_manifest(
        app_state_root=app_state,
        app_version="1.2.3",
        channel="dev",
        mutable_app_root=target,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="abc123",
        bootstrap_version="1.0.0",
        build_id="commit-1",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["mutable_app_root"] == str(target)
    assert payload["channel"] == "dev"
    assert payload["build_id"] == "commit-1"


def test_update_manifest_comparison(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "commit-abc",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    app_state = tmp_path / "OpeningTrainer"
    mutable_root = tmp_path / "app"
    bundled_helper = mutable_root / "updater" / "apply_app_update.ps1"
    bundled_wrapper = mutable_root / "updater" / "invoke_apply_app_update.ps1"
    bundled_helper.parent.mkdir(parents=True, exist_ok=True)
    bundled_helper.write_text("Write-Host helper", encoding="utf-8")
    bundled_wrapper.write_text("Write-Host wrapper", encoding="utf-8")
    write_installed_app_manifest(
        app_state_root=app_state,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="old",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )

    has_update, manifest, installed = check_for_update(str(manifest_path), app_state_root=app_state)
    assert has_update is True
    assert manifest.app_version == "2.0.0"
    assert manifest.build_id == "commit-abc"
    assert installed is not None


def test_dev_runtime_without_installed_manifest_reports_updater_unavailable(tmp_path):
    app_state = tmp_path / "OpeningTrainer"
    supported, reason = evaluate_updater_runtime_support(app_state_root=app_state, runtime_mode="dev")

    assert supported is False
    assert reason == "Updater is only available for installed consumer builds."


def test_check_for_update_in_dev_runtime_missing_installed_manifest_is_non_install_corruption_error(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "commit-abc",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    app_state = tmp_path / "OpeningTrainer"

    with pytest.raises(UpdaterUnsupportedInRuntimeError, match="installed consumer builds"):
        check_for_update(str(manifest_path), app_state_root=app_state, runtime_mode="dev")


def test_check_for_update_in_consumer_runtime_missing_installed_manifest_still_fails_strictly(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "commit-abc",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    app_state = tmp_path / "OpeningTrainer"

    with pytest.raises(UpdaterInstallStateError, match="missing required updater metadata"):
        check_for_update(str(manifest_path), app_state_root=app_state, runtime_mode="consumer")


def test_update_available_when_version_equal_but_build_id_differs(tmp_path):
    manifest_path = tmp_path / "app_update_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "commit-new",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    app_state = tmp_path / "OpeningTrainer"
    mutable_root = tmp_path / "app"
    bundled_helper = mutable_root / "updater" / "apply_app_update.ps1"
    bundled_wrapper = mutable_root / "updater" / "invoke_apply_app_update.ps1"
    bundled_helper.parent.mkdir(parents=True, exist_ok=True)
    bundled_helper.write_text("Write-Host helper", encoding="utf-8")
    bundled_wrapper.write_text("Write-Host wrapper", encoding="utf-8")
    write_installed_app_manifest(
        app_state_root=app_state,
        app_version="2.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="old-sha",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )

    has_update, _manifest, _installed = check_for_update(str(manifest_path), app_state_root=app_state)
    assert has_update is True


def test_resolve_manifest_path_uses_installed_updater_config(tmp_path):
    app_state = tmp_path / "OpeningTrainer"
    updater_root = app_state / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    expected = "https://example.invalid/repo-main/app_update_manifest.json"
    (updater_root / "updater_config.json").write_text(
        json.dumps({"channel": "dev", "manifest_url": expected}),
        encoding="utf-8",
    )

    assert resolve_manifest_path_or_url(None, app_state_root=app_state) == expected


def test_launch_updater_helper_sets_safe_cwd_outside_mutable_root(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=app_state_root / "App",
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="old",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    helper_path = updater_root / "apply_app_update.ps1"
    wrapper_path = updater_root / "invoke_apply_app_update.ps1"
    helper_path.write_text("Write-Host helper", encoding="utf-8")
    wrapper_path.write_text("Write-Host wrapper", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    popen_calls: list[dict] = []
    log_messages: list[str] = []

    class DummyProcess:
        def poll(self):
            return 1

    def _fake_popen(cmd, **kwargs):
        popen_calls.append({"cmd": cmd, **kwargs})
        return DummyProcess()

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", _fake_popen)
    monkeypatch.setattr("opening_trainer.updater.log_line", lambda message, tag="startup": log_messages.append(message))

    launch_updater_helper(
        str(manifest_path),
        app_state_root=app_state_root,
        wait_for_pid=1234,
        relaunch_exe_path=None,
        relaunch_args=None,
    )

    assert len(popen_calls) == 1
    assert popen_calls[0]["cwd"] == str(app_state_root / "updater")
    encoded_command_index = popen_calls[0]["cmd"].index("-EncodedCommand")
    decoded_bootstrap = base64.b64decode(popen_calls[0]["cmd"][encoded_command_index + 1]).decode("utf-16le")
    assert "encoded_bootstrap_v1_entered" in decoded_bootstrap
    assert "$payloadTransport = 'json_base64_utf8'" in decoded_bootstrap
    assert "payload_transport={0}" in decoded_bootstrap
    assert "$payloadJsonBase64 = '" in decoded_bootstrap
    assert "[System.Convert]::FromBase64String($payloadJsonBase64)" in decoded_bootstrap
    assert "[System.Text.Encoding]::UTF8.GetString($payloadJsonBytes)" in decoded_bootstrap
    assert "$payloadJson = [string]$payloadJsonText" in decoded_bootstrap
    assert "ConvertFrom-Json -InputObject $payloadJson" in decoded_bootstrap
    assert "ConvertFrom-Json -InputObject \"{" not in decoded_bootstrap
    assert "$wrapperPath = [string]$payload.wrapper_path" in decoded_bootstrap
    assert "$helperPath = [string]$payload.helper_path" in decoded_bootstrap
    assert any("UPDATER_HELPER_LAUNCH" in msg for msg in log_messages)
    launch_audit = json.loads((app_state_root / "updater" / "launch_helper.audit.json").read_text(encoding="utf-8"))
    assert launch_audit["launch_mode"] == "encoded_bootstrap_v1"
    assert launch_audit["relaunch_args_json"] == "[\"--runtime-mode\", \"consumer\"]"
    assert "bootstrap_launch_log" in launch_audit["expected_artifacts"]
    assert "bootstrap_host_stdout_log" in launch_audit["expected_artifacts"]
    assert "bootstrap_host_stderr_log" in launch_audit["expected_artifacts"]


def test_launch_updater_helper_bootstrap_avoids_inline_raw_json_payload(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")

    popen_calls: list[dict] = []

    class DummyProcess:
        def poll(self):
            return 1

    def _fake_popen(cmd, **kwargs):
        popen_calls.append({"cmd": cmd, **kwargs})
        return DummyProcess()

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", _fake_popen)

    launch_updater_helper(None, app_state_root=app_state_root, wait_for_pid=1234)

    encoded_command_index = popen_calls[0]["cmd"].index("-EncodedCommand")
    decoded_bootstrap = base64.b64decode(popen_calls[0]["cmd"][encoded_command_index + 1]).decode("utf-16le")
    assert "quoted_payload" not in decoded_bootstrap
    assert "ConvertFrom-Json -InputObject \"{" not in decoded_bootstrap
    assert "ConvertFrom-Json -InputObject $payloadJson" in decoded_bootstrap


def test_launch_updater_helper_self_heals_helper_from_mutable_root(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    bundled_helper = mutable_root / "updater" / "apply_app_update.ps1"
    bundled_wrapper = mutable_root / "updater" / "invoke_apply_app_update.ps1"
    bundled_helper.parent.mkdir(parents=True, exist_ok=True)
    bundled_helper.write_text("Write-Host bundled-helper", encoding="utf-8")
    bundled_wrapper.write_text("Write-Host wrapper", encoding="utf-8")
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    popen_calls: list[dict] = []

    class DummyProcess:
        def poll(self):
            return 1

    def _fake_popen(cmd, **kwargs):
        popen_calls.append({"cmd": cmd, **kwargs})
        return DummyProcess()

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", _fake_popen)

    launch_updater_helper(str(manifest_path), app_state_root=app_state_root, wait_for_pid=1234)

    assert (app_state_root / "updater" / "apply_app_update.ps1").exists()
    assert (app_state_root / "updater" / "invoke_apply_app_update.ps1").exists()
    assert len(popen_calls) == 1


def test_launch_updater_helper_self_heals_helper_from_bootstrap_installer(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    mutable_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    bootstrap_root = tmp_path / "Program Files" / "Opening Trainer" / "installer"
    bootstrap_root.mkdir(parents=True, exist_ok=True)
    (bootstrap_root / "apply_app_update.ps1").write_text("Write-Host installer-helper", encoding="utf-8")
    (bootstrap_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")
    monkeypatch.setattr("opening_trainer.updater._bootstrap_root_candidates", lambda app_state_root: [bootstrap_root])

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    class DummyProcess:
        def poll(self):
            return 1

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", lambda cmd, **kwargs: DummyProcess())
    launch_updater_helper(str(manifest_path), app_state_root=app_state_root, wait_for_pid=1234)

    assert (app_state_root / "updater" / "apply_app_update.ps1").read_text(encoding="utf-8") == "Write-Host installer-helper"
    assert (app_state_root / "updater" / "invoke_apply_app_update.ps1").read_text(encoding="utf-8") == "Write-Host wrapper"


def test_launch_updater_helper_classifies_missing_bootstrap_proof(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")

    class DummyProcess:
        def poll(self):
            return 1

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", lambda cmd, **kwargs: DummyProcess())

    result = launch_updater_helper(None, app_state_root=app_state_root, wait_for_pid=1234)
    assert result.helper_bootstrap_proven is False
    assert result.failure_detail == "bootstrap_not_proven_process_exited_without_matching_proof"


def test_launch_updater_helper_classifies_bootstrap_wrapper_nonzero(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")
    attempt_id = "attempt-fixed-1"
    (updater_root / "apply_update.bootstrap.failure.log").write_text(
        f"2026-04-03T00:00:00Z stage=bootstrap_wrapper_nonzero_exit update_attempt_id={attempt_id}",
        encoding="utf-8",
    )

    class DummyUuid:
        hex = attempt_id

    class DummyProcess:
        def poll(self):
            return 1

    monkeypatch.setattr("opening_trainer.updater.uuid.uuid4", lambda: DummyUuid())
    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", lambda cmd, **kwargs: DummyProcess())

    result = launch_updater_helper(None, app_state_root=app_state_root, wait_for_pid=1234)
    assert result.helper_bootstrap_proven is False
    assert result.failure_detail == "bootstrap_proven_wrapper_nonzero_exit"


def test_launch_updater_helper_classifies_wrapper_proven_without_helper_bootstrap(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")
    attempt_id = "attempt-fixed-2"
    (updater_root / "apply_update.wrapper.log").write_text(
        f"2026-04-03T00:00:00Z WRAPPER_ENTERED update_attempt_id={attempt_id}",
        encoding="utf-8",
    )

    class DummyUuid:
        hex = attempt_id

    class DummyProcess:
        def poll(self):
            return 1

    monkeypatch.setattr("opening_trainer.updater.uuid.uuid4", lambda: DummyUuid())
    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", lambda cmd, **kwargs: DummyProcess())

    result = launch_updater_helper(None, app_state_root=app_state_root, wait_for_pid=1234)
    assert result.helper_bootstrap_proven is False
    assert result.failure_detail == "wrapper_proven_process_exited_without_helper_bootstrap"


def test_launch_updater_helper_serializes_relaunch_args_json(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")

    class DummyProcess:
        def poll(self):
            return 1

    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", lambda cmd, **kwargs: DummyProcess())
    launch_updater_helper(
        None,
        app_state_root=app_state_root,
        wait_for_pid=1234,
        relaunch_args=["--runtime-mode", "consumer"],
    )
    payload = json.loads((updater_root / "launch_helper.audit.json").read_text(encoding="utf-8"))
    assert payload["relaunch_args_json"] == "[\"--runtime-mode\", \"consumer\"]"


def test_launch_updater_helper_windows_launch_contract_and_host_capture(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")

    popen_calls: list[dict] = []

    class DummyProcess:
        def poll(self):
            return 1

    def _fake_popen(cmd, **kwargs):
        popen_calls.append({"cmd": cmd, **kwargs})
        kwargs["stderr"].write(b"host-level launch failure text")
        kwargs["stderr"].flush()
        return DummyProcess()

    monkeypatch.setattr("opening_trainer.updater._is_windows_platform", lambda: True)
    monkeypatch.setattr("opening_trainer.updater.subprocess.CREATE_NO_WINDOW", 0x08000000, raising=False)
    monkeypatch.setattr("opening_trainer.updater.subprocess.Popen", _fake_popen)
    monkeypatch.setattr("opening_trainer.updater._resolve_powershell_executable", lambda: r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe")

    result = launch_updater_helper(None, app_state_root=app_state_root, wait_for_pid=1234)

    assert result.helper_bootstrap_proven is False
    assert result.failure_detail == "bootstrap_host_failed_before_script_entry"
    assert result.proof_artifact == str(updater_root / "apply_update.bootstrap.host.stderr.log")
    assert len(popen_calls) == 1
    assert popen_calls[0]["cmd"][0].endswith("powershell.exe")
    assert "-NonInteractive" in popen_calls[0]["cmd"]
    assert popen_calls[0]["stdin"] is not None
    assert popen_calls[0]["creationflags"] == 0x08000000
    assert popen_calls[0]["stdout"].name.endswith("apply_update.bootstrap.host.stdout.log")
    assert popen_calls[0]["stderr"].name.endswith("apply_update.bootstrap.host.stderr.log")
    launch_audit = json.loads((updater_root / "launch_helper.audit.json").read_text(encoding="utf-8"))
    assert launch_audit["popen_kwargs_summary"]["stdin"] == "DEVNULL"
    assert launch_audit["popen_kwargs_summary"]["creationflags"] == 0x08000000
    assert launch_audit["expected_artifacts"]["bootstrap_host_stderr_log"].endswith("apply_update.bootstrap.host.stderr.log")


def test_check_for_update_raises_when_manifest_missing_and_not_recoverable(tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "channel": "dev",
                "app_version": "2.0.0",
                "build_id": "build-2",
                "payload_filename": "OpeningTrainer-app.zip",
                "payload_url": "https://example.invalid/dev/OpeningTrainer-app.zip",
                "payload_sha256": "abc123",
                "published_at_utc": "2026-03-31T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(UpdaterInstallStateError):
        check_for_update(str(manifest_path), app_state_root=app_state_root)


def test_log_install_runtime_diagnostics_reports_payload_identity(monkeypatch, tmp_path):
    app_state_root = tmp_path / "Local" / "OpeningTrainer"
    mutable_root = app_state_root / "App"
    write_installed_app_manifest(
        app_state_root=app_state_root,
        app_version="1.0.0",
        channel="dev",
        mutable_app_root=mutable_root,
        payload_filename="OpeningTrainer-app.zip",
        payload_sha256="hash",
        bootstrap_version="1.0.0",
        build_id="commit-old",
    )
    updater_root = app_state_root / "updater"
    updater_root.mkdir(parents=True, exist_ok=True)
    (updater_root / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (updater_root / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")
    (updater_root / "updater_config.json").write_text(json.dumps({"channel": "dev"}), encoding="utf-8")
    (mutable_root / "updater").mkdir(parents=True, exist_ok=True)
    (mutable_root / "updater" / "apply_app_update.ps1").write_text("Write-Host helper", encoding="utf-8")
    (mutable_root / "updater" / "invoke_apply_app_update.ps1").write_text("Write-Host wrapper", encoding="utf-8")
    (mutable_root / "payload_identity.json").write_text(
        json.dumps({"marker_schema_version": 1, "app_version": "1.0.0", "build_id": "commit-old", "channel": "dev"}),
        encoding="utf-8",
    )
    messages: list[str] = []
    monkeypatch.setattr("opening_trainer.updater.log_line", lambda message, tag="startup": messages.append(message))

    log_install_runtime_diagnostics(app_state_root=app_state_root, phase="startup")

    assert any("INSTALL_RUNTIME_DIAGNOSTICS" in message for message in messages)
    assert any(INSTALL_DIAGNOSTIC_MARKER in message for message in messages)
    assert any("payload_identity=marker_schema_version=1" in message for message in messages)


def test_apply_helper_logs_cwd_relocation_and_swap_retries():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert "HELPER_CWD_BEFORE_RELOCATE" in script
    assert "HELPER_CWD_AFTER_RELOCATE" in script
    assert "SWAP_TARGETS mutable_root=" in script
    assert "SWAP_MOVE_ATTEMPT attempt=" in script
    assert "SWAP_MOVE_ATTEMPT_FAILED attempt=" in script


def test_apply_helper_rejects_manifest_and_staged_identity_drift_before_swap():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert "Assert-StagedPayloadIdentityMatchesManifest" in script
    assert "manifest.latest.json" in script
    assert "STAGED_PAYLOAD_IDENTITY_MISMATCH" in script
    assert "before swap" in script


def test_apply_helper_uses_detached_relaunch_trampoline_with_restart_contract():
    script_path = Path("installer/scripts/apply_app_update.ps1")
    script = script_path.read_text(encoding="utf-8")

    assert 'relaunch_trampoline_' in script
    assert "$delaySeconds = 5" in script
    assert "PYINSTALLER_RESET_ENVIRONMENT" in script
    assert "[System.Environment]::SetEnvironmentVariable('PYINSTALLER_RESET_ENVIRONMENT', '1', 'Process')" in script
    assert "SetErrorMode" in script
    assert "SEM_FAILCRITICALERRORS" in script
    assert "SEM_NOGPFAULTERRORBOX" in script
    assert "SEM_NOOPENFILEERRORBOX" in script
    assert "`$errorModeFlags = [uint32](`$semFailCriticalErrors -bor `$semNoGpFaultErrorBox -bor `$semNoOpenFileErrorBox)" in script
    assert "Start-Process -FilePath ([string]`$payload.exe) -ArgumentList `$argsArray -WindowStyle Hidden" in script
    assert "Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden" in script
    assert "-NonInteractive" in script
    assert "-File', $relaunchTrampolinePath" in script
    assert "scheduled_detached_trampoline" in script
    assert "restart_env=PYINSTALLER_RESET_ENVIRONMENT:1" in script
    assert "error_mode_suppression=SetErrorMode:SEM_FAILCRITICALERRORS|SEM_NOGPFAULTERRORBOX|SEM_NOOPENFILEERRORBOX" in script
