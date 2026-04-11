from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from opening_trainer.main import _apply_runtime_environment
from opening_trainer.runtime import RuntimeOverrides, load_runtime_config


def _write_opening_locked_artifact(root: Path) -> Path:
    artifact_root = root / "opening_locked_mode"
    artifact_root.mkdir(parents=True, exist_ok=True)
    (artifact_root / "manifest.json").write_text(json.dumps({"opening_count": 1}), encoding="utf-8")
    sqlite_path = artifact_root / "opening_locked_openings.sqlite"
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("CREATE TABLE opening_membership(position_key TEXT, opening_name TEXT, is_exact INTEGER)")
        conn.execute("CREATE TABLE canonical_continuation(opening_name TEXT, position_key TEXT, move_uci TEXT, ply_index INTEGER)")
    return artifact_root


def test_dev_mode_default_and_profile_root_stays_repo_runtime(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.runtime_mode.value == "dev"
    assert runtime.runtime_paths.app_state_root == tmp_path / "runtime"
    assert runtime.runtime_paths.profile_root == tmp_path / "runtime" / "profiles"
    assert runtime.runtime_paths.app_payload_root == tmp_path / "runtime" / "app_payload"


def test_consumer_mode_uses_local_app_data_roots(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="consumer"))

    assert runtime.runtime_mode.value == "consumer"
    assert runtime.runtime_paths.app_state_root == local_app_data / "OpeningTrainer"
    assert runtime.runtime_paths.content_root == local_app_data / "OpeningTrainerContent"
    assert runtime.runtime_paths.log_root == local_app_data / "OpeningTrainer" / "logs"


def test_consumer_asset_paths_are_content_root_relative(monkeypatch, tmp_path):
    content_root = tmp_path / "Lad" / "OpeningTrainerContent"
    (content_root / "stockfish").mkdir(parents=True)
    (content_root / "opening_book.bin").write_bytes(b"book")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Lad"))

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="consumer"))

    assert Path(str(runtime.config.predecessor_master_db_path)) == content_root / "canonical_predecessor_master.sqlite"
    assert runtime.book.path == (content_root / "opening_book.bin").resolve()
    assert runtime.engine.path == (content_root / "stockfish").resolve()


def test_dev_opening_locked_artifact_discovery_uses_runtime_config_override(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    external_assets = tmp_path / "external_assets"
    artifact_root = _write_opening_locked_artifact(external_assets)
    (tmp_path / "runtime.local.json").write_text(
        json.dumps({"opening_locked_artifact_root": str(artifact_root)}),
        encoding="utf-8",
    )
    monkeypatch.chdir(repo_root)

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))

    assert runtime.opening_locked_artifact.loaded is True
    assert runtime.opening_locked_artifact.sqlite_path == artifact_root / "opening_locked_openings.sqlite"
    assert "workspace-runtime-config" in runtime.opening_locked_artifact.detail


def test_dev_opening_locked_artifact_discovery_uses_environment_override(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    external_assets = tmp_path / "external_assets_env"
    artifact_root = _write_opening_locked_artifact(external_assets)
    monkeypatch.chdir(repo_root)
    monkeypatch.setenv("OPENING_TRAINER_OPENING_LOCKED_ARTIFACT_ROOT", str(artifact_root))

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))

    assert runtime.opening_locked_artifact.loaded is True
    assert runtime.opening_locked_artifact.sqlite_path == artifact_root / "opening_locked_openings.sqlite"
    assert "source=environment" in runtime.opening_locked_artifact.detail


def test_consumer_missing_content_fails_clearly(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "Lad"))

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="consumer"))

    assert runtime.book.available is False
    assert runtime.engine.available is False
    assert "consumer content-root path" in runtime.book.detail
    assert "consumer stockfish root" in runtime.engine.detail


def test_consumer_runtime_loader_accepts_bom_prefixed_runtime_config(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    app_state_root = local_app_data / "OpeningTrainer"
    content_root = local_app_data / "OpeningTrainerContent"
    stockfish_dir = content_root / "stockfish"
    stockfish_dir.mkdir(parents=True)
    (stockfish_dir / "stockfish-windows-x86-64.exe").write_bytes(b"engine")
    (content_root / "opening_book.bin").write_bytes(b"book")

    runtime_payload = {
        "engine_executable_path": str(stockfish_dir / "stockfish-windows-x86-64.exe"),
        "opening_book_path": str(content_root / "opening_book.bin"),
    }
    app_state_root.mkdir(parents=True)
    (app_state_root / "runtime.consumer.json").write_text(
        "\ufeff" + json.dumps(runtime_payload),
        encoding="utf-8",
    )

    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="consumer"))

    assert runtime.runtime_mode.value == "consumer"
    assert runtime.config.engine_executable_path == str(stockfish_dir / "stockfish-windows-x86-64.exe")


def test_runtime_mode_resolution_precedence_cli_wins_over_env_and_artifacts(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    app_state_root = local_app_data / "OpeningTrainer"
    app_state_root.mkdir(parents=True)
    (app_state_root / "runtime.consumer.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setenv("OPENING_TRAINER_RUNTIME_MODE", "consumer")
    monkeypatch.setenv("OPENING_TRAINER_ASSUME_INSTALLED", "1")

    runtime = load_runtime_config(RuntimeOverrides(runtime_mode="dev"))

    assert runtime.runtime_mode.value == "dev"
    assert runtime.runtime_mode_source == "cli"


def test_runtime_mode_resolution_precedence_env_wins_over_artifacts(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    app_state_root = local_app_data / "OpeningTrainer"
    app_state_root.mkdir(parents=True)
    (app_state_root / "runtime.consumer.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setenv("OPENING_TRAINER_RUNTIME_MODE", "dev")
    monkeypatch.setenv("OPENING_TRAINER_ASSUME_INSTALLED", "1")

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.runtime_mode.value == "dev"
    assert runtime.runtime_mode_source == "environment"


def test_installed_consumer_artifacts_infer_consumer_mode_and_local_logs(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    app_state_root = local_app_data / "OpeningTrainer"
    content_root = local_app_data / "OpeningTrainerContent"
    app_state_root.mkdir(parents=True)
    content_root.mkdir(parents=True)
    (app_state_root / "runtime.consumer.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setenv("OPENING_TRAINER_ASSUME_INSTALLED", "1")
    monkeypatch.delenv("OPENING_TRAINER_RUNTIME_MODE", raising=False)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.runtime_mode.value == "consumer"
    assert runtime.runtime_mode_source == "auto-consumer"
    assert runtime.runtime_paths.log_root == local_app_data / "OpeningTrainer" / "logs"


def test_no_artifacts_defaults_to_dev_even_when_installed_assumption_is_set(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.setenv("OPENING_TRAINER_ASSUME_INSTALLED", "1")
    monkeypatch.delenv("OPENING_TRAINER_RUNTIME_MODE", raising=False)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.runtime_mode.value == "consumer"
    assert runtime.runtime_mode_source == "auto-consumer"


def test_installed_local_app_data_executable_path_infers_consumer_mode(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    exe_path = local_app_data / "OpeningTrainer" / "App" / "OpeningTrainer.exe"
    exe_path.parent.mkdir(parents=True)
    exe_path.write_bytes(b"")
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.delenv("OPENING_TRAINER_ASSUME_INSTALLED", raising=False)
    monkeypatch.delenv("OPENING_TRAINER_RUNTIME_MODE", raising=False)
    monkeypatch.setattr("opening_trainer.runtime_mode.sys.executable", str(exe_path))
    monkeypatch.setattr("opening_trainer.runtime_mode.sys.frozen", False, raising=False)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.runtime_mode.value == "consumer"
    assert runtime.runtime_mode_source == "auto-consumer"
    assert runtime.runtime_paths.app_state_root == local_app_data / "OpeningTrainer"
    assert runtime.runtime_paths.app_payload_root == local_app_data / "OpeningTrainer" / "App"


def test_apply_runtime_environment_binds_session_log_dir_from_runtime_paths(monkeypatch, tmp_path):
    bound_paths: list[Path] = []
    runtime_context = type(
        "RuntimeContext",
        (),
        {"runtime_paths": type("RuntimePaths", (), {"log_root": tmp_path / "app" / "logs"})()},
    )()
    monkeypatch.setattr("opening_trainer.main.initialize_session_logging", lambda p: bound_paths.append(p))

    _apply_runtime_environment(runtime_context)

    assert bound_paths == [tmp_path / "app" / "logs" / "sessions"]
    assert Path(os.environ["OPENING_TRAINER_INSTANCE_DIAGNOSTICS_PATH"]) == (
        tmp_path / "app" / "logs" / "instance" / "opening_trainer_instance.json"
    )


def test_installed_content_manifest_contract_is_shared_between_installer_and_runtime() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    content_bootstrap_script = (repo_root / "installer" / "scripts" / "install_consumer_content.ps1").read_text(encoding="utf-8")
    runtime_mode_source = (repo_root / "src" / "opening_trainer" / "runtime_mode.py").read_text(encoding="utf-8")
    installer_manifest = json.loads((repo_root / "installer" / "consumer_content_manifest.json").read_text(encoding="utf-8"))

    assert installer_manifest["installed_manifest_filename"] == "installed_content_manifest.json"
    assert "Manifest installed_manifest_filename must be '" in content_bootstrap_script
    assert "runtime/install contract compatibility" in content_bootstrap_script
    assert 'installed_manifest_exists = (state_root / "installed_content_manifest.json").exists()' in runtime_mode_source
