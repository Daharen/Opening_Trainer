from __future__ import annotations

import json
import os
from pathlib import Path

from opening_trainer.main import _apply_runtime_environment
from opening_trainer.runtime import RuntimeOverrides, load_runtime_config


def test_dev_mode_default_and_profile_root_stays_repo_runtime(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    runtime = load_runtime_config(RuntimeOverrides())

    assert runtime.runtime_mode.value == "dev"
    assert runtime.runtime_paths.app_state_root == tmp_path / "runtime"
    assert runtime.runtime_paths.profile_root == tmp_path / "runtime" / "profiles"


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

    assert runtime.runtime_mode.value == "dev"
    assert runtime.runtime_mode_source == "default"


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
