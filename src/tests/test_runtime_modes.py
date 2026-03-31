from __future__ import annotations

import json
from pathlib import Path

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
