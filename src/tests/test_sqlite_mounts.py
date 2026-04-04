from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from opening_trainer.bundle_contract import resolve_bundle_payload
from opening_trainer.bundle_corpus import BuilderAggregateCorpusProvider
from opening_trainer.sqlite_mounts import MountedSQLiteManager
from opening_trainer.zstd_compat import compress as zstd_compress


def _write_sqlite_payload(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE positions (position_id INTEGER PRIMARY KEY, position_key TEXT NOT NULL, position_key_format TEXT NOT NULL, side_to_move TEXT NOT NULL, candidate_move_count INTEGER NOT NULL, total_observations INTEGER NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE moves (move_id INTEGER PRIMARY KEY, position_id INTEGER NOT NULL, move_key TEXT NOT NULL, move_key_format TEXT NOT NULL, raw_count INTEGER NOT NULL, example_san TEXT)"
        )
        connection.execute(
            "INSERT INTO positions(position_id, position_key, position_key_format, side_to_move, candidate_move_count, total_observations) VALUES (1, 'start', 'fen_normalized', 'white', 1, 10)"
        )
        connection.execute(
            "INSERT INTO moves(position_id, move_key, move_key_format, raw_count, example_san) VALUES (1, 'e2e4', 'uci', 10, 'e4')"
        )
        connection.commit()
    finally:
        connection.close()


def _compress_to_zst(sqlite_path: Path) -> Path:
    compressed_path = sqlite_path.with_name(sqlite_path.name + ".zst")
    compressed_path.write_bytes(zstd_compress(sqlite_path.read_bytes()))
    return compressed_path


def test_mounted_sqlite_manager_prefers_plain_when_plain_and_zst_exist(tmp_path):
    manager = MountedSQLiteManager()
    manager._root = tmp_path / "mount_root"
    manager._owner_dir = manager._root / "pid-12345"

    plain = tmp_path / "payload.sqlite"
    _write_sqlite_payload(plain)
    _compress_to_zst(plain)

    resolution, lease = manager.resolve(plain)

    assert resolution.used_plain_sqlite is True
    assert resolution.used_compressed_sqlite is False
    assert resolution.active_path == plain
    assert lease is None


def test_mounted_sqlite_manager_mounts_zst_and_releases_on_lease_release(tmp_path):
    manager = MountedSQLiteManager()
    manager._root = tmp_path / "mount_root"
    manager._owner_dir = manager._root / "pid-12345"

    plain = tmp_path / "payload.sqlite"
    _write_sqlite_payload(plain)
    compressed = _compress_to_zst(plain)
    plain.unlink()

    resolution, lease = manager.resolve(plain)

    assert resolution.used_plain_sqlite is False
    assert resolution.used_compressed_sqlite is True
    assert resolution.active_path.exists()
    assert lease is not None

    mounted_path = resolution.active_path
    lease.release()
    assert not mounted_path.exists()
    assert compressed.exists()


def test_resolve_bundle_payload_supports_zst_only_sqlite_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    manifest = {
        "build_status": "aggregation_complete",
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "payload_format": "sqlite",
    }
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True)
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    plain_sqlite = data_dir / "corpus.sqlite"
    _write_sqlite_payload(plain_sqlite)
    _compress_to_zst(plain_sqlite)
    plain_sqlite.unlink()

    resolution, error = resolve_bundle_payload(manifest, bundle_dir)

    assert error is None
    assert resolution is not None
    assert resolution.used_compressed_sqlite is True
    assert resolution.payload_path.exists()
    assert resolution.payload_path.name.endswith(".sqlite")
    if resolution.mounted_sqlite_lease is not None:
        resolution.mounted_sqlite_lease.release()


def test_builder_provider_releases_mounted_sqlite_on_close(tmp_path):
    bundle_dir = tmp_path / "bundle"
    manifest = {
        "build_status": "aggregation_complete",
        "position_key_format": "fen_normalized",
        "move_key_format": "uci",
        "payload_format": "sqlite",
        "sqlite_corpus_file": "data/corpus.sqlite",
    }
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True)
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    plain_sqlite = data_dir / "corpus.sqlite"
    _write_sqlite_payload(plain_sqlite)
    _compress_to_zst(plain_sqlite)
    plain_sqlite.unlink()

    provider = BuilderAggregateCorpusProvider(bundle_dir)
    mounted_path = provider.metadata.aggregate_path
    assert mounted_path.exists()

    provider.close()
    assert not mounted_path.exists()


def test_mounted_sqlite_manager_cleans_stale_abandoned_mount_dirs(tmp_path):
    manager = MountedSQLiteManager()
    manager._root = tmp_path / "mount_root"
    manager._owner_dir = manager._root / "pid-12345"
    manager._current_pid = 12345
    stale = manager._root / "pid-999999"
    stale.mkdir(parents=True)
    (stale / "stale.sqlite").write_bytes(b"junk")

    plain = tmp_path / "payload.sqlite"
    _write_sqlite_payload(plain)
    _compress_to_zst(plain)
    plain.unlink()

    resolution, lease = manager.resolve(plain)
    assert resolution.active_path.exists()
    assert not stale.exists()
    if lease is not None:
        lease.release()
