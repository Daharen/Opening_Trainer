from __future__ import annotations

import json
from pathlib import Path

from opening_trainer.corpus.catalog import (
    DEFAULT_CORPUS_CATALOG_ROOT,
    discover_corpus_catalog,
    resolve_time_control_category,
    sort_key_rating_band,
)
from opening_trainer.settings import TrainerSettings
from opening_trainer.ui.gui_app import OpeningTrainerGUI
from opening_trainer.zstd_compat import compress as zstd_compress


def _write_timing_bundle(
    root: Path,
    name: str,
    *,
    time_control_id: str,
    initial: int,
    increment: int,
    minimum: int,
    maximum: int,
    retained: int,
    rating_policy: str = "both_players_in_band",
    zst_only_exact_payload: bool = False,
) -> Path:
    bundle_dir = root / name
    data_dir = bundle_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    exact_payload = data_dir / "exact_corpus.sqlite"
    if zst_only_exact_payload:
        import sqlite3

        connection = sqlite3.connect(exact_payload)
        connection.execute(
            "CREATE TABLE positions (position_key TEXT PRIMARY KEY, total_observed_count INTEGER, candidate_moves_json TEXT)"
        )
        connection.commit()
        connection.close()
    else:
        exact_payload.write_bytes(b"sqlite")
    if zst_only_exact_payload:
        (data_dir / "exact_corpus.sqlite.zst").write_bytes(zstd_compress(exact_payload.read_bytes()))
        exact_payload.unlink()
    (data_dir / "behavioral_profile_set.sqlite").write_bytes(b"overlay")
    manifest = {
        "build_status": "finalized",
        "time_control_id": time_control_id,
        "initial_time_seconds": initial,
        "increment_seconds": increment,
        "target_rating_band": {"minimum": minimum, "maximum": maximum},
        "rating_policy": rating_policy,
        "retained_ply_depth": retained,
        "payload_version": "v1",
        "canonical_exact_payload_file": "data/exact_corpus.sqlite",
        "behavioral_profile_set_file": "data/behavioral_profile_set.sqlite",
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return bundle_dir


def test_catalog_discovers_and_groups_only_present_valid_bundles(tmp_path):
    _write_timing_bundle(
        tmp_path,
        "full_400_600_both_in_band_ply30_tc600p0_timing_conditioned",
        time_control_id="600+0",
        initial=600,
        increment=0,
        minimum=400,
        maximum=600,
        retained=30,
    )
    _write_timing_bundle(
        tmp_path,
        "full_1000_1200_both_in_band_ply30_tc600p0_timing_conditioned",
        time_control_id="600+0",
        initial=600,
        increment=0,
        minimum=1000,
        maximum=1200,
        retained=30,
    )
    _write_timing_bundle(
        tmp_path,
        "full_400_600_both_in_band_ply30_tc120p2_timing_conditioned",
        time_control_id="120+2",
        initial=120,
        increment=2,
        minimum=400,
        maximum=600,
        retained=30,
    )

    catalog = discover_corpus_catalog(tmp_path)
    grouped = catalog.grouped()

    assert len(catalog.entries) == 3
    assert "Rapid" in grouped
    assert "600+0" in grouped["Rapid"]
    assert set(grouped["Rapid"]["600+0"].keys()) == {"400-600", "1000-1200"}
    assert "Bullet" in grouped
    assert "120+2" in grouped["Bullet"]
    assert "300+0" not in grouped.get("Blitz", {})


def test_catalog_invalid_bundle_is_isolated_without_breaking_catalog(tmp_path):
    _write_timing_bundle(
        tmp_path,
        "valid",
        time_control_id="300+0",
        initial=300,
        increment=0,
        minimum=1200,
        maximum=1400,
        retained=20,
    )
    broken = tmp_path / "broken"
    broken.mkdir(parents=True, exist_ok=True)
    (broken / "manifest.json").write_text("{", encoding="utf-8")

    catalog = discover_corpus_catalog(tmp_path)

    assert len(catalog.entries) == 1
    assert len(catalog.invalid_entries) == 1
    assert catalog.invalid_entries[0].bundle_dir == broken


def test_catalog_marks_canonical_exact_payload_present_for_zst_only_bundle(tmp_path):
    _write_timing_bundle(
        tmp_path,
        "zst_only_exact",
        time_control_id="600+0",
        initial=600,
        increment=0,
        minimum=1000,
        maximum=1200,
        retained=30,
        zst_only_exact_payload=True,
    )

    catalog = discover_corpus_catalog(tmp_path)

    assert len(catalog.entries) == 1
    assert catalog.entries[0].canonical_exact_payload_exists is True


def test_catalog_discovery_does_not_mount_sqlite_payloads_for_availability(monkeypatch, tmp_path):
    _write_timing_bundle(
        tmp_path,
        "zst_only_exact",
        time_control_id="600+0",
        initial=600,
        increment=0,
        minimum=1000,
        maximum=1200,
        retained=30,
        zst_only_exact_payload=True,
    )

    def _unexpected_mount(*_args, **_kwargs):
        raise AssertionError("catalog discovery should not mount sqlite payloads")

    monkeypatch.setattr("opening_trainer.bundle_contract.get_mounted_sqlite_manager", _unexpected_mount)

    catalog = discover_corpus_catalog(tmp_path)

    assert len(catalog.entries) == 1
    assert catalog.entries[0].canonical_exact_payload_exists is True


def test_structured_and_legacy_paths_converge_to_same_authoritative_bundle_state(tmp_path):
    bundle_dir = _write_timing_bundle(
        tmp_path,
        "selected_bundle",
        time_control_id="600+0",
        initial=600,
        increment=0,
        minimum=400,
        maximum=600,
        retained=30,
    )

    class SessionStub:
        def __init__(self):
            self.settings = TrainerSettings()
            self._saved = self.settings
            self.settings_store = self

        def max_supported_training_depth(self):
            return 5

        def update_settings(self, settings):
            self.settings = settings
            self._saved = settings
            return settings

        def load(self, maximum_depth=None):
            del maximum_depth
            return self._saved

    gui = OpeningTrainerGUI.__new__(OpeningTrainerGUI)
    gui.panel_visible = False
    gui.move_list_visible = True
    gui.session = SessionStub()

    gui._set_last_bundle_path(str(bundle_dir))
    loaded = gui.session.settings_store.load(maximum_depth=5)

    assert loaded.last_bundle_path == str(bundle_dir)
    assert loaded.last_corpus_catalog_root == DEFAULT_CORPUS_CATALOG_ROOT


def test_time_control_category_helper_is_stable_for_lane_one_examples():
    assert resolve_time_control_category("600+0", 600) == "Rapid"
    assert resolve_time_control_category("300+0", 300) == "Blitz"
    assert resolve_time_control_category("120+2", 120) == "Bullet"


def test_rating_band_sorting_is_numeric_and_human_sensible():
    unsorted = ["1800-2000", "3000-3999", "1000-1200", "600-800", "400-600", "800-1000"]

    sorted_bands = sorted(unsorted, key=sort_key_rating_band)

    assert sorted_bands[:4] == ["400-600", "600-800", "800-1000", "1000-1200"]
    assert sorted_bands[-2:] == ["1800-2000", "3000-3999"]


def test_rating_band_sorting_handles_nonstandard_values_after_numeric_ranges():
    unsorted = ["pro", "2200-2400", "n/a", "400-600", "open"]

    sorted_bands = sorted(unsorted, key=sort_key_rating_band)

    assert sorted_bands[:2] == ["400-600", "2200-2400"]
    assert sorted_bands[2:] == ["n/a", "open", "pro"]
