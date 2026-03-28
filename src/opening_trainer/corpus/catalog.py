from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from ..bundle_contract import (
    manifest_declared_behavioral_profile_set_path,
    manifest_declared_canonical_exact_payload_path,
    manifest_payload_version,
)
from ..runtime import inspect_corpus_bundle

DEFAULT_CORPUS_CATALOG_ROOT = r"F:\Opening Trainer Large Data File\Timing Conditioned Corpus Bundles"


@dataclass(frozen=True)
class BundleCatalogEntry:
    bundle_dir: Path
    manifest_path: Path
    bundle_kind: str
    payload_format: str | None
    payload_version: str | None
    time_control_id: str
    initial_time_seconds: float
    increment_seconds: float
    target_rating_band: str
    rating_policy: str | None
    retained_ply_depth: int | None
    max_supported_player_moves: int | None
    canonical_exact_payload_exists: bool
    timing_overlay_exists: bool
    display_label: str


@dataclass(frozen=True)
class InvalidCatalogEntry:
    manifest_path: Path
    bundle_dir: Path
    reason: str


@dataclass(frozen=True)
class CorpusCatalog:
    root: Path
    entries: tuple[BundleCatalogEntry, ...] = field(default_factory=tuple)
    invalid_entries: tuple[InvalidCatalogEntry, ...] = field(default_factory=tuple)

    def grouped(self) -> dict[str, dict[str, dict[str, tuple[BundleCatalogEntry, ...]]]]:
        grouped: dict[str, dict[str, dict[str, list[BundleCatalogEntry]]]] = {}
        for entry in self.entries:
            category = resolve_time_control_category(entry.time_control_id, entry.initial_time_seconds)
            grouped.setdefault(category, {}).setdefault(entry.time_control_id, {}).setdefault(entry.target_rating_band, []).append(entry)
        return {
            category: {
                tc: {band: tuple(sorted(items, key=_variant_sort_key)) for band, items in bands.items()}
                for tc, bands in sorted(time_controls.items(), key=lambda item: item[0])
            }
            for category, time_controls in sorted(grouped.items(), key=lambda item: item[0])
        }


def discover_corpus_catalog(root: Path | str) -> CorpusCatalog:
    root_path = Path(root).expanduser()
    if not root_path.exists() or not root_path.is_dir():
        return CorpusCatalog(root=root_path.resolve() if root_path.exists() else root_path)

    entries: list[BundleCatalogEntry] = []
    invalid: list[InvalidCatalogEntry] = []
    for manifest_path in sorted(root_path.rglob("manifest.json")):
        bundle_dir = manifest_path.parent
        entry, error = _catalog_entry_from_manifest(bundle_dir=bundle_dir, manifest_path=manifest_path)
        if entry is not None:
            entries.append(entry)
        elif error is not None:
            invalid.append(error)
    return CorpusCatalog(root=root_path.resolve(), entries=tuple(entries), invalid_entries=tuple(invalid))


def resolve_time_control_category(time_control_id: str, initial_time_seconds: float | int | None = None) -> str:
    seconds = _safe_float(initial_time_seconds)
    if seconds is None:
        parsed_seconds, _parsed_increment = _parse_time_control_id(time_control_id)
        seconds = parsed_seconds
    if seconds is None:
        return "Other"
    if seconds <= 120:
        return "Bullet"
    if seconds <= 300:
        return "Blitz"
    return "Rapid"


def bundle_variant_label(entry: BundleCatalogEntry) -> str:
    details: list[str] = []
    if entry.retained_ply_depth is not None:
        details.append(f"ply {entry.retained_ply_depth}")
    if entry.rating_policy:
        details.append(entry.rating_policy)
    if entry.max_supported_player_moves is not None:
        details.append(f"max {entry.max_supported_player_moves} moves")
    if not details:
        return "Default"
    return " | ".join(details)


def _catalog_entry_from_manifest(bundle_dir: Path, manifest_path: Path) -> tuple[BundleCatalogEntry | None, InvalidCatalogEntry | None]:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, InvalidCatalogEntry(manifest_path=manifest_path, bundle_dir=bundle_dir, reason=f"manifest parse failed: {exc}")

    compatibility = inspect_corpus_bundle(bundle_dir)
    if not compatibility.available:
        return None, InvalidCatalogEntry(
            manifest_path=manifest_path,
            bundle_dir=bundle_dir,
            reason=compatibility.failure_reason or compatibility.detail,
        )

    time_control_id = _coerce_time_control_id(manifest)
    initial_time_seconds = _safe_float(manifest.get("initial_time_seconds") or manifest.get("initial_seconds"))
    increment_seconds = _safe_float(manifest.get("increment_seconds"))
    parsed_initial, parsed_increment = _parse_time_control_id(time_control_id)
    if initial_time_seconds is None:
        initial_time_seconds = parsed_initial
    if increment_seconds is None:
        increment_seconds = parsed_increment

    rating_band = _format_rating_band(manifest.get("target_rating_band") or manifest.get("rating_band") or manifest.get("elo_band"))
    if not time_control_id or initial_time_seconds is None or increment_seconds is None or not rating_band:
        return None, InvalidCatalogEntry(
            manifest_path=manifest_path,
            bundle_dir=bundle_dir,
            reason="missing required timing/rating metadata",
        )

    canonical_payload_path = manifest_declared_canonical_exact_payload_path(manifest, bundle_dir)
    timing_overlay_exists = _timing_overlay_exists(manifest=manifest, bundle_dir=bundle_dir)
    max_supported_player_moves = _safe_int(manifest.get("max_supported_player_moves"))
    retained_ply_depth = compatibility.retained_ply_depth
    if retained_ply_depth is None:
        retained_ply_depth = _safe_int(manifest.get("retained_ply_depth"))

    entry = BundleCatalogEntry(
        bundle_dir=compatibility.bundle_dir,
        manifest_path=compatibility.manifest_path,
        bundle_kind=compatibility.bundle_kind or "unknown",
        payload_format=compatibility.payload_format,
        payload_version=manifest_payload_version(manifest),
        time_control_id=time_control_id,
        initial_time_seconds=initial_time_seconds,
        increment_seconds=increment_seconds,
        target_rating_band=rating_band,
        rating_policy=_coerce_text(manifest.get("rating_policy")),
        retained_ply_depth=retained_ply_depth,
        max_supported_player_moves=max_supported_player_moves,
        canonical_exact_payload_exists=bool(canonical_payload_path and canonical_payload_path.exists()),
        timing_overlay_exists=timing_overlay_exists,
        display_label=_build_display_label(time_control_id=time_control_id, rating_band=rating_band, retained_ply_depth=retained_ply_depth),
    )
    return entry, None


def _timing_overlay_exists(manifest: dict[str, object], bundle_dir: Path) -> bool:
    behavioral = manifest_declared_behavioral_profile_set_path(manifest, bundle_dir)
    if behavioral is not None and behavioral.exists():
        return True
    overlay_relative = manifest.get("timing_overlay_file") or manifest.get("timing_overlay_payload_file")
    if isinstance(overlay_relative, str) and overlay_relative.strip():
        return (bundle_dir / Path(overlay_relative)).exists()
    return False


def _build_display_label(*, time_control_id: str, rating_band: str, retained_ply_depth: int | None) -> str:
    retained = f" | PLY {retained_ply_depth}" if retained_ply_depth is not None else ""
    return f"{time_control_id} | {rating_band}{retained}"


def _coerce_time_control_id(manifest: dict[str, object]) -> str | None:
    value = manifest.get("time_control_id") or manifest.get("time_format_label")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _format_rating_band(value: object) -> str | None:
    if isinstance(value, dict):
        minimum = value.get("minimum")
        maximum = value.get("maximum")
        if minimum is not None and maximum is not None:
            return f"{minimum}-{maximum}"
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _safe_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_time_control_id(time_control_id: str | None) -> tuple[float | None, float | None]:
    if not time_control_id or "+" not in time_control_id:
        return None, None
    initial, increment = time_control_id.split("+", 1)
    return _safe_float(initial), _safe_float(increment)


def _coerce_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _variant_sort_key(entry: BundleCatalogEntry) -> tuple[int, str, str]:
    retained = entry.retained_ply_depth if entry.retained_ply_depth is not None else -1
    return retained, entry.rating_policy or "", str(entry.bundle_dir)
