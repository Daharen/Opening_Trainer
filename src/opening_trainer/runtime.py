from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
import re
from pathlib import Path
from typing import Any

from .runtime_mode import ENV_RUNTIME_MODE, RuntimeMode, resolve_runtime_mode_with_source
from .runtime_paths import RuntimePaths, resolve_runtime_paths
from .bundle_contract import (
    BUNDLE_AGGREGATE_RELATIVE_PATH,
    BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH,
    BUNDLE_MANIFEST_NAME,
    BUNDLE_SQLITE_RELATIVE_PATH,
    SUPPORTED_BUNDLE_MOVE_KEY_FORMAT,
    SUPPORTED_BUNDLE_POSITION_KEY_FORMAT,
    classify_bundle_contract,
    manifest_declared_aggregate_path,
    manifest_declared_canonical_exact_payload_path,
    manifest_declared_exact_sqlite_path,
    manifest_declared_compatibility_exact_payload_path,
    manifest_declared_sqlite_path,
    manifest_payload_version,
    payload_status_mentions_counts,
    sqlite_payload_path_exists,
    aggregate_payload_exposes_raw_counts,
)
from .corpus import DEFAULT_ARTIFACT_PATH, load_artifact
from .evaluation import EvaluatorConfig

DEFAULT_RUNTIME_CONFIG_PATH = Path("runtime/runtime_config.json")
WORKSPACE_RUNTIME_CONFIG_PATH = Path("runtime.local.json")
WORKSPACE_BOOK_PATHS = (
    Path("runtime/opening_book.bin"),
    Path("assets/opening_book.bin"),
    Path("data/opening_book.bin"),
)
DEFAULT_BOOK_PATHS = (
    Path("runtime/opening_book.bin"),
    Path("assets/opening_book.bin"),
    Path("data/opening_book.bin"),
)
WORKSPACE_ENGINE_PATHS = (
    Path("tools/stockfish/stockfish-windows-x86-64-avx2.exe"),
    Path("tools/stockfish/stockfish-windows-x86-64.exe"),
    Path("tools/stockfish/stockfish.exe"),
)
DEFAULT_ENGINE_PATHS = (
    Path("runtime/engine/stockfish"),
    Path("runtime/stockfish"),
    Path("assets/stockfish"),
)
WORKSPACE_CORPUS_PATHS = (
    Path("data/opening_corpus.json"),
    Path("artifacts/opening_corpus.json"),
)
ENV_RUNTIME_CONFIG = "OPENING_TRAINER_RUNTIME_CONFIG"
ENV_CORPUS_PATH = "OPENING_TRAINER_CORPUS_PATH"
ENV_CORPUS_BUNDLE_DIR = "OPENING_TRAINER_CORPUS_BUNDLE_DIR"
ENV_ENGINE_PATH = "OPENING_TRAINER_ENGINE_PATH"
ENV_BOOK_PATH = "OPENING_TRAINER_BOOK_PATH"
ENV_STRICT_ASSETS = "OPENING_TRAINER_STRICT_ASSETS"
ENV_ENGINE_DEPTH = "OPENING_TRAINER_ENGINE_DEPTH"
ENV_ENGINE_TIME_LIMIT = "OPENING_TRAINER_ENGINE_TIME_LIMIT"
ENV_OPPONENT_FALLBACK_MODE = "OPENING_TRAINER_OPPONENT_FALLBACK_MODE"
ENV_PREDECESSOR_MASTER_DB_PATH = "OPENING_TRAINER_PREDECESSOR_MASTER_DB_PATH"
ENV_PRACTICAL_RISK_RECONCILED_PATH = "OPENING_TRAINER_PRACTICAL_RISK_RECONCILED_PATH"


@dataclass(frozen=True)
class ResolvedAssetPath:
    label: str
    path: str | Path | None
    source: str
    available: bool
    detail: str


@dataclass(frozen=True)
class RuntimeConfig:
    corpus_bundle_dir: str | None = None
    corpus_artifact_path: str | None = None
    engine_executable_path: str | None = None
    opening_book_path: str | None = None
    engine_depth: int | None = None
    engine_time_limit_seconds: float | None = None
    predecessor_master_db_path: str | None = None
    strict_assets: bool = False
    opponent_fallback_mode: str = "current_bundle_only"
    practical_risk_reconciled_path: str | None = None

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "RuntimeConfig":
        return cls(
            corpus_bundle_dir=payload.get("corpus_bundle_dir"),
            corpus_artifact_path=payload.get("corpus_artifact_path"),
            engine_executable_path=payload.get("engine_executable_path"),
            opening_book_path=payload.get("opening_book_path"),
            engine_depth=payload.get("engine_depth"),
            engine_time_limit_seconds=payload.get("engine_time_limit_seconds"),
            predecessor_master_db_path=payload.get("predecessor_master_db_path"),
            strict_assets=bool(payload.get("strict_assets", False)),
            opponent_fallback_mode=str(payload.get("opponent_fallback_mode", "current_bundle_only")),
            practical_risk_reconciled_path=payload.get("practical_risk_reconciled_path"),
        )


@dataclass(frozen=True)
class RuntimeOverrides:
    runtime_mode: str | None = None
    corpus_bundle_dir: str | None = None
    corpus_artifact_path: str | None = None
    engine_executable_path: str | None = None
    opening_book_path: str | None = None
    runtime_config_path: str | None = None
    engine_depth: int | None = None
    engine_time_limit_seconds: float | None = None
    predecessor_master_db_path: str | None = None
    strict_assets: bool | None = None
    opponent_fallback_mode: str | None = None
    practical_risk_reconciled_path: str | None = None


@dataclass(frozen=True)
class RuntimeStartupStatus:
    mode: str
    user_color: str
    corpus_status: str
    book_status: str
    engine_status: str
    doctrine_status: str
    lines: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RuntimeContext:
    runtime_mode: RuntimeMode
    runtime_mode_source: str
    runtime_mode_reason: str
    runtime_paths: RuntimePaths
    config: RuntimeConfig
    evaluator_config: EvaluatorConfig
    corpus: ResolvedAssetPath
    book: ResolvedAssetPath
    engine: ResolvedAssetPath
    config_source: str

    def startup_status(self, mode: str, user_color: str) -> RuntimeStartupStatus:
        fully_capable = self.corpus.available and self.engine.available
        doctrine_status = (
            "Doctrine-capable: corpus-backed opponent source + engine tolerance active."
            if fully_capable
            else "Degraded mode: one or more runtime authorities are unavailable; status lines below explain why."
        )
        opponent_order = (
            "corpus aggregate bundle or legacy corpus artifact -> Stockfish fallback -> random legal last-ditch fallback"
        )
        lines = (
            f"Mode: {mode}",
            f"User color: {user_color}",
            f"Corpus: {self.corpus.detail}",
            f"Book: {self.book.detail}",
            f"Engine: {self.engine.detail}",
            f"Opponent source order: {opponent_order}",
            f"Opponent human fallback mode: {self.config.opponent_fallback_mode}",
            f"Stockfish fallback available: {'yes' if self.engine.available else 'no'}",
            "Random fallback remains enabled only as the last-ditch opponent source.",
            doctrine_status,
        )
        return RuntimeStartupStatus(
            mode=mode,
            user_color=user_color,
            corpus_status=self.corpus.detail,
            book_status=self.book.detail,
            engine_status=self.engine.detail,
            doctrine_status=doctrine_status,
            lines=lines,
        )


def load_runtime_config(overrides: RuntimeOverrides | None = None) -> RuntimeContext:
    overrides = overrides or RuntimeOverrides()
    repo_root = _repo_root()
    workspace_root = _workspace_root()
    local_app_data_root = _default_local_app_data_root()
    runtime_mode_resolution = resolve_runtime_mode_with_source(
        overrides.runtime_mode,
        app_state_root=local_app_data_root / "OpeningTrainer",
        content_root=local_app_data_root / "OpeningTrainerContent",
    )
    runtime_mode = runtime_mode_resolution.mode
    resolved_runtime_paths = resolve_runtime_paths(runtime_mode, repo_root=repo_root, workspace_root=workspace_root)
    config_resolution = _resolve_config_file_path(
        override_path=overrides.runtime_config_path,
        runtime_mode=runtime_mode,
        runtime_paths=resolved_runtime_paths.paths,
    )
    file_config = RuntimeConfig()
    if config_resolution.path is not None and config_resolution.path.exists():
        file_config = RuntimeConfig.from_mapping(_load_json_file(config_resolution.path))

    config_prefers_file_assets = config_resolution.kind == "explicit"
    strict_assets = _resolve_bool(
        overrides.strict_assets,
        file_config.strict_assets,
        os.getenv(ENV_STRICT_ASSETS),
        prefer_file_value=config_prefers_file_assets,
    )
    config = RuntimeConfig(
        corpus_bundle_dir=_pick_asset_value(
            override_value=overrides.corpus_bundle_dir,
            file_value=file_config.corpus_bundle_dir,
            env_value=os.getenv(ENV_CORPUS_BUNDLE_DIR),
            prefer_file_value=config_prefers_file_assets,
        ),
        corpus_artifact_path=_pick_asset_value(
            override_value=overrides.corpus_artifact_path,
            file_value=file_config.corpus_artifact_path,
            env_value=os.getenv(ENV_CORPUS_PATH),
            prefer_file_value=config_prefers_file_assets,
        ),
        engine_executable_path=_pick_asset_value(
            override_value=overrides.engine_executable_path,
            file_value=file_config.engine_executable_path,
            env_value=os.getenv(ENV_ENGINE_PATH),
            prefer_file_value=config_prefers_file_assets,
        ),
        opening_book_path=_pick_asset_value(
            override_value=overrides.opening_book_path,
            file_value=file_config.opening_book_path,
            env_value=os.getenv(ENV_BOOK_PATH),
            prefer_file_value=config_prefers_file_assets,
        ),
        engine_depth=_coerce_int(
            _pick_asset_value(
                override_value=overrides.engine_depth,
                file_value=file_config.engine_depth,
                env_value=os.getenv(ENV_ENGINE_DEPTH),
                prefer_file_value=config_prefers_file_assets,
            )
        ),
        engine_time_limit_seconds=_coerce_float(
            _pick_asset_value(
                override_value=overrides.engine_time_limit_seconds,
                file_value=file_config.engine_time_limit_seconds,
                env_value=os.getenv(ENV_ENGINE_TIME_LIMIT),
                prefer_file_value=config_prefers_file_assets,
            )
        ),
        predecessor_master_db_path=_pick_asset_value(
            override_value=overrides.predecessor_master_db_path,
            file_value=file_config.predecessor_master_db_path,
            env_value=os.getenv(ENV_PREDECESSOR_MASTER_DB_PATH),
            prefer_file_value=config_prefers_file_assets,
        )
        or str(resolved_runtime_paths.paths.predecessor_master_db_path),
        strict_assets=strict_assets,
        opponent_fallback_mode=str(
            _pick_asset_value(
                override_value=overrides.opponent_fallback_mode,
                file_value=file_config.opponent_fallback_mode,
                env_value=os.getenv(ENV_OPPONENT_FALLBACK_MODE),
                prefer_file_value=config_prefers_file_assets,
            )
            or "current_bundle_only"
        ),
        practical_risk_reconciled_path=_resolve_practical_risk_reconciled_path(
            override_value=overrides.practical_risk_reconciled_path,
            file_value=file_config.practical_risk_reconciled_path,
            env_value=os.getenv(ENV_PRACTICAL_RISK_RECONCILED_PATH),
            runtime_paths=resolved_runtime_paths.paths,
        ),
    )

    evaluator_base = EvaluatorConfig()
    evaluator_config = EvaluatorConfig(
        better_max_cp_loss=evaluator_base.better_max_cp_loss,
        overlay_best_max_cp_loss=evaluator_base.overlay_best_max_cp_loss,
        overlay_excellent_max_cp_loss=evaluator_base.overlay_excellent_max_cp_loss,
        overlay_good_max_cp_loss=evaluator_base.overlay_good_max_cp_loss,
        overlay_mistake_min_cp_loss=evaluator_base.overlay_mistake_min_cp_loss,
        overlay_blunder_min_cp_loss=evaluator_base.overlay_blunder_min_cp_loss,
        missed_win_enabled=evaluator_base.missed_win_enabled,
        missed_win_mate_ply_cap_by_mode=evaluator_base.missed_win_mate_ply_cap_by_mode,
        engine_depth=config.engine_depth or evaluator_base.engine_depth,
        engine_time_limit_seconds=config.engine_time_limit_seconds or evaluator_base.engine_time_limit_seconds,
        engine_path=config.engine_executable_path or evaluator_base.engine_path,
        active_envelope_player_moves=evaluator_base.active_envelope_player_moves,
        good_moves_acceptable=evaluator_base.good_moves_acceptable,
    )

    corpus = _resolve_corpus_asset(
        runtime_mode=runtime_mode,
        runtime_paths=resolved_runtime_paths.paths,
        bundle_dir=config.corpus_bundle_dir,
        bundle_source=_configured_asset_source(
            override_value=overrides.corpus_bundle_dir,
            file_value=file_config.corpus_bundle_dir,
            env_name=ENV_CORPUS_BUNDLE_DIR,
            prefer_file_value=config_prefers_file_assets,
            file_source=config_resolution.asset_source,
        ),
        legacy_artifact_path=config.corpus_artifact_path,
        legacy_source=_configured_asset_source(
            override_value=overrides.corpus_artifact_path,
            file_value=file_config.corpus_artifact_path,
            env_name=ENV_CORPUS_PATH,
            prefer_file_value=config_prefers_file_assets,
            file_source=config_resolution.asset_source,
        ),
    )
    engine = _resolve_engine_asset(
        runtime_mode=runtime_mode,
        runtime_paths=resolved_runtime_paths.paths,
        selected_path=config.engine_executable_path,
        selected_source=_configured_asset_source(
            override_value=overrides.engine_executable_path,
            file_value=file_config.engine_executable_path,
            env_name=ENV_ENGINE_PATH,
            prefer_file_value=config_prefers_file_assets,
            file_source=config_resolution.asset_source,
        ),
    )
    book = _resolve_file_asset(
        runtime_mode=runtime_mode,
        runtime_paths=resolved_runtime_paths.paths,
        selected_path=config.opening_book_path,
        selected_source=_configured_asset_source(
            override_value=overrides.opening_book_path,
            file_value=file_config.opening_book_path,
            env_name=ENV_BOOK_PATH,
            prefer_file_value=config_prefers_file_assets,
            file_source=config_resolution.asset_source,
        ),
        env_name=ENV_BOOK_PATH,
        workspace_candidates=WORKSPACE_BOOK_PATHS,
        repo_candidates=DEFAULT_BOOK_PATHS,
        label="opening book",
    )

    return RuntimeContext(
        runtime_mode=runtime_mode,
        runtime_mode_source=runtime_mode_resolution.source,
        runtime_mode_reason=runtime_mode_resolution.reason,
        runtime_paths=resolved_runtime_paths.paths,
        config=config,
        evaluator_config=EvaluatorConfig(
            **{**evaluator_config.snapshot(), "engine_path": str(engine.path) if engine.path else evaluator_config.engine_path}
        ),
        corpus=corpus,
        book=book,
        engine=engine,
        config_source=config_resolution.description,
    )


def _resolve_practical_risk_reconciled_path(
    *,
    override_value: str | None,
    file_value: str | None,
    env_value: str | None,
    runtime_paths: RuntimePaths,
) -> str:
    if override_value and str(override_value).strip():
        return str(override_value).strip()
    if file_value and str(file_value).strip():
        return str(file_value).strip()
    if env_value and str(env_value).strip():
        return str(env_value).strip()
    default_path = runtime_paths.content_root / "practical_risk" / "reconciled" / "default" / "practical_risk_reconciled.sqlite"
    return str(default_path)


def corpus_status_detail(path: str | Path | None) -> str:
    if path is None:
        return "no compatible corpus source loaded; opponent provider will use Stockfish fallback before random legal fallback"
    local_path = Path(path)
    if local_path.is_dir():
        compatibility = inspect_corpus_bundle(local_path)
        if compatibility.available:
            return compatibility.detail
        return (
            f"selected corpus bundle directory {local_path} rejected: {compatibility.failure_reason}; "
            "continuing with degraded fallback order"
        )
    if not local_path.exists():
        return "no compatible corpus source loaded; opponent provider will use Stockfish fallback before random legal fallback"
    artifact = load_artifact(local_path)
    return (
        f"loaded legacy corpus artifact {path} (source={path}, schema={artifact.schema_version}, "
        f"rating_policy={artifact.rating_policy}, retained_ply_depth={artifact.retained_ply_depth}, "
        f"positions={len(artifact.positions)})"
    )


@dataclass(frozen=True)
class ResolvedConfigFile:
    path: Path | None
    description: str
    kind: str
    asset_source: str | None


@dataclass(frozen=True)
class BundleCompatibility:
    bundle_dir: Path
    manifest_path: Path
    aggregate_path: Path
    available: bool
    detail: str
    failure_reason: str | None = None
    retained_ply_depth: int | None = None
    retained_ply_source: str | None = None
    payload_format: str | None = None
    bundle_kind: str | None = None


def bundle_retained_ply_depth_from_metadata(bundle_dir: Path, manifest: dict[str, object] | None = None) -> tuple[int | None, str | None]:
    metadata = manifest if isinstance(manifest, dict) else None
    candidate_keys = (
        "retained_ply_depth",
        "retained_opening_ply_depth",
        "opening_retained_ply_depth",
        "max_retained_ply_depth",
        "supported_ply_depth",
    )
    if metadata is not None:
        for key in candidate_keys:
            value = metadata.get(key)
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed >= 2:
                return parsed, f"manifest:{key}"
        identity_fields = (metadata.get("artifact_id"), metadata.get("bundle_id"), metadata.get("bundle_name"), metadata.get("name"))
        for field in identity_fields:
            parsed = _extract_ply_depth_from_text(field)
            if parsed is not None:
                return parsed, "manifest_identity"
    parsed_from_name = _extract_ply_depth_from_text(bundle_dir.name)
    if parsed_from_name is not None:
        return parsed_from_name, "bundle_directory_name"
    return None, None


def max_supported_player_moves_from_retained_plies(retained_ply_depth: int | None) -> int | None:
    if retained_ply_depth is None:
        return None
    return max(2, int(retained_ply_depth) // 2)


def _extract_ply_depth_from_text(value: object) -> int | None:
    if not isinstance(value, str) or not value.strip():
        return None
    match = re.search(r"(?:^|[^a-z0-9])ply[_-]?(\d+)(?:[^a-z0-9]|$)", value.lower())
    if match is None:
        return None
    try:
        parsed = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 2 else None


def inspect_corpus_bundle(bundle_dir: Path) -> BundleCompatibility:
    resolved_dir = bundle_dir.expanduser()
    manifest_path = resolved_dir / BUNDLE_MANIFEST_NAME
    default_payload_path = resolved_dir / BUNDLE_AGGREGATE_RELATIVE_PATH
    if not resolved_dir.exists():
        return BundleCompatibility(resolved_dir, manifest_path, default_payload_path, False, f"bundle directory missing at {resolved_dir}", "bundle directory does not exist")
    if not resolved_dir.is_dir():
        return BundleCompatibility(resolved_dir, manifest_path, default_payload_path, False, f"bundle path {resolved_dir} is not a directory", "bundle path is not a directory")
    if not manifest_path.exists():
        return BundleCompatibility(resolved_dir, manifest_path, default_payload_path, False, f"bundle directory {resolved_dir} missing manifest.json", "manifest.json is missing")

    try:
        manifest = _load_json_file(manifest_path)
    except json.JSONDecodeError as exc:
        return BundleCompatibility(resolved_dir, manifest_path, default_payload_path, False, f"bundle manifest could not be parsed: {exc}", "manifest.json is not valid JSON")

    retained_ply_depth, retained_source = bundle_retained_ply_depth_from_metadata(resolved_dir, manifest)
    bundle_kind = classify_bundle_contract(manifest)
    payload_version = manifest_payload_version(manifest)
    canonical_exact_payload = manifest_declared_canonical_exact_payload_path(manifest, resolved_dir)
    compatibility_exact_payload = manifest_declared_compatibility_exact_payload_path(manifest, resolved_dir)
    payload_status = manifest.get("payload_status")
    if bundle_kind == "legacy_aggregate":
        supported, payload_path, payload_format, failure_reason = _supports_builder_aggregate_bundle_without_mount(manifest, resolved_dir)
    else:
        supported, payload_path, payload_format, failure_reason = _supports_timing_conditioned_bundle_without_mount(manifest, resolved_dir)
    if not supported:
        detail = f"bundle manifest rejected: {failure_reason}"
        if payload_status is not None:
            detail = f"{detail} (builder_payload_status={payload_status!r})"
        return BundleCompatibility(
            resolved_dir,
            manifest_path,
            payload_path,
            False,
            detail,
            failure_reason,
            retained_ply_depth=retained_ply_depth,
            retained_ply_source=retained_source,
            bundle_kind=bundle_kind,
        )

    if not payload_format:
        payload_format = "jsonl"
    canonical_exact_payload_available = bool(canonical_exact_payload and sqlite_payload_path_exists(canonical_exact_payload))
    compatibility_exact_payload_available = bool(compatibility_exact_payload and sqlite_payload_path_exists(compatibility_exact_payload))
    position_key_format = manifest.get("position_key_format")
    move_key_format = manifest.get("move_key_format")
    return BundleCompatibility(
        resolved_dir.resolve(),
        manifest_path.resolve(),
        payload_path.resolve(),
        True,
        (
            f"loaded corpus bundle {resolved_dir.resolve()} (manifest ok, payload ok, "
            f"bundle_kind={bundle_kind}, build_status={manifest.get('build_status')}, payload_format={payload_format!r}, payload_path={str(payload_path)!r}, "
            f"canonical_exact_payload={str(canonical_exact_payload) if canonical_exact_payload else None!r}, "
            f"canonical_exact_payload_available={canonical_exact_payload_available!r}, "
            f"compatibility_exact_payload={str(compatibility_exact_payload) if compatibility_exact_payload else None!r}, "
            f"compatibility_exact_payload_available={compatibility_exact_payload_available!r}, "
            f"payload_version={payload_version!r}, "
            f"position_key_format={position_key_format}, move_key_format={move_key_format}, builder_payload_status={payload_status!r}, "
            f"retained_ply_depth={retained_ply_depth!r}, retained_ply_source={retained_source!r})"
        ),
        retained_ply_depth=retained_ply_depth,
        retained_ply_source=retained_source,
        payload_format=str(payload_format),
        bundle_kind=bundle_kind,
    )


def _supports_timing_conditioned_bundle_without_mount(
    manifest: dict[str, object],
    bundle_dir: Path,
) -> tuple[bool, Path, str, str]:
    candidate_paths: list[Path] = []
    canonical = manifest_declared_canonical_exact_payload_path(manifest, bundle_dir)
    if canonical is not None:
        candidate_paths.append(canonical)
    compatibility = manifest_declared_compatibility_exact_payload_path(manifest, bundle_dir)
    if compatibility is not None:
        candidate_paths.append(compatibility)
    exact_declared = manifest_declared_exact_sqlite_path(manifest, bundle_dir)
    if exact_declared is not None:
        candidate_paths.append(exact_declared)
    candidate_paths.extend(
        [
            bundle_dir / BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH,
            bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH,
        ]
    )
    for candidate in candidate_paths:
        if sqlite_payload_path_exists(candidate):
            return True, candidate, "sqlite", "supported timing-conditioned bundle"
    return False, candidate_paths[0] if candidate_paths else (bundle_dir / BUNDLE_EXACT_SQLITE_DEFAULT_RELATIVE_PATH), "sqlite", "timing-conditioned bundle did not expose a supported exact SQLite payload"


def _supports_builder_aggregate_bundle_without_mount(
    manifest: dict[str, object],
    bundle_dir: Path,
) -> tuple[bool, Path, str, str]:
    build_status = manifest.get("build_status")
    if build_status != "aggregation_complete":
        return False, bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH, "sqlite", f"build_status {build_status!r} is not supported"

    position_key_format = manifest.get("position_key_format")
    if position_key_format != SUPPORTED_BUNDLE_POSITION_KEY_FORMAT:
        return False, bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH, "sqlite", f"unsupported position_key_format {position_key_format!r}"

    move_key_format = manifest.get("move_key_format")
    if move_key_format != SUPPORTED_BUNDLE_MOVE_KEY_FORMAT:
        return False, bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH, "sqlite", f"unsupported move_key_format {move_key_format!r}"

    payload_format_declared = manifest.get("payload_format")
    if isinstance(payload_format_declared, str) and payload_format_declared.strip():
        normalized = payload_format_declared.strip().lower()
        if normalized == "sqlite":
            requested = manifest_declared_sqlite_path(manifest, bundle_dir) or (bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH)
            if sqlite_payload_path_exists(requested):
                return True, requested, "sqlite", "supported builder aggregate bundle"
            return False, requested, "sqlite", "missing sqlite payload"
        if normalized == "jsonl":
            aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir) or (bundle_dir / BUNDLE_AGGREGATE_RELATIVE_PATH)
            if not aggregate_path.exists():
                return False, aggregate_path, "jsonl", f"aggregate payload is missing at {aggregate_path}"
            if not aggregate_path.is_file():
                return False, aggregate_path, "jsonl", f"aggregate payload path {aggregate_path} is not a file"
            if not (aggregate_payload_exposes_raw_counts(aggregate_path) or payload_status_mentions_counts(manifest.get("payload_status"))):
                return False, aggregate_path, "jsonl", "aggregate payload does not expose raw counts required by the trainer runtime"
            return True, aggregate_path, "jsonl", "supported builder aggregate bundle"
        return False, bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH, normalized, f"unsupported payload_format {payload_format_declared!r}"

    sqlite_default = bundle_dir / BUNDLE_SQLITE_RELATIVE_PATH
    if sqlite_payload_path_exists(sqlite_default):
        return True, sqlite_default, "sqlite", "supported builder aggregate bundle"

    aggregate_path = manifest_declared_aggregate_path(manifest, bundle_dir) or (bundle_dir / BUNDLE_AGGREGATE_RELATIVE_PATH)
    if aggregate_path.exists() and aggregate_path.is_file():
        if not (aggregate_payload_exposes_raw_counts(aggregate_path) or payload_status_mentions_counts(manifest.get("payload_status"))):
            return False, aggregate_path, "jsonl", "aggregate payload does not expose raw counts required by the trainer runtime"
        return True, aggregate_path, "jsonl", "supported builder aggregate bundle"

    return False, sqlite_default, "sqlite", "bundle did not expose a supported payload; expected manifest payload_format, data/corpus.sqlite, or data/aggregated_position_move_counts.jsonl"


def _load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _resolve_config_file_path(
    override_path: str | None,
    runtime_mode: RuntimeMode,
    runtime_paths: RuntimePaths,
) -> ResolvedConfigFile:
    if override_path:
        return ResolvedConfigFile(Path(override_path), f"CLI flag --runtime-config: {override_path}", "explicit", "runtime-config")
    env_path = os.getenv(ENV_RUNTIME_CONFIG)
    if env_path:
        return ResolvedConfigFile(Path(env_path), f"environment variable {ENV_RUNTIME_CONFIG}: {env_path}", "explicit", "runtime-config")

    if runtime_mode is RuntimeMode.CONSUMER:
        consumer_candidate = runtime_paths.runtime_config_path
        if consumer_candidate is not None and consumer_candidate.exists():
            return ResolvedConfigFile(
                consumer_candidate,
                f"consumer app-state runtime config: {consumer_candidate}",
                "auto-consumer",
                "consumer-runtime-config",
            )
    else:
        workspace_candidate = runtime_paths.workspace_root / WORKSPACE_RUNTIME_CONFIG_PATH
        if workspace_candidate.exists():
            return ResolvedConfigFile(
                workspace_candidate,
                f"workspace-root default runtime config: {workspace_candidate}",
                "auto-workspace",
                "workspace-runtime-config",
            )

        repo_candidate = runtime_paths.repo_root / DEFAULT_RUNTIME_CONFIG_PATH
        if repo_candidate.exists():
            return ResolvedConfigFile(
                repo_candidate,
                f"repo-local default runtime config: {repo_candidate}",
                "auto-repo",
                "repo-runtime-config",
            )
    return ResolvedConfigFile(None, "built-in defaults", "defaults", None)


def _resolve_corpus_asset(
    runtime_mode: RuntimeMode,
    runtime_paths: RuntimePaths,
    bundle_dir: str | None,
    bundle_source: str | None,
    legacy_artifact_path: str | None,
    legacy_source: str | None,
) -> ResolvedAssetPath:
    bundle_resolution = _resolve_explicit_bundle_dir(bundle_dir, bundle_source)
    if bundle_resolution is not None:
        if bundle_resolution.available:
            return bundle_resolution
        legacy_resolution = _resolve_file_asset(
            runtime_mode=runtime_mode,
            runtime_paths=runtime_paths,
            selected_path=legacy_artifact_path,
            selected_source=legacy_source,
            env_name=ENV_CORPUS_PATH,
            workspace_candidates=WORKSPACE_CORPUS_PATHS,
            repo_candidates=(Path(DEFAULT_ARTIFACT_PATH),),
            label="legacy corpus artifact",
        )
        if legacy_resolution.available:
            return ResolvedAssetPath(
                label="corpus",
                path=legacy_resolution.path,
                source=legacy_resolution.source,
                available=True,
                detail=f"{bundle_resolution.detail}; falling back to {legacy_resolution.detail}",
            )
        return ResolvedAssetPath(
            label="corpus",
            path=bundle_resolution.path,
            source=bundle_resolution.source,
            available=False,
            detail=f"{bundle_resolution.detail}; no legacy corpus artifact available, so runtime will fall back to Stockfish then random legal moves",
        )

    legacy_resolution = _resolve_file_asset(
        runtime_mode=runtime_mode,
        runtime_paths=runtime_paths,
        selected_path=legacy_artifact_path,
        selected_source=legacy_source,
        env_name=ENV_CORPUS_PATH,
        workspace_candidates=WORKSPACE_CORPUS_PATHS,
        repo_candidates=(Path(DEFAULT_ARTIFACT_PATH),),
        label="legacy corpus artifact",
    )
    if legacy_resolution.available:
        return ResolvedAssetPath(
            label="corpus",
            path=legacy_resolution.path,
            source=legacy_resolution.source,
            available=True,
            detail=legacy_resolution.detail,
        )
    return ResolvedAssetPath(
        label="corpus",
        path=legacy_resolution.path,
        source=legacy_resolution.source,
        available=False,
        detail=f"{legacy_resolution.detail}; no compatible corpus source loaded, so runtime will fall back to Stockfish then random legal moves",
    )


def _resolve_explicit_bundle_dir(selected_path: str | None, selected_source: str | None) -> ResolvedAssetPath | None:
    if selected_path is None or selected_source is None:
        return None
    compatibility = inspect_corpus_bundle(Path(selected_path))
    detail = f"selected corpus bundle directory via {_winner_label(selected_source)}: {compatibility.detail}"
    return ResolvedAssetPath(
        label="corpus bundle directory",
        path=str(compatibility.bundle_dir),
        source=selected_source,
        available=compatibility.available,
        detail=detail,
    )


def _resolve_file_asset(
    runtime_mode: RuntimeMode,
    runtime_paths: RuntimePaths,
    selected_path: str | None,
    selected_source: str | None,
    env_name: str,
    workspace_candidates: tuple[Path, ...],
    repo_candidates: tuple[Path, ...],
    label: str,
) -> ResolvedAssetPath:
    winning_explicit = _resolve_explicit_asset_path(selected_path=selected_path, selected_source=selected_source, env_name=env_name, label=label)
    if winning_explicit is not None:
        return winning_explicit

    workspace_root = runtime_paths.workspace_root
    repo_root = runtime_paths.repo_root
    if runtime_mode is RuntimeMode.CONSUMER:
        if label == "opening book":
            consumer_path = runtime_paths.opening_book_path
        elif label == "legacy corpus artifact":
            consumer_path = runtime_paths.content_root / "opening_corpus.json"
        else:
            consumer_path = runtime_paths.content_root / f"{label.replace(' ', '_')}"
        if consumer_path.exists():
            return ResolvedAssetPath(
                label=label,
                path=consumer_path.resolve(),
                source="consumer-default",
                available=True,
                detail=f"{label} loaded from consumer content-root path {consumer_path.resolve()}",
            )
        return ResolvedAssetPath(
            label=label,
            path=consumer_path,
            source="consumer-default",
            available=False,
            detail=f"{label} missing at consumer content-root path {consumer_path}",
        )

    workspace_probes = tuple(workspace_root / candidate for candidate in workspace_candidates)
    repo_probes = tuple(repo_root / candidate for candidate in repo_candidates)
    for candidate in workspace_probes:
        if candidate.exists():
            return ResolvedAssetPath(
                label=label,
                path=candidate.resolve(),
                source="workspace-default",
                available=True,
                detail=f"{label} loaded from workspace-root default path {candidate.resolve()}",
            )
    for candidate in repo_probes:
        if candidate.exists():
            return ResolvedAssetPath(
                label=label,
                path=candidate.resolve(),
                source="repo-default",
                available=True,
                detail=f"{label} loaded from repo-local default path {candidate.resolve()}",
            )
    checked = [str(path) for path in (*workspace_probes, *repo_probes)]
    return ResolvedAssetPath(
        label=label,
        path=workspace_probes[0].resolve() if workspace_probes else (repo_probes[0].resolve() if repo_probes else None),
        source="default",
        available=False,
        detail=f"{label} not found; checked workspace-root default(s) then repo-local default(s): {', '.join(checked)}",
    )


def _resolve_engine_asset(
    runtime_mode: RuntimeMode,
    runtime_paths: RuntimePaths,
    selected_path: str | None,
    selected_source: str | None,
) -> ResolvedAssetPath:
    winning_explicit = _resolve_explicit_engine_path(selected_path=selected_path, selected_source=selected_source)
    if winning_explicit is not None:
        return winning_explicit

    if runtime_mode is RuntimeMode.CONSUMER:
        stockfish_root = runtime_paths.stockfish_root
        if stockfish_root.exists():
            return ResolvedAssetPath(
                "engine",
                stockfish_root.resolve(),
                "consumer-default",
                True,
                f"engine resolved from consumer stockfish root {stockfish_root.resolve()}",
            )
        return ResolvedAssetPath(
            "engine",
            stockfish_root,
            "consumer-default",
            False,
            f"engine missing at consumer stockfish root {stockfish_root}",
        )

    workspace_root = runtime_paths.workspace_root
    repo_root = runtime_paths.repo_root
    for candidate in tuple(workspace_root / path for path in WORKSPACE_ENGINE_PATHS):
        if candidate.exists():
            return ResolvedAssetPath(
                "engine",
                candidate.resolve(),
                "workspace-default",
                True,
                f"engine resolved from workspace-root default path {candidate.resolve()}",
            )
    for candidate in tuple(repo_root / path for path in DEFAULT_ENGINE_PATHS):
        if candidate.exists():
            return ResolvedAssetPath("engine", candidate.resolve(), "repo-default", True, f"engine resolved from repo-local default path {candidate.resolve()}")
    which_stockfish = shutil.which("stockfish")
    if which_stockfish:
        return ResolvedAssetPath("engine", Path(which_stockfish).resolve(), "default", True, f"engine resolved from PATH stockfish at {which_stockfish}")
    return ResolvedAssetPath(
        "engine",
        Path("stockfish"),
        "default",
        False,
        "engine not found; checked CLI/config/env winner, workspace-root defaults, repo-local defaults, and PATH stockfish",
    )


def _resolve_explicit_asset_path(selected_path: str | None, selected_source: str | None, env_name: str, label: str) -> ResolvedAssetPath | None:
    if selected_path is None or selected_source is None:
        return None

    local_probe_path = _local_probe_path(selected_path)
    available = local_probe_path.exists()
    detail = _explicit_asset_detail(
        label,
        selected_path,
        local_probe_path,
        available,
        selected_source=selected_source,
        env_name=env_name if selected_source == "environment" else None,
    )
    return ResolvedAssetPath(label=label, path=selected_path, source=selected_source, available=available, detail=detail)


def _resolve_explicit_engine_path(selected_path: str | None, selected_source: str | None) -> ResolvedAssetPath | None:
    if selected_path is None or selected_source is None:
        return None

    local_probe_path = _local_probe_path(selected_path)
    path_lookup_match = shutil.which(selected_path)
    available = local_probe_path.exists() or path_lookup_match is not None
    detail = _explicit_engine_detail(
        selected_path,
        local_probe_path,
        available,
        selected_source=selected_source,
        env_name=ENV_ENGINE_PATH if selected_source == "environment" else None,
        which_match=path_lookup_match,
    )
    return ResolvedAssetPath("engine", selected_path, selected_source, available, detail)


def _configured_asset_source(
    override_value: str | int | float | None,
    file_value: str | int | float | None,
    env_name: str,
    prefer_file_value: bool,
    file_source: str | None,
) -> str | None:
    if override_value is not None:
        return "cli"
    env_value = os.getenv(env_name)
    if prefer_file_value and file_value is not None:
        return file_source
    if env_value is not None:
        return "environment"
    if file_value is not None:
        return file_source
    return None





def _default_local_app_data_root() -> Path:
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data)
    return Path.home() / "AppData" / "Local"

def _repo_root() -> Path:
    return Path.cwd().resolve()


def _workspace_root() -> Path:
    return _repo_root().parent


def _pick_asset_value(
    override_value: Any,
    file_value: Any,
    env_value: Any,
    prefer_file_value: bool,
) -> Any:
    if override_value is not None:
        return override_value
    if prefer_file_value:
        return _first_non_none(file_value, env_value)
    return _first_non_none(env_value, file_value)


def _first_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _coerce_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _coerce_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _resolve_bool(override: bool | None, file_value: bool, env_value: str | None, prefer_file_value: bool) -> bool:
    if override is not None:
        return override
    parsed_env = None
    if env_value is not None:
        parsed_env = env_value.strip().lower() in {"1", "true", "yes", "on"}
    if prefer_file_value:
        return file_value if file_value is not None else bool(parsed_env)
    return parsed_env if parsed_env is not None else file_value


def _local_probe_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    return candidate.resolve() if candidate.exists() else candidate


def _explicit_asset_detail(
    label: str,
    configured_value: str,
    probe_path: Path,
    available: bool,
    selected_source: str,
    env_name: str | None = None,
) -> str:
    source = _winner_label(selected_source)
    detail = f"{label} {'loaded' if available else 'missing'} from {source}; configured value={configured_value}"
    if env_name:
        detail = f"{detail}; source detail=environment variable {env_name}"
    probe_note = _probe_note(configured_value, probe_path)
    return f"{detail}; {probe_note}" if probe_note else detail


def _explicit_engine_detail(
    configured_value: str,
    probe_path: Path,
    available: bool,
    selected_source: str,
    env_name: str | None = None,
    which_match: str | None = None,
) -> str:
    source = _winner_label(selected_source)
    detail = f"engine {'resolved' if available else 'missing'} from {source}; configured value={configured_value}"
    if env_name:
        detail = f"{detail}; source detail=environment variable {env_name}"
    probe_parts: list[str] = []
    probe_note = _probe_note(configured_value, probe_path)
    if probe_note:
        probe_parts.append(probe_note)
    if which_match is not None:
        probe_parts.append(f"PATH lookup match={which_match}")
    return f"{detail}; {'; '.join(probe_parts)}" if probe_parts else detail


def _winner_label(selected_source: str | None) -> str:
    if selected_source == "cli":
        return "CLI winner"
    if selected_source == "runtime-config":
        return "runtime-config winner"
    if selected_source == "workspace-runtime-config":
        return "workspace runtime.local.json winner"
    if selected_source == "repo-runtime-config":
        return "repo runtime config winner"
    if selected_source == "consumer-runtime-config":
        return "consumer runtime config winner"
    if selected_source == "environment":
        return "environment winner"
    return "configured winner"


def _probe_note(configured_value: str, probe_path: Path) -> str:
    probe_value = str(probe_path)
    if probe_value != configured_value:
        return f"local probe candidate={probe_value}"
    return ""
