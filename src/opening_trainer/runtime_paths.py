from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .runtime_mode import RuntimeMode


@dataclass(frozen=True)
class RuntimePaths:
    mode: RuntimeMode
    repo_root: Path
    workspace_root: Path
    app_state_root: Path
    content_root: Path
    log_root: Path
    profile_root: Path
    runtime_config_path: Path | None
    corpus_bundle_root: Path
    predecessor_master_db_path: Path
    opening_book_path: Path
    opening_names_path: Path
    stockfish_root: Path


@dataclass(frozen=True)
class ResolvedRuntimePaths:
    paths: RuntimePaths
    source: str


def _default_local_app_data_root() -> Path:
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data)
    return Path.home() / "AppData" / "Local"


def resolve_runtime_paths(mode: RuntimeMode, *, repo_root: Path, workspace_root: Path) -> ResolvedRuntimePaths:
    if mode is RuntimeMode.CONSUMER:
        local_app_data_root = _default_local_app_data_root()
        app_state_root = local_app_data_root / "OpeningTrainer"
        content_root = local_app_data_root / "OpeningTrainerContent"
        return ResolvedRuntimePaths(
            paths=RuntimePaths(
                mode=mode,
                repo_root=repo_root,
                workspace_root=workspace_root,
                app_state_root=app_state_root,
                content_root=content_root,
                log_root=app_state_root / "logs",
                profile_root=app_state_root / "profiles",
                runtime_config_path=app_state_root / "runtime.consumer.json",
                corpus_bundle_root=content_root / "Timing Conditioned Corpus Bundles",
                predecessor_master_db_path=content_root / "canonical_predecessor_master.sqlite",
                opening_book_path=content_root / "opening_book.bin",
                opening_names_path=content_root / "opening_book_names.zip",
                stockfish_root=content_root / "stockfish",
            ),
            source="consumer-localappdata-defaults",
        )

    content_root = repo_root
    return ResolvedRuntimePaths(
        paths=RuntimePaths(
            mode=mode,
            repo_root=repo_root,
            workspace_root=workspace_root,
            app_state_root=repo_root / "runtime",
            content_root=content_root,
            log_root=workspace_root / "logs",
            profile_root=repo_root / "runtime" / "profiles",
            runtime_config_path=workspace_root / "runtime.local.json",
            corpus_bundle_root=repo_root / "runtime" / "bundles",
            predecessor_master_db_path=Path(
                r"F:\Opening Trainer Large Data File\Work Surface\opening_trainer_content_seed_rapid600_v1\canonical_predecessor_master.sqlite"
            ),
            opening_book_path=repo_root / "runtime" / "opening_book.bin",
            opening_names_path=repo_root / "data" / "opening_book_names.zip",
            stockfish_root=repo_root / "tools" / "stockfish",
        ),
        source="dev-workspace-defaults",
    )
