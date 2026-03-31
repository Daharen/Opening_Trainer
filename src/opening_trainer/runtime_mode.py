from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

ENV_RUNTIME_MODE = "OPENING_TRAINER_RUNTIME_MODE"


class RuntimeMode(str, Enum):
    DEV = "dev"
    CONSUMER = "consumer"

    @classmethod
    def parse(cls, value: str | None) -> "RuntimeMode | None":
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        for mode in cls:
            if mode.value == normalized:
                return mode
        raise ValueError(f"Unsupported runtime mode: {value!r}. Expected one of: dev, consumer")


@dataclass(frozen=True)
class ResolvedRuntimeMode:
    mode: RuntimeMode
    source: str
    reason: str


def resolve_runtime_mode_with_source(
    override: str | RuntimeMode | None = None,
    *,
    app_state_root: Path | None = None,
    content_root: Path | None = None,
) -> ResolvedRuntimeMode:
    if isinstance(override, RuntimeMode):
        return ResolvedRuntimeMode(mode=override, source="cli", reason=f"explicit CLI --runtime-mode={override.value}")

    parsed_override = RuntimeMode.parse(override)
    if parsed_override is not None:
        return ResolvedRuntimeMode(mode=parsed_override, source="cli", reason=f"explicit CLI --runtime-mode={parsed_override.value}")

    parsed_env = RuntimeMode.parse(os.getenv(ENV_RUNTIME_MODE))
    if parsed_env is not None:
        return ResolvedRuntimeMode(mode=parsed_env, source="environment", reason=f"environment variable {ENV_RUNTIME_MODE}={parsed_env.value}")

    if _is_installed_consumer_host() and _has_consumer_install_artifacts(app_state_root, content_root):
        return ResolvedRuntimeMode(
            mode=RuntimeMode.CONSUMER,
            source="auto-consumer",
            reason="inferred consumer mode from installed app-state/content artifacts",
        )

    return ResolvedRuntimeMode(mode=RuntimeMode.DEV, source="default", reason="defaulted to dev mode (no explicit override and no installed-consumer artifacts)")


def resolve_runtime_mode(override: str | RuntimeMode | None = None) -> RuntimeMode:
    return resolve_runtime_mode_with_source(override).mode


def _is_installed_consumer_host() -> bool:
    if os.getenv("OPENING_TRAINER_ASSUME_INSTALLED") == "1":
        return True
    if getattr(sys, "frozen", False):
        return True
    executable_dir = Path(sys.executable).resolve().parent
    return "program files" in str(executable_dir).lower()


def _has_consumer_install_artifacts(app_state_root: Path | None, content_root: Path | None) -> bool:
    state_root = app_state_root or _default_app_state_root()
    content = content_root or _default_content_root()

    runtime_config_exists = (state_root / "runtime.consumer.json").exists()
    installed_manifest_exists = (state_root / "installed_content_manifest.json").exists()
    required_content_entries_exist = all(
        (content / entry).exists()
        for entry in (
            "opening_book.bin",
            "stockfish",
            "Timing Conditioned Corpus Bundles",
        )
    )
    return runtime_config_exists or installed_manifest_exists or required_content_entries_exist


def _default_local_app_data_root() -> Path:
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data)
    return Path.home() / "AppData" / "Local"


def _default_app_state_root() -> Path:
    return _default_local_app_data_root() / "OpeningTrainer"


def _default_content_root() -> Path:
    return _default_local_app_data_root() / "OpeningTrainerContent"
