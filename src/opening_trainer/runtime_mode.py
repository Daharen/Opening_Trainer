from __future__ import annotations

import os
from enum import Enum

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


def resolve_runtime_mode(override: str | RuntimeMode | None = None) -> RuntimeMode:
    if isinstance(override, RuntimeMode):
        return override
    parsed_override = RuntimeMode.parse(override)
    if parsed_override is not None:
        return parsed_override
    parsed_env = RuntimeMode.parse(os.getenv(ENV_RUNTIME_MODE))
    if parsed_env is not None:
        return parsed_env
    return RuntimeMode.DEV
