from __future__ import annotations

import os
import subprocess

import chess.engine

from .config import EvaluatorConfig


def engine_popen_kwargs() -> dict[str, object]:
    if os.name != "nt":
        return {}
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return {"creationflags": creationflags} if creationflags else {}


def launch_engine(config: EvaluatorConfig) -> chess.engine.SimpleEngine:
    return chess.engine.SimpleEngine.popen_uci(config.engine_path, **engine_popen_kwargs())
