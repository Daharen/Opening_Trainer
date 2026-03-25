from __future__ import annotations

import os
import subprocess
import threading

import chess.engine

from .config import EvaluatorConfig


def engine_popen_kwargs() -> dict[str, object]:
    if os.name != "nt":
        return {}
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return {"creationflags": creationflags} if creationflags else {}


def launch_engine(config: EvaluatorConfig) -> chess.engine.SimpleEngine:
    return chess.engine.SimpleEngine.popen_uci(config.engine_path, **engine_popen_kwargs())


def shutdown_engine(engine: chess.engine.SimpleEngine | None, *, quit_timeout_seconds: float = 0.75) -> None:
    if engine is None:
        return
    quit_done = threading.Event()

    def _quit() -> None:
        try:
            engine.quit()
        except Exception:
            pass
        finally:
            quit_done.set()

    worker = threading.Thread(target=_quit, daemon=True)
    worker.start()
    quit_done.wait(timeout=max(0.05, quit_timeout_seconds))
    if quit_done.is_set():
        return
    _terminate_engine_process(engine)
    quit_done.wait(timeout=0.2)


def _terminate_engine_process(engine: chess.engine.SimpleEngine) -> None:
    transport = getattr(getattr(engine, "protocol", None), "transport", None)
    if transport is not None:
        try:
            transport.terminate()
            return
        except Exception:
            pass
        try:
            transport.kill()
            return
        except Exception:
            pass

    process = getattr(engine, "process", None)
    if process is None:
        return
    try:
        process.terminate()
    except Exception:
        pass
