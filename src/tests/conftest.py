from pathlib import Path
import os
from types import SimpleNamespace
import sys

import pytest

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from opening_trainer.session_logging import (
    SESSION_ID_ENV,
    SESSION_LOG_DIR_ENV,
    SESSION_LOG_PATH_ENV,
    reset_logger_for_tests,
)
from opening_trainer.single_instance import INSTANCE_DIAGNOSTICS_PATH_ENV


@pytest.fixture
def runtime_context_factory(tmp_path):
    def _build(*, runtime_mode: str = "dev", strict_assets: bool = False, runtime_mode_source: str = "default", runtime_mode_reason: str = "test"):
        runtime_root = tmp_path / "runtime"
        return SimpleNamespace(
            config=SimpleNamespace(strict_assets=strict_assets),
            runtime_mode=SimpleNamespace(value=runtime_mode),
            runtime_mode_source=runtime_mode_source,
            runtime_mode_reason=runtime_mode_reason,
            runtime_paths=SimpleNamespace(
                app_state_root=runtime_root / "OpeningTrainer",
                log_root=runtime_root / "logs",
            ),
            evaluator_config=SimpleNamespace(engine_path=None),
            config_source="tests",
            corpus=SimpleNamespace(detail="corpus", path=None, available=True, label="corpus"),
            book=SimpleNamespace(detail="book", path=None, available=True, label="book"),
            engine=SimpleNamespace(detail="engine", path=None, available=True, label="engine"),
        )

    return _build


@pytest.fixture(autouse=True)
def _cleanup_runtime_side_effects():
    yield
    reset_logger_for_tests()
    for env_var in (INSTANCE_DIAGNOSTICS_PATH_ENV, SESSION_ID_ENV, SESSION_LOG_PATH_ENV, SESSION_LOG_DIR_ENV):
        os.environ.pop(env_var, None)
