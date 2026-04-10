from __future__ import annotations

import argparse
import ctypes
import os
import shutil
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
import sys

from .corpus import CorpusIngestor, DEFAULT_ARTIFACT_PATH, save_artifact
from .runtime import RuntimeOverrides, load_runtime_config
from .session import TrainingSession
from .session_logging import get_session_logger, initialize_session_logging, log_line
from .single_instance import INSTANCE_DIAGNOSTICS_PATH_ENV
from .updater import check_for_update, launch_updater_helper, log_install_runtime_diagnostics, resolve_manifest_path_or_url


def _apply_runtime_environment(runtime_context) -> None:
    runtime_paths = getattr(runtime_context, "runtime_paths", None)
    if runtime_paths is None:
        return
    session_log_dir = runtime_paths.log_root / "sessions"
    initialize_session_logging(session_log_dir)
    instance_diagnostics_path = runtime_paths.log_root / "instance" / "opening_trainer_instance.json"
    os.environ[INSTANCE_DIAGNOSTICS_PATH_ENV] = str(instance_diagnostics_path)


def _build_runtime_overrides(args: argparse.Namespace) -> RuntimeOverrides:
    return RuntimeOverrides(
        runtime_mode=args.runtime_mode,
        corpus_bundle_dir=args.corpus_bundle_dir,
        corpus_artifact_path=args.corpus_artifact,
        engine_executable_path=args.engine_path,
        opening_book_path=args.book_path,
        runtime_config_path=args.runtime_config,
        engine_depth=args.engine_depth,
        engine_time_limit_seconds=args.engine_time_limit,
        practical_risk_reconciled_path=args.practical_risk_reconciled_path,
        strict_assets=True if args.strict_assets else None,
    )


def run_cli(runtime_overrides: RuntimeOverrides | None = None) -> None:
    runtime_context = load_runtime_config(runtime_overrides)
    _apply_runtime_environment(runtime_context)
    get_session_logger()
    log_line("Opening Trainer v2 (CLI)", tag="startup")
    session = TrainingSession(runtime_context=runtime_context, mode="cli")
    while True:
        session.start_new_game()
        session.run_session()


def _is_frozen_consumer_launch(runtime_context) -> bool:
    runtime_mode = getattr(getattr(runtime_context, "runtime_mode", None), "value", "")
    return bool(getattr(sys, "frozen", False)) and runtime_mode == "consumer"


def _startup_failure_log_path(runtime_context) -> Path:
    runtime_paths = getattr(runtime_context, "runtime_paths", None)
    if runtime_paths is not None:
        app_state_root = getattr(runtime_paths, "app_state_root", None)
        if app_state_root is not None:
            return Path(app_state_root) / "startup_failure.log"
    local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home()))
    return local_app_data / "OpeningTrainer" / "startup_failure.log"


def _write_startup_failure_artifact(runtime_context, stage: str, exc: Exception) -> Path:
    log_path = _startup_failure_log_path(runtime_context)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    details = [
        f"timestamp_utc={datetime.now(timezone.utc).isoformat()}",
        f"stage={stage}",
        f"runtime_mode={getattr(getattr(runtime_context, 'runtime_mode', None), 'value', 'unknown')}",
        f"runtime_mode_source={getattr(runtime_context, 'runtime_mode_source', 'unknown')}",
        f"runtime_mode_reason={getattr(runtime_context, 'runtime_mode_reason', 'unknown')}",
        f"executable={sys.executable}",
        f"frozen={bool(getattr(sys, 'frozen', False))}",
        "",
        "traceback:",
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    ]
    log_path.write_text("\n".join(details), encoding="utf-8")
    return log_path


def _show_startup_failure_dialog(stage: str, exc: Exception, log_path: Path) -> None:
    message = (
        "Opening Trainer failed to start.\n\n"
        f"Failure stage: {stage}\n"
        f"Error: {exc}\n\n"
        f"Details were written to:\n{log_path}"
    )
    if os.name == "nt":
        try:
            ctypes.windll.user32.MessageBoxW(0, message, "Opening Trainer Startup Failure", 0x10 | 0x0)
            return
        except Exception:
            pass
    print(message, file=sys.stderr)


def _handle_frozen_consumer_gui_failure(runtime_context, stage: str, exc: Exception) -> None:
    log_path = _write_startup_failure_artifact(runtime_context, stage, exc)
    log_line(f"GUI_STARTUP_FATAL: stage={stage}; log_path={log_path}; error={exc}", tag="error")
    _show_startup_failure_dialog(stage, exc, log_path)
    raise SystemExit(1)


def _probe_gui_bootstrap(runtime_context) -> None:
    try:
        from .ui import gui_app  # noqa: F401
    except Exception as exc:
        raise SystemExit(f"GUI probe import failed: {exc}") from exc
    try:
        import tkinter as tk

        root = tk.Tk()
        root.withdraw()
        root.update_idletasks()
        root.destroy()
    except Exception as exc:
        raise SystemExit(f"GUI probe bootstrap failed: {exc}") from exc
    log_line("GUI probe succeeded.", tag="startup")


def _probe_real_gui_startup(runtime_context) -> None:
    from .ui.gui_app import launch_gui
    import tempfile

    previous_cwd = Path.cwd()
    probe_temp_dir = Path(tempfile.mkdtemp(prefix="opening_trainer_probe_"))
    log_line(f"GUI_PROBE_TEMP_CWD_CREATED: path={probe_temp_dir}", tag="startup")
    try:
        os.chdir(probe_temp_dir)
        launch_gui(runtime_context=runtime_context, probe_real_startup=True)
    except Exception as exc:
        raise SystemExit(f"Real GUI startup probe failed: {exc}") from exc
    finally:
        os.chdir(previous_cwd)
        log_line(f"GUI_PROBE_TEMP_CWD_RESTORED: path={previous_cwd}", tag="startup")
        _cleanup_probe_temp_dir(probe_temp_dir)
    log_line("GUI_PROBE_REAL_STARTUP_OK", tag="startup")
    log_line("Real GUI startup probe succeeded.", tag="startup")


def _cleanup_probe_temp_dir(path: Path, *, retries: int = 4, retry_delay_seconds: float = 0.1) -> None:
    if not path.exists():
        return
    for attempt in range(1, retries + 1):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except PermissionError as exc:
            if attempt == retries:
                log_line(f"GUI_PROBE_TEMP_CWD_CLEANUP_DEFERRED: path={path}; error={exc}", tag="warning")
                return
            log_line(
                f"GUI_PROBE_TEMP_CWD_CLEANUP_RETRY: path={path}; attempt={attempt}/{retries}; error={exc}",
                tag="warning",
            )
            time.sleep(retry_delay_seconds)
        except OSError as exc:
            if attempt == retries:
                log_line(f"GUI_PROBE_TEMP_CWD_CLEANUP_DEFERRED: path={path}; error={exc}", tag="warning")
                return
            log_line(
                f"GUI_PROBE_TEMP_CWD_CLEANUP_RETRY: path={path}; attempt={attempt}/{retries}; error={exc}",
                tag="warning",
            )
            time.sleep(retry_delay_seconds)


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Opening Trainer")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--gui", action="store_true", help="Launch the local Tkinter board GUI.")
    mode.add_argument("--cli", action="store_true", help="Run the console trainer.")
    parser.add_argument("--runtime-config", help="Optional JSON runtime config path.")
    parser.add_argument("--runtime-mode", choices=("dev", "consumer"), help="Select runtime lane mode (default: dev).")
    parser.add_argument("--corpus-bundle-dir", help="Override the builder corpus bundle directory.")
    parser.add_argument("--corpus-artifact", help="Override the legacy corpus artifact path.")
    parser.add_argument("--engine-path", help="Override the engine executable path.")
    parser.add_argument("--book-path", help="Override the Polyglot opening-book path.")
    parser.add_argument("--engine-depth", type=int, help="Override engine search depth.")
    parser.add_argument("--engine-time-limit", type=float, help="Override engine analysis time limit in seconds.")
    parser.add_argument("--practical-risk-reconciled-path", help="Override Stage D practical risk reconciled SQLite path.")
    parser.add_argument("--strict-assets", action="store_true", help="Exit if configured runtime assets are missing.")
    parser.add_argument("--show-runtime", action="store_true", help="Print resolved runtime asset diagnostics and exit.")
    parser.add_argument(
        "--probe-gui-bootstrap",
        action="store_true",
        help="Perform lightweight GUI import/bootstrap validation and exit.",
    )
    parser.add_argument(
        "--probe-real-gui-startup",
        action="store_true",
        help="Exercise real GUI startup path without entering the full mainloop.",
    )
    parser.add_argument(
        "--check-for-update",
        metavar="MANIFEST_PATH_OR_URL",
        help="Check app_update_manifest.json and print update status.",
    )
    parser.add_argument(
        "--apply-update",
        metavar="MANIFEST_PATH_OR_URL",
        help="Apply app update payload from manifest and relaunch the app.",
    )
    parser.add_argument(
        "--build-corpus",
        nargs="+",
        metavar="PGN",
        help="Build a runtime corpus artifact from local PGN file(s) and exit.",
    )
    parser.add_argument(
        "--build-corpus-output",
        default=DEFAULT_ARTIFACT_PATH,
        help="Output path for --build-corpus (default: data/opening_corpus.json).",
    )
    args = parser.parse_args(argv)
    runtime_overrides = _build_runtime_overrides(args)

    if args.check_for_update:
        local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        app_state_root = local_app_data / "OpeningTrainer"
        manifest_ref = resolve_manifest_path_or_url(args.check_for_update, app_state_root=app_state_root)
        has_update, manifest, installed = check_for_update(manifest_ref, app_state_root=app_state_root)
        print(
            f"UPDATE_CHECK channel={manifest.channel} installed={None if not installed else installed.get('app_version')} installed_build={None if not installed else installed.get('build_id')} latest={manifest.app_version} latest_build={manifest.build_id} has_update={has_update}"
        )
        return

    if args.apply_update:
        local_app_data = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        app_state_root = local_app_data / "OpeningTrainer"
        launch_updater_helper(
            args.apply_update,
            app_state_root=app_state_root,
            wait_for_pid=os.getpid(),
            relaunch_exe_path=Path(sys.executable),
            relaunch_args=["--runtime-mode", "consumer"],
        )
        print("UPDATE_APPLY_HELPER_LAUNCHED")
        return

    if args.build_corpus:
        artifact = CorpusIngestor().build_artifact(args.build_corpus)
        output_path = save_artifact(artifact, args.build_corpus_output)
        message = f"Built corpus artifact with {len(artifact.positions)} positions at {output_path}"
        log_line(message, tag="corpus")
        return

    runtime_context = load_runtime_config(runtime_overrides)
    _apply_runtime_environment(runtime_context)
    get_session_logger()
    log_install_runtime_diagnostics(app_state_root=runtime_context.runtime_paths.app_state_root, phase="startup")
    log_line(
        f"Runtime mode resolution: mode={runtime_context.runtime_mode.value}; source={runtime_context.runtime_mode_source}; reason={runtime_context.runtime_mode_reason}",
        tag="startup",
    )
    if args.show_runtime:
        log_line(f"Runtime config source: {runtime_context.config_source}", tag="startup")
        log_line(runtime_context.corpus.detail, tag="startup")
        log_line(runtime_context.book.detail, tag="startup")
        log_line(runtime_context.engine.detail, tag="startup")
        log_line(
            f"Practical risk reconciled path: {runtime_context.config.practical_risk_reconciled_path or 'auto/default'}",
            tag="startup",
        )
        return
    if args.probe_gui_bootstrap:
        _probe_gui_bootstrap(runtime_context)
        return
    if args.probe_real_gui_startup:
        _probe_real_gui_startup(runtime_context)
        return

    if runtime_context.config.strict_assets:
        required_assets = (runtime_context.engine, runtime_context.book)
        missing = [asset.label for asset in required_assets if asset.path is not None and not asset.available]
        if missing:
            raise SystemExit(f"Strict runtime assets enabled; missing required asset(s): {', '.join(missing)}")

    if args.cli:
        run_cli(runtime_overrides)
        return

    try:
        from .ui.gui_app import DuplicateInstanceLaunchBlockedError, launch_gui
    except Exception as exc:
        if _is_frozen_consumer_launch(runtime_context):
            _handle_frozen_consumer_gui_failure(runtime_context, "gui_import", exc)
        log_line(f"GUI unavailable ({exc}). Falling back to CLI.", tag="error")
        run_cli(runtime_overrides)
        return

    try:
        launch_gui(runtime_context=runtime_context)
    except DuplicateInstanceLaunchBlockedError as exc:
        log_line(f"GUI launch blocked by duplicate instance ({exc}).", tag="error")
        raise SystemExit(1)
    except Exception as exc:
        if _is_frozen_consumer_launch(runtime_context):
            _handle_frozen_consumer_gui_failure(runtime_context, "gui_bootstrap", exc)
        log_line(f"GUI launch failed ({exc}). Falling back to CLI.", tag="error")
        run_cli(runtime_overrides)
