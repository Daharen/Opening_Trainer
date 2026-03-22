from __future__ import annotations

import argparse

from .session import TrainingSession


def run_cli() -> None:
    print("Opening Trainer v2 (CLI)", flush=True)
    session = TrainingSession()
    while True:
        session.start_new_game()
        session.run_session()


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Opening Trainer")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--gui", action="store_true", help="Launch the local Tkinter board GUI.")
    mode.add_argument("--cli", action="store_true", help="Run the console trainer.")
    args = parser.parse_args(argv)

    if args.cli:
        run_cli()
        return

    try:
        from .ui.gui_app import launch_gui
    except Exception as exc:
        print(f"GUI unavailable ({exc}). Falling back to CLI.", flush=True)
        run_cli()
        return

    launch_gui()
