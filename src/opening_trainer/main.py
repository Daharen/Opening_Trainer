from __future__ import annotations

import argparse

from .corpus import CorpusIngestor, DEFAULT_ARTIFACT_PATH, save_artifact
from .runtime import RuntimeOverrides, load_runtime_config
from .session import TrainingSession


def _build_runtime_overrides(args: argparse.Namespace) -> RuntimeOverrides:
    return RuntimeOverrides(
        corpus_artifact_path=args.corpus_artifact,
        engine_executable_path=args.engine_path,
        opening_book_path=args.book_path,
        runtime_config_path=args.runtime_config,
        engine_depth=args.engine_depth,
        engine_time_limit_seconds=args.engine_time_limit,
        strict_assets=True if args.strict_assets else None,
    )


def run_cli(runtime_overrides: RuntimeOverrides | None = None) -> None:
    runtime_context = load_runtime_config(runtime_overrides)
    print("Opening Trainer v2 (CLI)", flush=True)
    session = TrainingSession(runtime_context=runtime_context, mode="cli")
    while True:
        session.start_new_game()
        session.run_session()


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Opening Trainer")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--gui", action="store_true", help="Launch the local Tkinter board GUI.")
    mode.add_argument("--cli", action="store_true", help="Run the console trainer.")
    parser.add_argument("--runtime-config", help="Optional JSON runtime config path.")
    parser.add_argument("--corpus-artifact", help="Override the corpus artifact path.")
    parser.add_argument("--engine-path", help="Override the engine executable path.")
    parser.add_argument("--book-path", help="Override the Polyglot opening-book path.")
    parser.add_argument("--engine-depth", type=int, help="Override engine search depth.")
    parser.add_argument("--engine-time-limit", type=float, help="Override engine analysis time limit in seconds.")
    parser.add_argument("--strict-assets", action="store_true", help="Exit if configured runtime assets are missing.")
    parser.add_argument("--show-runtime", action="store_true", help="Print resolved runtime asset diagnostics and exit.")
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

    if args.build_corpus:
        artifact = CorpusIngestor().build_artifact(args.build_corpus)
        output_path = save_artifact(artifact, args.build_corpus_output)
        print(f"Built corpus artifact with {len(artifact.positions)} positions at {output_path}", flush=True)
        return

    runtime_context = load_runtime_config(runtime_overrides)
    if args.show_runtime:
        print(f"Runtime config source: {runtime_context.config_source}", flush=True)
        print(runtime_context.corpus.detail, flush=True)
        print(runtime_context.book.detail, flush=True)
        print(runtime_context.engine.detail, flush=True)
        return

    if runtime_context.config.strict_assets:
        missing = [asset.label for asset in (runtime_context.corpus, runtime_context.engine, runtime_context.book) if asset.path is not None and not asset.available]
        if missing:
            raise SystemExit(f"Strict runtime assets enabled; missing required asset(s): {', '.join(missing)}")

    if args.cli:
        run_cli(runtime_overrides)
        return

    try:
        from .ui.gui_app import launch_gui
    except Exception as exc:
        print(f"GUI unavailable ({exc}). Falling back to CLI.", flush=True)
        run_cli(runtime_overrides)
        return

    launch_gui(runtime_context=runtime_context)
