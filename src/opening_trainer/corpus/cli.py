from __future__ import annotations

import argparse

from .artifact import save_artifact
from .constants import DEFAULT_ARTIFACT_PATH
from .ingest import CorpusIngestor


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build an Opening Trainer corpus artifact from PGN files.")
    parser.add_argument("pgn_paths", nargs="+", help="One or more local PGN files.")
    parser.add_argument(
        "--output",
        default=DEFAULT_ARTIFACT_PATH,
        help="Artifact output path (default: data/opening_corpus.json).",
    )
    args = parser.parse_args(argv)

    artifact = CorpusIngestor().build_artifact(args.pgn_paths)
    output_path = save_artifact(artifact, args.output)
    print(f"Built corpus artifact with {len(artifact.positions)} positions at {output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
