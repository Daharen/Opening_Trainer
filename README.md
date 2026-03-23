# Opening Trainer

A local chess opening-discipline trainer focused on rapid early-game repetition.

## Project Goal

The trainer enforces move-quality discipline during the first five player moves.
Runs fail immediately when a move is rejected and then restart into a fresh game.
Runs succeed after five accepted player moves and then also restart immediately.

This lane also restores the project's core trajectory: corpus-backed, inspectable,
low-band human opponent sourcing built from local PGN ingestion artifacts.

## Current Stage

Checkpoint v2  
Sprint Step 2 corpus-backed ingestion artifact baseline with engine-unavailability hardening

## Current Implemented Behavior

The application currently provides:

1. Local chess board state using python-chess.
2. Legal move validation before evaluation.
3. Randomized player color at game start.
4. Explicit session states for player turn, opponent turn, fail, success, and authority-unavailable pause.
5. Corpus-backed opponent move generation when a built artifact is present.
6. Explicit provisional random fallback when no corpus artifact has been built yet.
7. Structured move evaluation through explicit book and engine authority seams.
8. Compact evaluation feedback with canonical judgment, overlay label, and reason text.
9. Immediate console flushing so wrapper-launched sessions remain visible.

## Evaluator Baseline

### Canonical model

The evaluator resolves every legal player move inside the active envelope using a
strict authority stack:

1. **Book** if an explicit opening-book authority approves the move.
2. **Better** if book does not approve it but engine tolerance still accepts it.
3. **AuthorityUnavailable** if book does not approve it and engine analysis is unavailable.
4. **Fail** if neither authority accepts it and engine analysis was available.

Book / Better / Fail remain the ordinary move-quality outcomes. `AuthorityUnavailable`
is an explicit runtime safety outcome so missing engine binaries do not masquerade
as real player failures.

### Overlay labels

The current graded overlay baseline is:

- Book
- Best
- Excellent
- Good
- Inaccuracy
- Mistake
- Blunder
- MissedWin
- AuthorityUnavailable

Pass-compatible overlays are Book, Best, Excellent, and Good.
Fail-compatible overlays are Inaccuracy, Mistake, Blunder, and MissedWin.
`AuthorityUnavailable` is intentionally neither a normal pass nor a normal fail.

### Default thresholds

The evaluator keeps thresholds centralized in `EvaluatorConfig`.
Current defaults are intentionally conservative and easy to tune:

- `better_max_cp_loss = 90`
- `overlay_best_max_cp_loss = 15`
- `overlay_excellent_max_cp_loss = 45`
- `overlay_good_max_cp_loss = 90`
- `overlay_mistake_min_cp_loss = 140`
- `overlay_blunder_min_cp_loss = 260`
- `missed_win_enabled = True`
- `missed_win_mate_ply_cap_by_mode = {"default": 4}`
- `active_envelope_player_moves = 5`

### Active envelope

The current training envelope is the player's first five moves. A failed move
inside that envelope ends the run immediately. Clearing all five accepted moves
restarts the trainer into a fresh game.

### Engine-unavailability behavior

- Opening-book authority is still an explicit seam with no bundled book asset, so the
  default book layer reports itself as unavailable.
- Engine tolerance uses python-chess UCI integration and expects a reachable engine
  binary at the configured path.
- If the engine executable is absent or invalid and book authority does not accept the
  move, the evaluator returns an explicit `AuthorityUnavailable` result.
- The session layer surfaces that condition clearly, pauses the run explicitly, and does
  **not** record it as an ordinary fail.

## Corpus Ingestion Pipeline

The repository now includes a separable ingestion pipeline that converts one or more
local Lichess-style PGN files into a deterministic runtime artifact.

### Initial rating-band policy

The first authoritative eligibility policy is conservative realism:

- target rating band: **475 to 525**
- policy: **both players must be inside the target band**
- missing or malformed rating tags are rejected

### Retained opening depth

The trainer runtime still grades only the user's first five moves, but the ingestion
artifact keeps a broader early opening window:

- retained depth: **16 plies**

### Artifact contract

The first runtime artifact is JSON and preserves:

- schema version
- source file list
- target rating band metadata
- rating eligibility policy
- retained opening depth
- sparse policy metadata
- weighting policy metadata
- deterministic position records keyed by normalized FEN-derived position keys

Each position record preserves:

- `position_key`
- `side_to_move`
- candidate moves keyed by **UCI**
- `raw_count` per move
- `effective_weight` per move
- `total_observed_count`
- sparse annotations

Raw counts are preserved exactly even though effective weights are also stored.
The current shaping policy is intentionally light: `effective_weight = raw_count ** 0.75`.
That suppresses one-off tail dominance without erasing rare moves from the artifact.

### Build an artifact locally

```bash
python -m opening_trainer.corpus.cli path/to/games_a.pgn path/to/games_b.pgn --output data/opening_corpus.json
```

If `--output` is omitted, the default runtime artifact path is:

```text
data/opening_corpus.json
```

## Workspace-Root Runtime Asset Model

The canonical source of truth remains the Git repo checked out in the `repo` folder.
Large local runtime assets are intentionally allowed to live outside Git in the workspace root that contains that repo clone and the outer launcher scripts.

The intended operating model is:

- workspace root contains launcher scripts, logs, and local runtime assets
- repo root contains the canonical committed application code
- local runtime assets are not alternate code branches and should not be treated as canonical source

### Conventional discovery order

Runtime discovery keeps explicit precedence. For each asset class, the winner is selected in this order:

1. explicit CLI flag
2. explicit runtime config file path passed by CLI
3. environment variable
4. workspace-root `runtime.local.json`
5. conventional workspace-root asset path
6. repo-local default path
7. `PATH` lookup where applicable

### Workspace-root defaults

Ordinary launcher-driven runs now auto-discover these workspace-root conventions when present:

- runtime config: `../runtime.local.json`
- Stockfish engine: `../tools/stockfish/stockfish-windows-x86-64-avx2.exe`
- corpus artifact: `../data/opening_corpus.json` or `../artifacts/opening_corpus.json`
- opening book: `../runtime/opening_book.bin`, `../assets/opening_book.bin`, or `../data/opening_book.bin`

If a workspace-root asset is absent, startup stays explicit and degrades cleanly to the existing repo-local fallback doctrine.
Manual CLI flags still override discovered workspace defaults.

## Runtime Opponent Sourcing

At startup, the session now prefers an explicitly selected builder corpus bundle directory before any legacy `opening_corpus.json` path. The supported builder bundle contract for this runtime lane is `manifest.json` plus `data/aggregated_position_move_counts.jsonl`, with `position_key_format=fen_normalized` and `move_key_format=uci`.

- Preferred corpus discovery order: CLI `--corpus-bundle-dir`, runtime config `corpus_bundle_dir`, environment `OPENING_TRAINER_CORPUS_BUNDLE_DIR`, workspace-root `runtime.local.json`, then legacy corpus artifact conventions such as `data/opening_corpus.json`.
- Opponent fallback order is explicit: selected corpus bundle (or legacy corpus artifact if bundle resolution fails), then Stockfish-generated fallback, then random legal move only as the last resort.
- If no artifact exists, the trainer prints a clear message and uses the explicit provisional random fallback provider.

When the corpus-backed provider is active, it:

1. Normalizes the current board to a deterministic position key.
2. Looks up candidate opponent moves by that key.
3. Samples using stored effective weights while preserving raw-count metadata.
4. Falls back through earlier move-stack prefixes when possible.
5. Raises a clear runtime lookup failure if no acceptable corpus-backed move exists.

The runtime provider keeps enough metadata to explain later:

- which move was sampled
- the sampled move's raw count
- its effective weight
- the total position count
- whether the position was sparse
- whether fallback/backoff was applied
- what alternatives existed

## Supported Run Modes

The repository can currently be started in the following ways:

1. `python main.py`
2. `python run_trainer.py`
3. `python -m opening_trainer`
4. `python main.py --cli`
5. `python main.py --gui`

## Not Yet Implemented

The following systems are still out of scope for this lane:

1. Persistent review queues or failure storage.
2. Mastery scheduling.
3. Rich stats UI.
4. Remote or cloud corpus acquisition.
5. Heavy sparse-policy tuning UI.
6. Bundled opening-book assets.
7. Great / Brilliant overlays.

## Local GUI Validation Surface

The default launch path now attempts to open a lightweight local Tkinter board GUI first.
This GUI is intentionally a thin validation surface over the same trainer session and evaluator pipeline used by the CLI.
It is meant to accelerate human opening-validation reps, not to serve as a polished end-user platform UI.

### Launch GUI mode

- `python main.py`
- `python -m opening_trainer`
- `python run_trainer.py`
- `python main.py --gui`

### Launch CLI mode

- `python main.py --cli`
- `python -m opening_trainer --cli`

### GUI behavior notes

- The board uses click source square then click destination square move entry.
- Board orientation follows the randomized assigned player color.
- Console feedback remains active for canonical judgment, overlay label, reason text, and preferred move output.
- If Tkinter is unavailable at runtime, the app prints a clear fallback message and continues in CLI mode.


## Runtime Asset Configuration

Ordinary runs now resolve runtime assets through one shared configuration flow used by both CLI and GUI startup.
You do **not** need to edit source files for local setup.

### Supported runtime assets

- corpus artifact path
- engine executable path
- optional Polyglot opening-book path
- optional engine depth and time-limit overrides
- optional strict-assets mode for fail-fast local validation

### Discovery order

Each asset is resolved in this deterministic order:

1. explicit CLI flag
2. explicit JSON runtime config setting
3. environment variable
4. conventional default path

Winning paths are surfaced in startup diagnostics so local runs never silently switch authorities.

### CLI flags

```bash
python main.py --cli   --corpus-bundle-dir /path/to/artifacts/my_bundle   --corpus-artifact data/opening_corpus.json   --engine-path /path/to/stockfish   --book-path data/opening_book.bin
```

Optional diagnostics/build helpers:

```bash
python main.py --show-runtime
python main.py --build-corpus path/to/games.pgn --build-corpus-output data/opening_corpus.json
```

### JSON runtime config

Create `runtime/runtime_config.json` or point to a file with `--runtime-config`.
Example:

```json
{
  "corpus_bundle_dir": "../artifacts/my_bundle",
  "corpus_artifact_path": "data/opening_corpus.json",
  "engine_executable_path": "/path/to/stockfish",
  "opening_book_path": "data/opening_book.bin",
  "engine_depth": 12,
  "engine_time_limit_seconds": 0.2,
  "strict_assets": false
}
```

### Environment variables

- `OPENING_TRAINER_RUNTIME_CONFIG`
- `OPENING_TRAINER_CORPUS_BUNDLE_DIR`
- `OPENING_TRAINER_CORPUS_PATH`
- `OPENING_TRAINER_ENGINE_PATH`
- `OPENING_TRAINER_BOOK_PATH`
- `OPENING_TRAINER_ENGINE_DEPTH`
- `OPENING_TRAINER_ENGINE_TIME_LIMIT`
- `OPENING_TRAINER_STRICT_ASSETS`

### Conventional default paths

- corpus artifact: `data/opening_corpus.json`
- opening book: `runtime/opening_book.bin`, `assets/opening_book.bin`, then `data/opening_book.bin`
- engine executable: `runtime/engine/stockfish`, `runtime/stockfish`, `assets/stockfish`, then `stockfish` on `PATH`

## Startup Summary and Degraded Mode

Every new run now prints a compact runtime startup summary that reports:

- current mode
- assigned user color
- corpus authority status
- book authority status
- engine authority status
- whether the trainer is fully doctrine-capable or running in declared degraded mode

### Reading the startup messages

- **Corpus bundle loaded**: aggregate-bundle opponent sourcing is active.
- **Legacy corpus loaded**: legacy `opening_corpus.json` opponent sourcing is active for backward compatibility.
- **Corpus unavailable or incompatible**: the trainer reports the exact bundle/corpus failure and falls back to Stockfish before using random legal moves.
- **Book loaded**: moves can pass via actual opening-book membership.
- **Book missing**: no book authority is available, so only engine-backed Better evaluation can approve non-book moves.
- **Engine resolved**: engine tolerance is available for Better evaluation.
- **Engine missing / unavailable**: the trainer pauses with `AuthorityUnavailable` when book does not approve a move.
- **Degraded mode**: one or more runtime authorities are unavailable; the trainer says so explicitly instead of pretending the run is fully configured.

This lane does **not** add review persistence or final evaluation retuning.
