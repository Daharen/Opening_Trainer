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

## Runtime Opponent Sourcing

At startup, the session attempts to load `data/opening_corpus.json`.

- If the artifact exists, the trainer uses the corpus-backed opponent provider.
- If the artifact does not exist, the trainer prints a clear message and uses the
  explicit provisional random fallback provider.

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
