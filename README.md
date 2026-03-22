Opening Trainer

A local chess opening-discipline trainer focused on rapid early-game repetition.

## Project Goal

The trainer enforces move-quality discipline during the first five player moves.
Runs fail immediately when a move is rejected and then restart into a fresh game.
Runs succeed after five accepted player moves and then also restart immediately.

## Current Stage

Checkpoint v2
Sprint Step 1 baseline with graded overlay labels

## Current Implemented Behavior

The application currently provides:

1. Local chess board state using python-chess.
2. Legal move validation before evaluation.
3. Randomized player color at game start.
4. Explicit session states for player turn, opponent turn, fail, success, and restart.
5. Provisional random opponent move generation.
6. Structured move evaluation through explicit book and engine authority seams.
7. Compact evaluation feedback with canonical judgment, overlay label, and reason text.
8. Immediate console flushing so wrapper-launched sessions remain visible.

## Evaluator Baseline

### Canonical model

The evaluator resolves every legal player move inside the active envelope using a
strict authority stack:

1. **Book** if an explicit opening-book authority approves the move.
2. **Better** if book does not approve it but engine tolerance still accepts it.
3. **Fail** if neither authority accepts it.

Book / Better / Fail remain the canonical internal judgment. Overlay labels are
user-facing metadata layered on top of that result.

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

Pass-compatible overlays are Book, Best, Excellent, and Good.
Fail-compatible overlays are Inaccuracy, Mistake, Blunder, and MissedWin.

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

The current training envelope is the player’s first five moves. A failed move
inside that envelope ends the run immediately. Clearing all five accepted moves
restarts the trainer into a fresh game.

### MissedWin guardrail

`MissedWin` is intentionally level-aware. The current implementation only raises
that label when the engine reports a forced mate for the player within the
configured short mate horizon cap and the played move throws that win away.
Deeper wins are not promoted to `MissedWin` yet.

### Runtime requirements

- Opening book authority is currently an explicit seam with no bundled book
  asset, so the default book layer reports itself as unavailable.
- Engine tolerance uses python-chess UCI integration and expects a reachable
  engine binary at the configured path. If the engine is unavailable at runtime,
  the evaluator returns explicit unavailability metadata instead of crashing.

## Supported Run Modes

The repository can currently be started in the following ways:

1. `python main.py`
2. `python run_trainer.py`

## Not Yet Implemented

The following systems are still out of scope for this lane:

1. Real low-ELO corpus-based opponent move sampling.
2. Bundled opening-book assets or corpus ingestion.
3. Persistent review queues or failure storage.
4. Advanced UI.
5. Great / Brilliant overlays.
