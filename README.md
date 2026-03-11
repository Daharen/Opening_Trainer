Opening Trainer

A local chess opening-discipline trainer focused on rapid early-game repetition.

1. Project Goal

The trainer enforces move-quality discipline during the first five player moves.
Runs fail immediately when a move is rejected and then restart into a fresh game.
Runs succeed after five accepted player moves and then also restart immediately.

2. Current Stage

Checkpoint v1
Sprint 1 Step 2.6
Non-interactive console visibility baseline

3. Current Implemented Behavior

The application currently provides:

1. Local chess board state using python-chess
2. Legal move validation
3. Randomized player color at game start
4. Explicit session states for player turn, opponent turn, fail, success, and restart
5. Provisional opponent move generation
6. Provisional evaluator contract with transparent reasoning output
7. Immediate console flushing so wrapper-launched sessions remain visible

4. Supported Run Modes

The repository can currently be started in the following ways:

1. python main.py
2. python run_trainer.py

5. Notes

This project currently uses a console interface rather than a GUI window.
Startup output and prompts are flushed explicitly so the trainer remains visible
when launched by wrapper scripts or other non-interactive hosts.

6. Not Yet Implemented

The following systems are still provisional:

1. Real low-ELO corpus-based opponent move sampling
2. Opening-book membership checks
3. Engine-based Better tolerance evaluation
4. Tuned fail thresholds
5. Rich UI and feedback presentation
