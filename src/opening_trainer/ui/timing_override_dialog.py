from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..developer_timing import DeveloperTimingOverrideState


class TimingOverrideDialog:
    CLOCK_BUCKETS = ["Auto", "comfortable", "medium", "low", "critical"]
    THINK_BUCKETS = ["Auto", "none", "instant", "short", "medium", "long"]
    OPENING_BANDS = ["Auto", "01-10", "11-20", "21-30", "31+"]
    DELAY_SCALES = ["0.25", "0.5", "1.0", "2.0"]

    def __init__(self, root: tk.Misc, session):
        self.root = root
        self.session = session
        self.window: tk.Toplevel | None = None
        self.controls: dict[str, tk.StringVar] = {}
        self.comboboxes: dict[str, ttk.Combobox] = {}
        self.enabled_var = tk.BooleanVar(value=False)
        self.force_ordinary_var = tk.BooleanVar(value=False)
        self.diagnostics_var = tk.StringVar(value="")

    def open(self) -> None:
        if self.window is not None and self.window.winfo_exists():
            self.refresh()
            self.window.deiconify()
            self.window.lift()
            return
        self.window = tk.Toplevel(self.root)
        self.window.title("Developer Timing Override")
        self.window.geometry("780x560")
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        frame = ttk.Frame(self.window, padding=10)
        frame.pack(fill="both", expand=True)
        frame.columnconfigure(1, weight=1)
        ttk.Label(frame, text="Developer/test-only timing override panel.", font=("TkDefaultFont", 10, "bold")).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 8))
        ttk.Checkbutton(frame, text="Enable timing overrides", variable=self.enabled_var).grid(row=1, column=0, columnspan=3, sticky="w")
        ttk.Checkbutton(frame, text="Force ordinary corpus play (bypass review predecessor path)", variable=self.force_ordinary_var).grid(row=2, column=0, columnspan=3, sticky="w", pady=(0, 8))

        row = 3
        row = self._combo(frame, row, "Force time control", "force_time_control_id", ["Auto"])
        row = self._combo(frame, row, "Force mover ELO band", "force_mover_elo_band", ["Auto"])
        row = self._combo(frame, row, "Force clock pressure bucket", "force_clock_pressure_bucket", self.CLOCK_BUCKETS)
        row = self._combo(frame, row, "Force prev opp think bucket", "force_prev_opp_think_bucket", self.THINK_BUCKETS)
        row = self._combo(frame, row, "Force opening ply band", "force_opening_ply_band", self.OPENING_BANDS)
        row = self._combo(frame, row, "Visible delay scale", "visible_delay_scale", self.DELAY_SCALES)
        row = self._entry(frame, row, "Visible delay min seconds", "visible_delay_min_seconds")
        row = self._entry(frame, row, "Visible delay max seconds", "visible_delay_max_seconds")

        actions = ttk.Frame(frame)
        actions.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(10, 8))
        ttk.Button(actions, text="Apply", command=self._apply).pack(side="left")
        ttk.Button(actions, text="Reset to Auto / Disable", command=self._reset).pack(side="left", padx=(8, 0))
        ttk.Button(actions, text="Refresh diagnostics", command=self.refresh).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="Live timing diagnostics", font=("TkDefaultFont", 10, "bold")).grid(row=row, column=0, columnspan=3, sticky="w", pady=(4, 4))
        row += 1
        ttk.Label(frame, textvariable=self.diagnostics_var, justify="left", anchor="w", wraplength=740).grid(row=row, column=0, columnspan=3, sticky="nsew")
        frame.rowconfigure(row, weight=1)
        self.refresh()

    def _combo(self, frame, row: int, label: str, key: str, values: list[str]) -> int:
        ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=2)
        var = tk.StringVar(value=values[0] if values else "Auto")
        self.controls[key] = var
        combo = ttk.Combobox(frame, textvariable=var, values=values, state="normal")
        combo.grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        self.comboboxes[key] = combo
        return row + 1

    def _entry(self, frame, row: int, label: str, key: str) -> int:
        ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=2)
        var = tk.StringVar(value="")
        self.controls[key] = var
        ttk.Entry(frame, textvariable=var).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        return row + 1

    def refresh(self) -> None:
        state = self.session.developer_timing_overrides
        dimensions = self.session.overlay_key_dimensions()
        self.enabled_var.set(state.enabled)
        self.force_ordinary_var.set(state.force_ordinary_corpus_play)
        self._set_combo_values("force_time_control_id", ["Auto", *dimensions["time_control_id"]], state.force_time_control_id)
        self._set_combo_values("force_mover_elo_band", ["Auto", *dimensions["mover_elo_band"]], state.force_mover_elo_band)
        self.controls["force_clock_pressure_bucket"].set(state.force_clock_pressure_bucket)
        self.controls["force_prev_opp_think_bucket"].set(state.force_prev_opp_think_bucket)
        self.controls["force_opening_ply_band"].set(state.force_opening_ply_band)
        self.controls["visible_delay_scale"].set(str(state.visible_delay_scale))
        self.controls["visible_delay_min_seconds"].set("" if state.visible_delay_min_seconds is None else str(state.visible_delay_min_seconds))
        self.controls["visible_delay_max_seconds"].set("" if state.visible_delay_max_seconds is None else str(state.visible_delay_max_seconds))
        d = self.session.timing_diagnostics
        self.diagnostics_var.set(
            f"Bundle path: {d.bundle_path or 'n/a'}\n"
            f"Overlay source: {d.overlay_source}\n"
            f"Overlay availability: {d.overlay_available}\n"
            f"Raw native context components: {d.raw_runtime_context_components.get('native') if isinstance(d.raw_runtime_context_components, dict) else 'n/a'}\n"
            f"Override-adjusted context components: {d.raw_runtime_context_components.get('override_adjusted') if isinstance(d.raw_runtime_context_components, dict) else 'n/a'}\n"
            f"Effective context key: {d.effective_context_key or 'n/a'}\n"
            f"Fallback keys attempted: {list(d.fallback_keys_attempted)}\n"
            f"Matched context key: {d.matched_context_key or 'n/a'}\n"
            f"Lookup mode: {d.lookup_mode}\n"
            f"Bundle invariant time control: {d.bundle_invariant_time_control_id or 'n/a'}\n"
            f"Bundle invariant rating scope: {d.bundle_invariant_rating_band or 'n/a'}\n"
            f"Invariants ignored for overlay match: {d.invariants_ignored_for_match}\n"
            f"Fallback used: {d.fallback_used}\n"
            f"Move-pressure profile: {d.move_pressure_profile_id or 'n/a'}\n"
            f"Think-time profile: {d.think_time_profile_id or 'n/a'}\n"
            f"Sampled think time: {d.sampled_think_time_seconds if d.sampled_think_time_seconds is not None else 'n/a'}\n"
            f"Visible delay applied: {d.visible_delay_applied_seconds if d.visible_delay_applied_seconds is not None else 'none'}\n"
            f"Visible delay reason: {d.visible_delay_reason}\n"
            f"Last opponent source: {d.last_opponent_source or 'n/a'}\n"
            f"Review predecessor bypassed by override: {d.review_predecessor_bypassed}"
        )

    def _set_combo_values(self, key: str, values: list[str], current_value: str) -> None:
        combo = self.comboboxes.get(key)
        if current_value not in values:
            values.append(current_value)
        if combo is not None:
            combo.configure(values=values)
        self.controls[key].set(current_value)

    def _apply(self) -> None:
        state = DeveloperTimingOverrideState(
            enabled=self.enabled_var.get(),
            force_time_control_id=self.controls["force_time_control_id"].get() or "Auto",
            force_mover_elo_band=self.controls["force_mover_elo_band"].get() or "Auto",
            force_clock_pressure_bucket=self.controls["force_clock_pressure_bucket"].get() or "Auto",
            force_prev_opp_think_bucket=self.controls["force_prev_opp_think_bucket"].get() or "Auto",
            force_opening_ply_band=self.controls["force_opening_ply_band"].get() or "Auto",
            force_ordinary_corpus_play=self.force_ordinary_var.get(),
            visible_delay_scale=float(self.controls["visible_delay_scale"].get() or "1.0"),
            visible_delay_min_seconds=(float(self.controls["visible_delay_min_seconds"].get()) if self.controls["visible_delay_min_seconds"].get().strip() else None),
            visible_delay_max_seconds=(float(self.controls["visible_delay_max_seconds"].get()) if self.controls["visible_delay_max_seconds"].get().strip() else None),
        )
        self.session.update_developer_timing_overrides(state)
        self.refresh()

    def _reset(self) -> None:
        self.session.reset_developer_timing_overrides()
        self.refresh()

    def close(self) -> None:
        if self.window is not None and self.window.winfo_exists():
            self.window.destroy()
        self.window = None
