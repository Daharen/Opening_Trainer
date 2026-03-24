from __future__ import annotations

import queue
import tkinter as tk
from tkinter import ttk

from ..session_logging import SessionLogger


class DevConsoleWindow:
    def __init__(self, root: tk.Misc, logger: SessionLogger):
        self._root = root
        self._logger = logger
        self._window: tk.Toplevel | None = None
        self._text: tk.Text | None = None
        self._queue: queue.Queue[str] = queue.Queue(maxsize=5000)
        self._unsubscribe = None
        self._autoscroll = tk.BooleanVar(value=True)
        self._poll_handle = None

    def open(self) -> None:
        if self._window is not None and self._window.winfo_exists():
            self._window.deiconify()
            self._window.lift()
            return
        self._window = tk.Toplevel(self._root)
        self._window.title("Developer Console")
        self._window.geometry("920x420")
        self._window.protocol("WM_DELETE_WINDOW", self.close)

        controls = ttk.Frame(self._window)
        controls.pack(fill="x", padx=8, pady=(8, 4))
        ttk.Checkbutton(controls, text="Auto-scroll", variable=self._autoscroll).pack(side="left")

        text = tk.Text(self._window, wrap="none", height=22)
        text.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        text.configure(state="disabled")
        self._text = text

        bootstrap = self._logger.bootstrap_lines()
        if bootstrap:
            self._append_lines(list(bootstrap))

        self._unsubscribe = self._logger.subscribe(self._on_log_line)
        self._poll_handle = self._root.after(50, self._drain_queue)

    def _on_log_line(self, line: str) -> None:
        try:
            self._queue.put_nowait(line)
        except queue.Full:
            return

    def _drain_queue(self) -> None:
        lines: list[str] = []
        for _ in range(400):
            try:
                lines.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if lines:
            self._append_lines(lines)
        if self._window is not None and self._window.winfo_exists():
            self._poll_handle = self._root.after(50, self._drain_queue)

    def _append_lines(self, lines: list[str]) -> None:
        if self._text is None:
            return
        self._text.configure(state="normal")
        self._text.insert("end", "\n".join(lines) + "\n")
        self._text.configure(state="disabled")
        if self._autoscroll.get():
            self._text.see("end")

    def close(self) -> None:
        if self._poll_handle is not None:
            self._root.after_cancel(self._poll_handle)
            self._poll_handle = None
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        if self._window is not None and self._window.winfo_exists():
            self._window.destroy()
        self._window = None
        self._text = None
