from __future__ import annotations

from dataclasses import dataclass
from subprocess import Popen
from typing import Callable


@dataclass
class OwnedProcess:
    process: Popen
    label: str
    terminate_on_shutdown: bool = True


class ProcessCleanupCoordinator:
    def __init__(self, logger: Callable[[str, str], None]) -> None:
        self._logger = logger
        self._owned: list[OwnedProcess] = []

    def register(self, process: Popen, *, label: str, terminate_on_shutdown: bool = True) -> None:
        self._owned.append(OwnedProcess(process=process, label=label, terminate_on_shutdown=terminate_on_shutdown))
        self._logger(f"PROCESS_OWNERSHIP_REGISTERED label={label} pid={getattr(process, 'pid', 'unknown')}", "startup")

    def cleanup(self) -> None:
        still_owned: list[OwnedProcess] = []
        for owned in self._owned:
            proc = owned.process
            poll = getattr(proc, "poll", None)
            exited = callable(poll) and poll() is not None
            if exited or not owned.terminate_on_shutdown:
                continue
            try:
                proc.terminate()
                self._logger(
                    f"PROCESS_OWNERSHIP_TERMINATE label={owned.label} pid={getattr(proc, 'pid', 'unknown')} result=issued",
                    "startup",
                )
            except Exception as exc:  # noqa: BLE001
                self._logger(
                    f"PROCESS_OWNERSHIP_TERMINATE label={owned.label} pid={getattr(proc, 'pid', 'unknown')} result=failed error={exc}",
                    "error",
                )
                still_owned.append(owned)
        self._owned = still_owned
