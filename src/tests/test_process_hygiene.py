from __future__ import annotations

from opening_trainer.process_hygiene import ProcessCleanupCoordinator


class _FakeProcess:
    def __init__(self, pid: int, exited: bool = False):
        self.pid = pid
        self._exited = exited
        self.terminate_calls = 0

    def poll(self):
        return 0 if self._exited else None

    def terminate(self):
        self.terminate_calls += 1


def test_cleanup_coordinator_terminates_registered_running_processes():
    messages: list[str] = []
    coordinator = ProcessCleanupCoordinator(lambda message, tag="startup": messages.append(f"{tag}:{message}"))
    process = _FakeProcess(pid=2001, exited=False)

    coordinator.register(process, label="test_helper")
    coordinator.cleanup()

    assert process.terminate_calls == 1
    assert any("PROCESS_OWNERSHIP_REGISTERED" in message for message in messages)
    assert any("PROCESS_OWNERSHIP_TERMINATE" in message for message in messages)


def test_cleanup_coordinator_skips_exited_processes():
    coordinator = ProcessCleanupCoordinator(lambda message, tag="startup": None)
    process = _FakeProcess(pid=2002, exited=True)

    coordinator.register(process, label="already_done")
    coordinator.cleanup()

    assert process.terminate_calls == 0
