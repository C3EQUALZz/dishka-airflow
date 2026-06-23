"""Pure domain value objects (no framework dependencies)."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Greeting:
    recipient: str
    message: str


@dataclass(frozen=True)
class TaskReport:
    task_id: str
    run_id: str

    def describe(self) -> str:
        return f"task={self.task_id} run={self.run_id}"
