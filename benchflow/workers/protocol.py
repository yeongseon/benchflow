"""Worker protocol — abstract interface for benchmark execution targets."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from benchflow.core.scenario.schema import Scenario, Step


class Worker(ABC):
    """
    Lifecycle-managed benchmark worker.

    Each thread gets its own Worker instance. Connection sharing across
    threads is forbidden — open() must create thread-local resources.

    Lifecycle: setup() → open() → [warmup() →] execute()* → close()
    """

    @abstractmethod
    def setup(self, *, dsn: str, worker_config: dict[str, Any], scenario: Scenario) -> None:
        """Initialize worker configuration (called once before open)."""

    @abstractmethod
    def open(self) -> None:
        """Establish database connection / session (thread-local)."""

    def warmup(self, steps: list[Step], duration_s: int) -> None:
        """Run warmup iterations (default: just call execute in a loop)."""
        import time

        deadline = time.perf_counter() + duration_s
        while time.perf_counter() < deadline:
            for step in steps:
                self.execute(step)

    @abstractmethod
    def execute(self, step: Step) -> None:
        """Execute a single query step. Runner measures latency externally."""

    def execute_raw(self, query: str) -> None:
        """Execute a raw SQL query (for setup/teardown).

        Default implementation raises NotImplementedError.
        Workers that support setup/teardown should override this.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement execute_raw(). "
            "Override this method to support setup/teardown queries."
        )

    def introspect(self) -> dict[str, Any]:
        """Return server metadata (version, config) after connection is open.

        Default returns empty dict. Workers should override to provide
        server version and configuration for reproducibility metadata.
        """
        return {}

    @abstractmethod
    def close(self) -> None:
        """Release database connection / session resources."""

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class WorkerFactory(ABC):
    """Creates Worker instances — one per concurrent thread."""

    @abstractmethod
    def create(self, thread_index: int) -> Worker:
        """Create a new worker for the given thread index."""


WORKER_REGISTRY: dict[str, type[WorkerFactory]] = {}


def register_worker(stack_id: str, factory_cls: type[WorkerFactory]) -> None:
    WORKER_REGISTRY[stack_id] = factory_cls


def get_worker_factory(stack_id: str) -> type[WorkerFactory]:
    if stack_id not in WORKER_REGISTRY:
        available = ", ".join(WORKER_REGISTRY.keys()) or "(none)"
        raise KeyError(f"Unknown worker stack_id: {stack_id!r}. Available: {available}")
    return WORKER_REGISTRY[stack_id]
