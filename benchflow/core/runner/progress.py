from __future__ import annotations

from typing import Protocol


class RunProgress(Protocol):
    def on_iteration_start(self, iteration: int, total: int) -> None: ...

    def on_target_start(
        self,
        target_name: str,
        stack_id: str,
        target_idx: int,
        total_targets: int,
    ) -> None: ...

    def on_setup_start(self) -> None: ...

    def on_setup_done(self) -> None: ...

    def on_warmup_start(self, target_name: str, duration_s: int) -> None: ...

    def on_warmup_done(self) -> None: ...

    def on_measurement_start(self, target_name: str, duration_s: int) -> None: ...

    def on_measurement_tick(self, elapsed_s: float, total_ops: int) -> None: ...

    def on_measurement_done(self, total_ops: int, duration_s: float) -> None: ...

    def on_target_done(self, stack_id: str, total_ops: int, status: str) -> None: ...

    def on_teardown_start(self) -> None: ...

    def on_teardown_done(self) -> None: ...

    def on_iteration_done(self, iteration: int) -> None: ...

    def on_pause(self, duration_s: float) -> None: ...


class NullProgress:
    def on_iteration_start(self, iteration: int, total: int) -> None:
        pass

    def on_target_start(
        self,
        target_name: str,
        stack_id: str,
        target_idx: int,
        total_targets: int,
    ) -> None:
        pass

    def on_setup_start(self) -> None:
        pass

    def on_setup_done(self) -> None:
        pass

    def on_warmup_start(self, target_name: str, duration_s: int) -> None:
        pass

    def on_warmup_done(self) -> None:
        pass

    def on_measurement_start(self, target_name: str, duration_s: int) -> None:
        pass

    def on_measurement_tick(self, elapsed_s: float, total_ops: int) -> None:
        pass

    def on_measurement_done(self, total_ops: int, duration_s: float) -> None:
        pass

    def on_target_done(self, stack_id: str, total_ops: int, status: str) -> None:
        pass

    def on_teardown_start(self) -> None:
        pass

    def on_teardown_done(self) -> None:
        pass

    def on_iteration_done(self, iteration: int) -> None:
        pass

    def on_pause(self, duration_s: float) -> None:
        pass
