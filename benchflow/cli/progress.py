from __future__ import annotations

import threading
from typing import cast

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.status import Status
from rich.text import Text


class OpsRateColumn(ProgressColumn):
    def render(self, task: Task) -> Text:
        ops = int(task.fields.get("ops", 0))
        ops_per_s = float(task.fields.get("ops_per_s", 0.0))
        return Text(f"{ops:,} ops • {ops_per_s:,.0f} ops/s")


class RichRunProgress:
    def __init__(self, console: Console) -> None:
        self._console = console
        self._status: Status | None = None
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None
        self._last_measurement_duration = 0.0
        self._lock = threading.Lock()

    def on_iteration_start(self, iteration: int, total: int) -> None:
        with self._lock:
            self._clear_live()
            self._console.print(f"[bold]Iteration {iteration}/{total}[/bold]")

    def on_target_start(
        self,
        target_name: str,
        stack_id: str,
        target_idx: int,
        total_targets: int,
    ) -> None:
        with self._lock:
            self._clear_live()
            panel = Panel.fit(
                f"Running {target_name} [{target_idx}/{total_targets} targets]",
                border_style="cyan",
            )
            self._console.print(panel)

    def on_setup_start(self) -> None:
        with self._lock:
            self._set_status("Preparing setup queries...", spinner="dots")

    def on_setup_done(self) -> None:
        with self._lock:
            self._clear_status()

    def on_warmup_start(self, target_name: str, duration_s: int) -> None:
        with self._lock:
            self._set_status(f"⏳ Warming up {target_name} for {duration_s}s...", spinner="dots")

    def on_warmup_done(self) -> None:
        with self._lock:
            self._clear_status()

    def on_measurement_start(self, target_name: str, duration_s: int) -> None:
        with self._lock:
            self._clear_status()
            self._clear_progress()
            self._progress = Progress(
                TextColumn("[bold cyan]Measuring[/bold cyan] {task.description}"),
                TimeElapsedColumn(),
                BarColumn(),
                OpsRateColumn(),
                TimeRemainingColumn(),
                console=self._console,
            )
            self._progress.start()
            self._task_id = cast(
                TaskID,
                self._progress.add_task(
                    target_name,
                    total=duration_s,
                    completed=0,
                    ops=0,
                    ops_per_s=0.0,
                ),
            )

    def on_measurement_tick(self, elapsed_s: float, total_ops: int) -> None:
        with self._lock:
            if self._progress is None or self._task_id is None:
                return
            task = self._progress.tasks[self._task_id]
            total = float(task.total or 0.0)
            completed = min(elapsed_s, total) if total > 0 else elapsed_s
            ops_per_s = total_ops / elapsed_s if elapsed_s > 0 else 0.0
            self._progress.update(
                cast(TaskID, self._task_id),
                completed=completed,
                ops=total_ops,
                ops_per_s=ops_per_s,
            )
            self._progress.refresh()

    def on_measurement_done(self, total_ops: int, duration_s: float) -> None:
        with self._lock:
            self._last_measurement_duration = duration_s
            self._clear_progress()

    def on_target_done(self, stack_id: str, total_ops: int, status: str) -> None:
        with self._lock:
            self._clear_live()
            marker = "✓" if status == "ok" else "✗"
            duration_s = self._last_measurement_duration
            ops_per_s = total_ops / duration_s if duration_s > 0 else 0.0
            self._console.print(
                f"{marker} {stack_id}: {total_ops:,} ops ({ops_per_s:,.0f} ops/s)",
            )

    def on_teardown_start(self) -> None:
        with self._lock:
            self._set_status("Cleaning up teardown queries...", spinner="dots")

    def on_teardown_done(self) -> None:
        with self._lock:
            self._clear_status()

    def on_iteration_done(self, iteration: int) -> None:
        with self._lock:
            self._clear_live()

    def on_pause(self, duration_s: float) -> None:
        with self._lock:
            self._set_status(f"⏸ Pausing {duration_s:.1f}s between iterations...", spinner="dots")

    def _set_status(self, message: str, spinner: str = "dots") -> None:
        self._clear_live()
        self._status = self._console.status(message, spinner=spinner)
        self._status.start()

    def _clear_status(self) -> None:
        if self._status is not None:
            self._status.stop()
            self._status = None

    def _clear_progress(self) -> None:
        if self._progress is not None:
            self._progress.stop()
            self._progress = None
            self._task_id = None

    def _clear_live(self) -> None:
        self._clear_status()
        self._clear_progress()
