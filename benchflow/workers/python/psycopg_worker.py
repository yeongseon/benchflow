from __future__ import annotations

import logging
from typing import Any

import psycopg

from benchflow.core.scenario.schema import Scenario, Step
from benchflow.workers.protocol import Worker, WorkerFactory, register_worker

logger = logging.getLogger(__name__)


class PsycopgWorker(Worker):
    def __init__(self) -> None:
        self._dsn: str = ""
        self._conn: psycopg.Connection | None = None

    def setup(self, *, dsn: str, worker_config: dict[str, Any], scenario: Scenario) -> None:
        self._dsn = dsn

    def open(self) -> None:
        self._conn = psycopg.connect(self._dsn, autocommit=True)

    def execute(self, step: Step) -> None:
        assert self._conn is not None
        params = step.resolve_params()
        with self._conn.cursor() as cur:
            cur.execute(step.query, params or None)
            cur.fetchall()

    def execute_raw(self, query: str) -> None:
        """Execute a raw SQL query for setup/teardown."""
        assert self._conn is not None
        with self._conn.cursor() as cur:
            cur.execute(query)

    def introspect(self) -> dict[str, Any]:
        """Return PostgreSQL server version and key settings."""
        assert self._conn is not None
        info: dict[str, Any] = {}
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT version()")
                row = cur.fetchone()
                if row:
                    info["server_version"] = row[0]

                # Capture key performance-relevant settings
                _settings_of_interest = [
                    "shared_buffers",
                    "work_mem",
                    "effective_cache_size",
                    "max_connections",
                    "random_page_cost",
                    "effective_io_concurrency",
                    "max_parallel_workers_per_gather",
                    "wal_level",
                    "synchronous_commit",
                    "checkpoint_completion_target",
                ]
                config: dict[str, str] = {}
                for setting in _settings_of_interest:
                    try:
                        cur.execute("SHOW %s" % setting)  # noqa: S608 — setting names are hardcoded
                        row = cur.fetchone()
                        if row:
                            config[setting] = row[0]
                    except Exception:
                        pass
                if config:
                    info["server_config"] = config
        except Exception as exc:
            logger.debug("introspect() failed: %s", exc)
        return info

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class PsycopgWorkerFactory(WorkerFactory):
    def create(self, thread_index: int) -> PsycopgWorker:
        return PsycopgWorker()


register_worker("python+psycopg", PsycopgWorkerFactory)
