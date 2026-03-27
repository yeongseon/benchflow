"""Worker for the legacy CUBRID-Python (CUBRIDdb) C extension driver."""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from benchflow.core.scenario.schema import Scenario, Step
from benchflow.workers.protocol import Worker, WorkerFactory, register_worker

logger = logging.getLogger(__name__)

_PYFORMAT_RE = re.compile(r"%\((\w+)\)s")


def _build_cubrid_url(dsn: str) -> str:
    """Convert cubrid://user:password@host:port/database → CUBRID:host:port:database:user:password:."""
    parsed = urlparse(dsn)
    host = parsed.hostname or "localhost"
    port = parsed.port or 33000
    database = (parsed.path or "/benchdb").lstrip("/")
    user = parsed.username or "dba"
    password = parsed.password or ""
    return f"CUBRID:{host}:{port}:{database}:{user}:{password}:"


def _translate_query(query: str, params: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """Convert %(name)s placeholders to ? and return ordered param tuple."""
    ordered: list[Any] = []

    def replacer(match: re.Match[str]) -> str:
        ordered.append(params[match.group(1)])
        return "?"

    translated = _PYFORMAT_RE.sub(replacer, query)
    return translated, tuple(ordered)


class CUBRIDdbWorker(Worker):
    """Benchmark worker using the legacy CUBRID-Python C extension driver (CUBRIDdb)."""

    def __init__(self) -> None:
        self._cubrid_url: str = ""
        self._conn: Any = None

    def setup(self, *, dsn: str, worker_config: dict[str, Any], scenario: Scenario) -> None:
        self._cubrid_url = _build_cubrid_url(dsn)

    def open(self) -> None:
        import CUBRIDdb

        self._conn = CUBRIDdb.connect(self._cubrid_url)
        self._conn.set_autocommit(False)

    def execute(self, step: Step) -> None:
        assert self._conn is not None
        params = step.resolve_params()
        cursor = self._conn.cursor()
        try:
            if params:
                query, ordered_params = _translate_query(step.query, params)
                cursor.execute(query, ordered_params)
            else:
                cursor.execute(step.query)
            if cursor.description is not None:
                cursor.fetchall()
        finally:
            cursor.close()

    def execute_raw(self, query: str) -> None:
        """Execute a raw SQL query for setup/teardown."""
        assert self._conn is not None
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
            self._conn.commit()
        finally:
            cursor.close()

    def introspect(self) -> dict[str, Any]:
        """Return CUBRID server version."""
        assert self._conn is not None
        info: dict[str, Any] = {}
        try:
            cursor = self._conn.cursor()
            try:
                cursor.execute("SELECT version()")
                row = cursor.fetchone()
                if row:
                    info["server_version"] = row[0]
            finally:
                cursor.close()
        except Exception as exc:
            logger.debug("introspect() failed: %s", exc)
        return info

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class CUBRIDdbWorkerFactory(WorkerFactory):
    def create(self, thread_index: int) -> CUBRIDdbWorker:
        return CUBRIDdbWorker()


register_worker("python+cubriddb", CUBRIDdbWorkerFactory)
