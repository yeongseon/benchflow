from __future__ import annotations

import logging
import re
from typing import Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from benchflow.core.scenario.schema import Scenario, Step
from benchflow.workers.protocol import Worker, WorkerFactory, register_worker

logger = logging.getLogger(__name__)

_PYFORMAT_RE = re.compile(r"%\((\w+)\)s")



class SQLAlchemyWorker(Worker):
    _shared_engine: Engine | None = None

    def __init__(self) -> None:
        self._dsn: str = ""
        self._conn: Connection | None = None

    def setup(self, *, dsn: str, worker_config: dict[str, Any], scenario: Scenario) -> None:
        self._dsn = dsn

    def open(self) -> None:
        if SQLAlchemyWorker._shared_engine is None:
            SQLAlchemyWorker._shared_engine = create_engine(self._dsn)
        self._conn = SQLAlchemyWorker._shared_engine.connect()

    def execute(self, step: Step) -> None:
        assert self._conn is not None
        params = step.resolve_params()
        # Translate psycopg %(name)s placeholders to SQLAlchemy :name style
        sa_query = _PYFORMAT_RE.sub(r":\1", step.query)
        result = self._conn.execute(text(sa_query), params or {})
        result.fetchall()

    def execute_raw(self, query: str) -> None:
        """Execute a raw SQL query for setup/teardown."""
        assert self._conn is not None
        self._conn.execute(text(query))
        self._conn.commit()

    def introspect(self) -> dict[str, Any]:
        """Return database server version via SQLAlchemy dialect."""
        assert self._conn is not None
        info: dict[str, Any] = {}
        try:
            result = self._conn.execute(text("SELECT version()"))
            row = result.fetchone()
            if row:
                info["server_version"] = row[0]
        except Exception as exc:
            logger.debug("introspect() failed: %s", exc)
        return info

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class SQLAlchemyWorkerFactory(WorkerFactory):
    def create(self, thread_index: int) -> SQLAlchemyWorker:
        return SQLAlchemyWorker()


register_worker("python+sqlalchemy", SQLAlchemyWorkerFactory)
