"""Connection pool for parallel worker threads.

Uses psycopg_pool.ConnectionPool to bound the number of simultaneous
database connections and reuse them across worker threads instead of
opening/closing a fresh connection per worker.
"""

from __future__ import annotations

import logging

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


def create_pool(dsn: str, min_size: int = 4, max_size: int = 32) -> ConnectionPool:
    """Create a new ConnectionPool.

    Each connection is configured with dict_row and the signals search_path,
    matching the behaviour of ``db.get_connection()``.
    """

    def _configure_conn(conn):
        conn.autocommit = False
        conn.row_factory = dict_row
        conn.execute("SET search_path = signals, public")

    pool = ConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        configure=_configure_conn,
    )
    logger.info("db_pool_created min_size=%d max_size=%d", min_size, max_size)
    return pool
