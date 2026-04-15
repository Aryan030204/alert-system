"""
db.py — Thin DB connection wrapper using mysql-connector-python.
"""

from __future__ import annotations
import logging
from contextlib import contextmanager
from typing import Any

import mysql.connector
from mysql.connector import Error as MySQLError

logger = logging.getLogger(__name__)


@contextmanager
def get_connection(db_cfg: dict):
    """
    Context manager that yields an open MySQL connection and closes it cleanly.

    Usage:
        with get_connection(brand_cfg["db"]) as conn:
            ...
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=db_cfg["host"],
            port=db_cfg.get("port", 3306),
            user=db_cfg["user"],
            password=db_cfg["password"],
            database=db_cfg["database"],
            connection_timeout=30,
            autocommit=True,
        )
        logger.debug("Connected to %s/%s", db_cfg["host"], db_cfg["database"])
        yield conn
    except MySQLError as exc:
        logger.error(
            "DB connection failed for %s/%s: %s",
            db_cfg["host"], db_cfg["database"], exc
        )
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()


def run_query(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """
    Execute *sql* with *params* and return all rows as a list of dicts.
    """
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return rows
    except MySQLError as exc:
        logger.error("Query failed: %s | params=%s | error=%s", sql[:120], params, exc)
        raise
    finally:
        cursor.close()