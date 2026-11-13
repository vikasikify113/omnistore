"""PostgreSQL connection pool and small helpers."""
from __future__ import annotations

from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

TABLES = [
    "pipeline_log", "notifications", "outbox", "order_items", "orders",
    "cart_items", "carts", "customers", "inventory", "products",
]


def make_pool(dsn: str, max_size: int = 8, min_size: int = 1, name: str = "omnistore") -> ConnectionPool:
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set. Copy .env.example to .env and fill it in.")
    pool = ConnectionPool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        open=False,
        name=name,
        timeout=20,
        kwargs={"row_factory": dict_row},
        check=ConnectionPool.check_connection,  # drops connections the server closed while idle
    )
    pool.open(wait=True, timeout=30)
    return pool


def apply_schema(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        conn.execute(SCHEMA_FILE.read_text(encoding="utf-8"))


def drop_all(pool: ConnectionPool) -> None:
    with pool.connection() as conn:
        for table in TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


def log_pipeline(conn, service: str, event_type: str, ref: str, action: str) -> None:
    """Write one row of the live pipeline view. Always called inside the caller's transaction."""
    conn.execute(
        "INSERT INTO pipeline_log (service, event_type, ref, action) VALUES (%s, %s, %s, %s)",
        (service, event_type, ref, action),
    )
