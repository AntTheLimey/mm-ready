"""Database connection management."""

from __future__ import annotations

import os
import psycopg2
import psycopg2.extras


def connect(
    host: str | None = None,
    port: int | None = None,
    dbname: str | None = None,
    user: str | None = None,
    password: str | None = None,
    dsn: str | None = None,
) -> psycopg2.extensions.connection:
    """Create a database connection from explicit args or a DSN string.

    CLI args take precedence over DSN components if both are provided.
    Falls back to standard PG* environment variables.
    """
    if dsn:
        conn = psycopg2.connect(dsn)
    else:
        params = {}
        if host:
            params["host"] = host
        if port:
            params["port"] = port
        if dbname:
            params["dbname"] = dbname
        if user:
            params["user"] = user
        if password:
            params["password"] = password
        elif os.environ.get("PGPASSWORD"):
            params["password"] = os.environ["PGPASSWORD"]
        conn = psycopg2.connect(**params)

    conn.set_session(readonly=True, autocommit=True)
    return conn


def get_pg_version(conn) -> str:
    """Return the PostgreSQL server version string."""
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        return cur.fetchone()[0]
