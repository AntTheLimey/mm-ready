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
    sslmode: str | None = None,
    sslcert: str | None = None,
    sslkey: str | None = None,
    sslrootcert: str | None = None,
) -> psycopg2.extensions.connection:
    """Create a database connection from explicit args or a DSN string.

    CLI args take precedence over DSN components if both are provided.
    Falls back to standard PG* environment variables.
    """
    if dsn:
        conn = psycopg2.connect(dsn)
    else:
        # Build params dict: CLI args > PG* env vars > libpq defaults
        _env_map = {
            "host": ("PGHOST", host),
            "port": ("PGPORT", port),
            "dbname": ("PGDATABASE", dbname),
            "user": ("PGUSER", user),
            "password": ("PGPASSWORD", password),
            "sslmode": ("PGSSLMODE", sslmode),
            "sslcert": ("PGSSLCERT", sslcert),
            "sslkey": ("PGSSLKEY", sslkey),
            "sslrootcert": ("PGSSLROOTCERT", sslrootcert),
        }
        params = {}
        for key, (env_var, cli_val) in _env_map.items():
            val = cli_val if cli_val is not None else os.environ.get(env_var)
            if val is not None:
                params[key] = val

        params.setdefault("port", 5432)
        conn = psycopg2.connect(**params)

    conn.set_client_encoding("UTF8")
    conn.set_session(readonly=True, autocommit=True)
    return conn


def get_pg_version(conn) -> str:
    """Return the PostgreSQL server version string."""
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        return cur.fetchone()[0]
