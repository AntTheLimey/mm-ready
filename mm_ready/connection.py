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

    def _resolve(cli_val: str | int | None, env_var: str) -> str | None:
        """Return CLI value (as string) if set, else env var, else None."""
        if cli_val is not None:
            return str(cli_val)
        return os.environ.get(env_var)

    if dsn:
        # psycopg2 lets keyword args override DSN components, so forward
        # all CLI/env params — they take precedence when provided.
        r_port = _resolve(port, "PGPORT")
        conn = psycopg2.connect(
            dsn,
            host=_resolve(host, "PGHOST"),
            port=int(r_port) if r_port else None,
            dbname=_resolve(dbname, "PGDATABASE"),
            user=_resolve(user, "PGUSER"),
            password=_resolve(password, "PGPASSWORD"),
            sslmode=_resolve(sslmode, "PGSSLMODE"),
            sslcert=_resolve(sslcert, "PGSSLCERT"),
            sslkey=_resolve(sslkey, "PGSSLKEY"),
            sslrootcert=_resolve(sslrootcert, "PGSSLROOTCERT"),
        )
    else:
        # Resolve each param: CLI args > PG* env vars > libpq defaults
        r_port = _resolve(port, "PGPORT")
        conn = psycopg2.connect(
            host=_resolve(host, "PGHOST"),
            port=int(r_port) if r_port else 5432,
            dbname=_resolve(dbname, "PGDATABASE"),
            user=_resolve(user, "PGUSER"),
            password=_resolve(password, "PGPASSWORD"),
            sslmode=_resolve(sslmode, "PGSSLMODE"),
            sslcert=_resolve(sslcert, "PGSSLCERT"),
            sslkey=_resolve(sslkey, "PGSSLKEY"),
            sslrootcert=_resolve(sslrootcert, "PGSSLROOTCERT"),
        )

    conn.set_client_encoding("UTF8")
    conn.set_session(readonly=True, autocommit=True)
    return conn


def get_pg_version(conn) -> str:
    """Return the PostgreSQL server version string."""
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        return cur.fetchone()[0]
