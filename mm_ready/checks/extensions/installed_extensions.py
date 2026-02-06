"""Audit all installed extensions for Spock compatibility."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class InstalledExtensionsCheck(BaseCheck):
    name = "installed_extensions"
    category = "extensions"
    description = "Audit installed extensions for known Spock compatibility issues"

    # Extensions known to have issues or considerations with logical replication
    KNOWN_ISSUES = {
        "postgis": "PostGIS is supported but ensure identical versions on all nodes.",
        "pg_partman": "Partition management must be coordinated across nodes.",
        "pgcrypto": "Supported. Ensure identical versions across nodes.",
        "pg_trgm": "Supported. Index-only, no replication concerns.",
        "btree_gist": "Supported. Index-only, no replication concerns.",
        "btree_gin": "Supported. Index-only, no replication concerns.",
        "hstore": "Supported. Ensure identical versions across nodes.",
        "ltree": "Supported. Ensure identical versions across nodes.",
        "citext": "Supported. Ensure identical versions across nodes.",
        "lo": "Large object helper — consider LOLOR instead for replication.",
        "pg_stat_statements": "Monitoring extension. Node-local data only.",
        "dblink": "Cross-database queries are node-local. Review usage.",
        "postgres_fdw": "Foreign data wrappers are node-local. Review usage.",
        "file_fdw": "Foreign data wrappers are node-local. Review usage.",
        "timescaledb": "TimescaleDB has its own replication. May conflict with Spock.",
        "citus": "Citus has its own distributed architecture. Incompatible with Spock.",
    }

    def run(self, conn) -> list[Finding]:
        """
        Audit installed PostgreSQL extensions and produce findings for known Spock compatibility issues.
        
        Executes a query against pg_catalog to list installed extensions with their versions and schema, creates a Finding for each extension that has a known issue (with severity determined by the extension), and appends a summary Finding that lists all discovered extensions.
        
        Parameters:
        	conn: A DB connection object that supports the context manager protocol for cursor() and allows execution of SQL queries.
        
        Returns:
        	findings (list[Finding]): A list containing per-extension Finding objects for extensions in KNOWN_ISSUES (severity INFO or WARNING) followed by a final summary Finding (severity CONSIDER) that lists all installed extensions.
        """
        query = """
            SELECT extname, extversion, n.nspname AS schema_name
            FROM pg_catalog.pg_extension e
            JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
            ORDER BY extname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        ext_list = []
        for extname, extversion, schema_name in rows:
            ext_list.append(f"{extname} ({extversion})")

            if extname in self.KNOWN_ISSUES:
                severity = (
                    Severity.WARNING if extname in ("timescaledb", "citus", "lo") else Severity.INFO
                )
                findings.append(
                    Finding(
                        severity=severity,
                        check_name=self.name,
                        category=self.category,
                        title=f"Extension '{extname}' v{extversion}",
                        detail=self.KNOWN_ISSUES[extname],
                        object_name=extname,
                        remediation=self.KNOWN_ISSUES[extname] if severity != Severity.INFO else "",
                        metadata={"version": extversion, "schema": schema_name},
                    )
                )

        # Summary
        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title=f"Installed extensions: {len(rows)}",
                detail="Extensions: " + ", ".join(ext_list),
                object_name="(extensions)",
                remediation="Ensure all extensions are installed at identical versions on every node.",
                metadata={"extensions": ext_list},
            )
        )
        return findings