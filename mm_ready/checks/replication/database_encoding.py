"""Check database encoding compatibility for Spock replication."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class DatabaseEncodingCheck(BaseCheck):
    name = "database_encoding"
    category = "replication"
    description = "Database encoding — all Spock nodes must use the same encoding"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                d.datname,
                pg_encoding_to_char(d.encoding) AS encoding,
                d.datcollate AS collation,
                d.datctype AS ctype
            FROM pg_database d
            WHERE d.datname = current_database();
        """
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()

        if not row:
            return []

        db_name, encoding, collation, ctype = row

        findings = []
        # Spock source requires same encoding on provider and subscriber.
        # Not UTF-8 specifically — just consistency. Flag non-UTF8 as INFO
        # since UTF-8 is by far the most portable choice.
        if encoding.upper() != "UTF8":
            findings.append(Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title=f"Database encoding is '{encoding}' (not UTF-8)",
                detail=(
                    f"Database '{db_name}' uses encoding '{encoding}'. Spock requires "
                    "all nodes to use the same encoding (verified in source code — it is "
                    "NOT restricted to UTF-8 as some documentation states). However, "
                    "UTF-8 is the most common and portable choice for multi-master "
                    "setups. All Spock nodes must be provisioned with the same encoding."
                ),
                object_name=db_name,
                remediation=(
                    "Ensure all Spock nodes use the same encoding. If provisioning new "
                    "nodes, consider using UTF-8 for maximum compatibility."
                ),
                metadata={
                    "encoding": encoding,
                    "collation": collation,
                    "ctype": ctype,
                },
            ))
        else:
            findings.append(Finding(
                severity=Severity.INFO,
                check_name=self.name,
                category=self.category,
                title=f"Database encoding: {encoding}",
                detail=(
                    f"Database '{db_name}' uses encoding '{encoding}' with "
                    f"collation '{collation}' and ctype '{ctype}'. "
                    "All Spock nodes must be provisioned with the same encoding."
                ),
                object_name=db_name,
                metadata={
                    "encoding": encoding,
                    "collation": collation,
                    "ctype": ctype,
                },
            ))

        return findings
