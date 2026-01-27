"""Check PostgreSQL version compatibility with Spock 5."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class PgVersionCheck(BaseCheck):
    name = "pg_version"
    category = "config"
    description = "PostgreSQL version compatibility with Spock 5"

    # Spock 5.x supports PostgreSQL 15, 16, 17, 18
    # (PG 18 added in Spock 5.0.3; confirmed via src/compat/ directories)
    SUPPORTED_MAJORS = {15, 16, 17, 18}

    def run(self, conn) -> list[Finding]:
        query = "SELECT version(), current_setting('server_version_num')::int;"
        with conn.cursor() as cur:
            cur.execute(query)
            version_str, version_num = cur.fetchone()

        major = version_num // 10000

        findings = []
        if major not in self.SUPPORTED_MAJORS:
            findings.append(Finding(
                severity=Severity.CRITICAL,
                check_name=self.name,
                category=self.category,
                title=f"PostgreSQL {major} is not supported by Spock 5",
                detail=(
                    f"Server is running PostgreSQL {major} ({version_str}). "
                    f"Spock 5 supports PostgreSQL versions: "
                    f"{', '.join(str(v) for v in sorted(self.SUPPORTED_MAJORS))}. "
                    "A PostgreSQL upgrade is required before Spock can be installed."
                ),
                object_name="pg_version",
                remediation=(
                    f"Upgrade PostgreSQL to version "
                    f"{max(self.SUPPORTED_MAJORS)} (recommended) or any of: "
                    f"{', '.join(str(v) for v in sorted(self.SUPPORTED_MAJORS))}."
                ),
                metadata={"major": major, "version_num": version_num},
            ))
        else:
            findings.append(Finding(
                severity=Severity.INFO,
                check_name=self.name,
                category=self.category,
                title=f"PostgreSQL {major} is supported by Spock 5",
                detail=f"Server is running {version_str}, which is compatible with Spock 5.",
                object_name="pg_version",
                metadata={"major": major, "version_num": version_num},
            ))

        return findings
