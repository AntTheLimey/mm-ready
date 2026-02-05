"""Check max_worker_processes is sufficient for Spock."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MaxWorkerProcessesCheck(BaseCheck):
    name = "max_worker_processes"
    category = "replication"
    description = "Sufficient worker processes for Spock background workers"

    def run(self, conn) -> list[Finding]:
        with conn.cursor() as cur:
            cur.execute("SHOW max_worker_processes;")
            max_workers = int(cur.fetchone()[0])

        findings = []
        # Spock needs several bgworkers: supervisor, writer, manager per sub, etc.
        if max_workers < 16:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=self.name,
                    category=self.category,
                    title=f"max_worker_processes = {max_workers}",
                    detail=(
                        f"max_worker_processes is set to {max_workers}. Spock uses multiple "
                        "background worker processes (supervisor, apply workers per subscription, "
                        "etc.). For a multi-master cluster, this should be set higher to "
                        "accommodate Spock workers alongside standard PostgreSQL workers."
                    ),
                    object_name="max_worker_processes",
                    remediation=(
                        "Set max_worker_processes to at least 16 (or higher for larger clusters) "
                        "in postgresql.conf. Requires a restart."
                    ),
                    metadata={"current_value": max_workers},
                )
            )
        return findings
