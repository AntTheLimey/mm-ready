"""Check parallel apply worker configuration."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class ParallelApplyCheck(BaseCheck):
    name = "parallel_apply"
    category = "config"
    description = "Parallel apply workers configuration for Spock performance"

    def run(self, conn) -> list[Finding]:
        """
        Validate Spock parallel-apply related PostgreSQL configuration and produce findings.
        
        Queries the database for `max_worker_processes`, `max_parallel_workers`, `max_logical_replication_workers`, and `max_sync_workers_per_subscription`, generates Findings for values that are below recommended thresholds, and always includes a summary Finding containing the observed parameter values.
        
        Parameters:
            conn: A DB-API compatible connection exposing a cursor() context manager used to execute `SHOW <parameter>;` and fetch the parameter value.
        
        Returns:
            list[Finding]: A list of Finding objects describing any configuration issues and a summary entry with all queried parameter values.
        """
        params = {}
        with conn.cursor() as cur:
            for param in [
                "max_worker_processes",
                "max_parallel_workers",
                "max_logical_replication_workers",
                "max_sync_workers_per_subscription",
            ]:
                try:
                    cur.execute(f"SHOW {param};")
                    params[param] = cur.fetchone()[0]
                except Exception:
                    params[param] = None

        findings = []

        # max_logical_replication_workers
        lr_workers = params.get("max_logical_replication_workers")
        if lr_workers is not None:
            lr_val = int(lr_workers)
            if lr_val < 4:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=self.name,
                        category=self.category,
                        title=f"max_logical_replication_workers = {lr_val}",
                        detail=(
                            f"max_logical_replication_workers is {lr_val}. Spock uses logical "
                            "replication workers for apply. Increase for better parallel apply throughput."
                        ),
                        object_name="max_logical_replication_workers",
                        remediation="Set max_logical_replication_workers to at least 4 in postgresql.conf.",
                        metadata={"current_value": lr_val},
                    )
                )

        # max_sync_workers_per_subscription
        sync_val = params.get("max_sync_workers_per_subscription")
        if sync_val is not None:
            sv = int(sync_val)
            if sv < 2:
                findings.append(
                    Finding(
                        severity=Severity.CONSIDER,
                        check_name=self.name,
                        category=self.category,
                        title=f"max_sync_workers_per_subscription = {sv}",
                        detail=(
                            f"max_sync_workers_per_subscription is {sv}. Higher values allow "
                            "faster initial table synchronization when setting up Spock subscriptions."
                        ),
                        object_name="max_sync_workers_per_subscription",
                        remediation="Consider increasing to 2-4 for faster initial sync.",
                        metadata={"current_value": sv},
                    )
                )

        # Summary
        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title="Parallel apply configuration summary",
                detail="\n".join(f"  {k} = {v}" for k, v in params.items()),
                object_name="(config)",
                remediation="Review values for your expected cluster size and workload.",
                metadata=params,
            )
        )
        return findings