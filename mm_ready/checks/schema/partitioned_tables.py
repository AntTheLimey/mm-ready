"""Check for partitioned tables and their partition strategies."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class PartitionedTablesCheck(BaseCheck):
    name = "partitioned_tables"
    category = "schema"
    description = "Partitioned tables — review partition strategy for Spock compatibility"

    def run(self, conn) -> list[Finding]:
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                pt.partstrat AS strategy,
                (
                    SELECT count(*)
                    FROM pg_catalog.pg_inherits i
                    WHERE i.inhparent = c.oid
                ) AS partition_count
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = c.oid
            WHERE c.relkind = 'p'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            ORDER BY n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        strategy_labels = {"r": "RANGE", "l": "LIST", "h": "HASH"}

        findings = []
        for schema_name, table_name, strategy, part_count in rows:
            fqn = f"{schema_name}.{table_name}"
            strat_label = strategy_labels.get(strategy, strategy)
            findings.append(Finding(
                severity=Severity.CONSIDER,
                check_name=self.name,
                category=self.category,
                title=f"Partitioned table '{fqn}' ({strat_label}, {part_count} partitions)",
                detail=(
                    f"Table '{fqn}' uses {strat_label} partitioning with {part_count} "
                    "partition(s). Spock 5 supports partition replication, but the partition "
                    "structure must be identical on all nodes. Adding/removing partitions "
                    "must be coordinated across the cluster."
                ),
                object_name=fqn,
                remediation=(
                    "Ensure partition definitions are identical across all nodes. "
                    "Plan partition maintenance (add/drop) as a coordinated cluster "
                    "operation.\n\n"
                    "Important: detaching a partition (ALTER TABLE ... DETACH PARTITION) "
                    "does NOT automatically remove it from the replication set. The "
                    "Spock AutoDDL code handles AT_AttachPartition but not "
                    "AT_DetachPartition. After detaching, manually remove the "
                    "orphaned table if replication is no longer needed:\n"
                    "  SELECT spock.repset_remove_table('default', 'schema.partition_name');"
                ),
                metadata={"strategy": strat_label, "partition_count": part_count},
            ))
        return findings
