"""Check for tables with multiple unique indexes — conflict resolution implications."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class MultipleUniqueIndexesCheck(BaseCheck):
    name = "multiple_unique_indexes"
    category = "schema"
    description = "Tables with multiple unique indexes — affects Spock conflict resolution"

    def run(self, conn) -> list[Finding]:
        """
        Identify tables that have more than one unique index and generate Findings describing potential Spock conflict-resolution implications.
        
        Parameters:
            conn: A DB-API compatible connection providing a cursor() context manager on which the query is executed.
        
        Returns:
            list[Finding]: A list of Finding objects, one per table that has more than one unique index. Each Finding includes the table's fully qualified name, the count and names of unique indexes (in metadata), a severity of `Severity.CONSIDER`, and remediation guidance regarding Spock's conflict-detection behavior.
        """
        query = """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                count(*) AS unique_idx_count,
                array_agg(i.relname ORDER BY i.relname) AS index_names
            FROM pg_catalog.pg_index ix
            JOIN pg_catalog.pg_class c ON c.oid = ix.indrelid
            JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE ix.indisunique
              AND c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'spock', 'pg_toast')
            GROUP BY n.nspname, c.relname
            HAVING count(*) > 1
            ORDER BY count(*) DESC, n.nspname, c.relname;
        """
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        findings = []
        for schema_name, table_name, idx_count, index_names in rows:
            fqn = f"{schema_name}.{table_name}"
            # Check which indexes are deferrable (Spock skips those)
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Table '{fqn}' has {idx_count} unique indexes",
                    detail=(
                        f"Table '{fqn}' has {idx_count} unique indexes: "
                        f"{', '.join(index_names)}. "
                        "When check_all_uc_indexes is enabled in Spock, the apply worker "
                        "iterates all unique indexes for conflict detection and uses the "
                        "first match it finds (spock_apply_heap.c). With multiple unique "
                        "constraints, conflicts may be detected on different indexes on "
                        "different nodes, which could lead to unexpected resolution behaviour."
                    ),
                    object_name=fqn,
                    remediation=(
                        "Review whether all unique indexes are necessary for replication "
                        "conflict detection. Consider whether check_all_uc_indexes should "
                        "be enabled, and ensure the application can tolerate conflict "
                        "resolution on any of the unique constraints."
                    ),
                    metadata={"unique_index_count": idx_count, "indexes": index_names},
                )
            )

        return findings