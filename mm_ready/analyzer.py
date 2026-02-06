"""Offline schema dump analyzer — runs static checks against a ParsedSchema."""

from __future__ import annotations

import re
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from mm_ready.models import CheckResult, Finding, ScanReport, Severity
from mm_ready.schema_parser import ParsedSchema

# Type alias for static check functions used by the analyzer
_CheckFunc = Callable[[ParsedSchema, str, str], list[Finding]]

# ---------------------------------------------------------------------------
# Known-issues map for installed_extensions (mirrors live check)
# ---------------------------------------------------------------------------

_EXTENSION_KNOWN_ISSUES: dict[str, str] = {
    "postgis": (
        "PostGIS is compatible with Spock but requires identical versions on all nodes. "
        "Spatial indexes and topology objects need careful replication planning."
    ),
    "pg_partman": (
        "pg_partman manages partitions via background worker. Ensure partition maintenance "
        "runs on all nodes or is replicated through DDL replication."
    ),
    "pgcrypto": ("pgcrypto is compatible. Ensure encryption keys are identical across nodes."),
    "pg_trgm": ("pg_trgm provides trigram similarity functions. Compatible with Spock."),
    "btree_gist": (
        "btree_gist provides GiST operator classes. Compatible with Spock, but exclusion "
        "constraints using these operators are evaluated locally per node."
    ),
    "btree_gin": ("btree_gin provides GIN operator classes. Compatible with Spock."),
    "hstore": ("hstore is compatible. Ensure the extension is installed on all nodes."),
    "ltree": ("ltree is compatible. Ensure the extension is installed on all nodes."),
    "citext": ("citext is compatible. Ensure the extension is installed on all nodes."),
    "lo": (
        "The 'lo' extension manages large object references but does not solve logical "
        "replication of large objects. Consider LOLOR instead."
    ),
    "pg_stat_statements": (
        "pg_stat_statements is a monitoring extension. Node-local — not replicated."
    ),
    "dblink": (
        "dblink allows cross-database queries. Connections are node-local; ensure "
        "connection strings are valid on all nodes."
    ),
    "postgres_fdw": (
        "postgres_fdw provides foreign data wrappers. Foreign tables are not replicated. "
        "Ensure FDW configurations are set up on each node."
    ),
    "file_fdw": (
        "file_fdw reads from local files. Node-local — file paths must exist on each node."
    ),
    "timescaledb": (
        "TimescaleDB has its own replication mechanisms that may conflict with Spock. "
        "Co-existence is not supported."
    ),
    "citus": ("Citus distributed tables are incompatible with Spock logical replication."),
    "pgstattuple": (
        "pgstattuple provides tuple-level statistics functions. "
        "Monitoring-only extension, compatible with Spock."
    ),
}

_EXTENSION_WARNING_NAMES = frozenset({"timescaledb", "citus", "lo"})

# ---------------------------------------------------------------------------
# Volatile default patterns (mirrors live check)
# ---------------------------------------------------------------------------

_VOLATILE_PATTERNS = [
    "now()",
    "current_timestamp",
    "current_date",
    "current_time",
    "clock_timestamp()",
    "statement_timestamp()",
    "transaction_timestamp()",
    "timeofday()",
    "random()",
    "gen_random_uuid()",
    "uuid_generate_",
    "pg_current_xact_id()",
]

# ---------------------------------------------------------------------------
# Numeric column name patterns (mirrors live check)
# ---------------------------------------------------------------------------

_NUMERIC_SUSPECT_PATTERNS = [
    "count",
    "total",
    "sum",
    "balance",
    "quantity",
    "qty",
    "amount",
    "tally",
    "counter",
    "num_",
    "cnt",
    "running_",
    "cumulative",
    "aggregate",
    "accrued",
    "inventory",
]

_NUMERIC_TYPES = frozenset(
    {
        "integer",
        "bigint",
        "smallint",
        "numeric",
        "real",
        "double precision",
        "int",
        "int4",
        "int8",
        "int2",
        "float4",
        "float8",
        "decimal",
    }
)

# ---------------------------------------------------------------------------
# Spock 5 supported PG majors
# ---------------------------------------------------------------------------

SUPPORTED_PG_MAJORS = {15, 16, 17, 18}

# ---------------------------------------------------------------------------
# Checks that require a live database connection (skipped in analyze mode)
# ---------------------------------------------------------------------------

_SKIPPED_CHECKS: list[tuple[str, str, str]] = [
    # (check_name, category, description)
    # -- replication --
    ("wal_level", "replication", "WAL level check (wal_level = logical)"),
    ("max_replication_slots", "replication", "Sufficient replication slots for Spock nodes"),
    ("max_worker_processes", "replication", "Sufficient worker processes for Spock apply workers"),
    ("max_wal_senders", "replication", "Sufficient WAL senders for replication connections"),
    ("database_encoding", "replication", "Database encoding consistency across nodes"),
    ("hba_config", "replication", "pg_hba.conf allows replication connections"),
    ("stale_replication_slots", "replication", "Check for stale replication slots"),
    ("multiple_databases", "replication", "Multiple databases in the cluster"),
    ("repset_membership", "replication", "Spock replication set membership audit"),
    ("subscription_health", "replication", "Spock subscription health check"),
    ("conflict_log", "replication", "Spock conflict log review"),
    ("exception_log", "replication", "Spock exception log review"),
    # -- config (except pg_version) --
    ("track_commit_timestamp", "config", "track_commit_timestamp GUC enabled"),
    ("shared_preload_libraries", "config", "shared_preload_libraries includes spock"),
    ("spock_gucs", "config", "Spock-specific GUC settings"),
    ("idle_transaction_timeout", "config", "Idle transaction timeout settings"),
    ("pg_minor_version", "config", "PostgreSQL minor version patch level"),
    ("parallel_apply", "config", "Parallel apply worker settings"),
    ("timezone_config", "config", "Timezone configuration consistency"),
    # -- extensions (except installed_extensions) --
    ("snowflake_check", "extensions", "pgEdge snowflake extension installation"),
    ("pg_stat_statements_check", "extensions", "pg_stat_statements availability"),
    ("lolor_check", "extensions", "LOLOR extension for large object replication"),
    # -- sql_patterns --
    ("advisory_locks", "sql_patterns", "Advisory lock usage in queries"),
    ("ddl_statements", "sql_patterns", "DDL statements in pg_stat_statements"),
    ("truncate_cascade", "sql_patterns", "TRUNCATE CASCADE usage"),
    ("concurrent_indexes", "sql_patterns", "Concurrent index operations"),
    ("temp_table_queries", "sql_patterns", "Temporary table usage in queries"),
    # -- functions --
    ("stored_procedures", "functions", "Stored procedures and functions audit"),
    ("trigger_functions", "functions", "Trigger functions audit"),
    ("views_audit", "functions", "Views audit"),
    # -- schema (live-only) --
    (
        "tables_update_delete_no_pk",
        "schema",
        "UPDATE/DELETE on tables without PKs (requires pg_stat)",
    ),
    ("row_level_security", "schema", "Row-level security policies"),
    ("partitioned_tables", "schema", "Partitioned table hierarchy"),
    ("tablespace_usage", "schema", "Non-default tablespace usage"),
    ("temp_tables", "schema", "Temporary table existence"),
    ("event_triggers", "schema", "Event triggers"),
    ("notify_listen", "schema", "NOTIFY/LISTEN channel usage"),
]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_analyze(
    schema: ParsedSchema,
    file_path: str,
    categories: list[str] | None = None,
    verbose: bool = False,
) -> ScanReport:
    """Run all static checks against a parsed schema dump.

    Args:
        schema: Parsed pg_dump schema model.
        file_path: Path to the source SQL file (for report metadata).
        categories: Optional list of categories to limit checks.
        verbose: Print progress to stderr.

    Returns:
        ScanReport populated with check results.
    """
    report = ScanReport(
        database=Path(file_path).stem,
        host=file_path,
        port=0,
        timestamp=datetime.now(timezone.utc),
        pg_version=schema.pg_version or "unknown",
        scan_mode="analyze",
    )

    # Build the list of static checks to run
    checks: list[tuple[str, str, str, _CheckFunc]] = [
        # (name, category, description, function)
        ("primary_keys", "schema", "Tables without primary keys", check_primary_keys),
        ("sequence_pks", "schema", "Primary keys using standard sequences", check_sequence_pks),
        ("foreign_keys", "schema", "Foreign key relationships", check_foreign_keys),
        (
            "deferrable_constraints",
            "schema",
            "Deferrable unique/PK constraints",
            check_deferrable_constraints,
        ),
        ("exclusion_constraints", "schema", "Exclusion constraints", check_exclusion_constraints),
        (
            "missing_fk_indexes",
            "schema",
            "Foreign key columns without indexes",
            check_missing_fk_indexes,
        ),
        ("unlogged_tables", "schema", "UNLOGGED tables", check_unlogged_tables),
        ("large_objects", "schema", "Large object (OID column) usage", check_large_objects),
        ("column_defaults", "schema", "Volatile column defaults", check_column_defaults),
        (
            "numeric_columns",
            "schema",
            "Numeric columns (Delta-Apply candidates)",
            check_numeric_columns,
        ),
        (
            "multiple_unique_indexes",
            "schema",
            "Tables with multiple unique indexes",
            check_multiple_unique_indexes,
        ),
        ("enum_types", "schema", "ENUM types", check_enum_types),
        ("generated_columns", "schema", "Generated/stored columns", check_generated_columns),
        ("rules", "schema", "Rules on tables", check_rules),
        ("inheritance", "schema", "Table inheritance (non-partition)", check_inheritance),
        (
            "installed_extensions",
            "extensions",
            "Installed extensions audit",
            check_installed_extensions,
        ),
        ("sequence_audit", "sequences", "Sequence inventory and ownership", check_sequence_audit),
        ("sequence_data_types", "sequences", "Sequence data types", check_sequence_data_types),
        ("pg_version", "config", "PostgreSQL version compatibility with Spock 5", check_pg_version),
    ]

    # Filter by categories if specified
    if categories:
        cat_set = set(categories)
        checks = [c for c in checks if c[1] in cat_set]

    total = len(checks)
    if verbose:
        print(f"Analyze: running {total} static checks against {file_path}...", file=sys.stderr)

    for i, (name, category, description, func) in enumerate(checks, 1):
        if verbose:
            print(f"  [{i}/{total}] {category}/{name}: {description}", file=sys.stderr)

        result = CheckResult(
            check_name=name,
            category=category,
            description=description,
        )

        try:
            result.findings = func(schema, name, category)
        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            if verbose:
                print(f"    ERROR: {result.error}", file=sys.stderr)

        report.results.append(result)

    # Add skipped checks
    for s_name, s_category, s_description in _SKIPPED_CHECKS:
        if categories and s_category not in categories:
            continue
        report.results.append(
            CheckResult(
                check_name=s_name,
                category=s_category,
                description=s_description,
                skipped=True,
                skip_reason="Requires live database connection",
            )
        )

    if verbose:
        print(
            f"Done. {report.critical_count} critical, "
            f"{report.warning_count} warnings, "
            f"{report.consider_count} consider, "
            f"{report.info_count} info.",
            file=sys.stderr,
        )

    return report


# ---------------------------------------------------------------------------
# 19 Static Check Functions
# ---------------------------------------------------------------------------


def check_primary_keys(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify tables that lack a primary key and produce findings for them.
    
    Skips partitioned parent tables. For each non-partitioned table without a primary key, emits a WARNING Finding describing replication implications and remediation guidance.
    
    Returns:
        list[Finding]: Findings for tables missing a primary key (each with severity WARNING).
    """
    findings: list[Finding] = []
    pk_tables = {
        (c.table_schema, c.table_name)
        for c in schema.constraints
        if c.constraint_type == "PRIMARY KEY"
    }

    for tbl in schema.tables:
        if tbl.partition_by:
            continue  # skip partitioned parent tables
        fqn = f"{tbl.schema_name}.{tbl.table_name}"
        if (tbl.schema_name, tbl.table_name) not in pk_tables:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=check_name,
                    category=category,
                    title=f"Table '{fqn}' has no primary key",
                    detail=(
                        f"Table '{fqn}' lacks a primary key. Spock automatically places "
                        "tables without primary keys into the 'default_insert_only' "
                        "replication set. In this set, only INSERT and TRUNCATE operations "
                        "are replicated — UPDATE and DELETE operations are silently filtered "
                        "out by the Spock output plugin and never sent to subscribers."
                    ),
                    object_name=fqn,
                    remediation=(
                        f"Add a primary key to '{fqn}' if UPDATE/DELETE replication is "
                        "needed. If the table is genuinely insert-only (e.g. an event log), "
                        "no action is required — it will replicate correctly in the "
                        "default_insert_only replication set."
                    ),
                )
            )
    return findings


def check_sequence_pks(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Detect primary key columns that are backed by standard sequences and report CRITICAL findings recommending migration to pgEdge snowflake.
    
    Scans the provided ParsedSchema for PRIMARY KEY constraints whose constituent columns are either identity columns or use a nextval() default; for each match, returns a Finding that describes the risk, remediation, and includes metadata.
    
    Parameters:
        schema (ParsedSchema): Parsed database schema to analyze.
    
    Returns:
        list[Finding]: A list of Findings, one per PK column using a standard sequence. Each Finding has severity `CRITICAL` and includes metadata keys `"column"` and `"sequence"`.
    """
    findings: list[Finding] = []
    pk_constraints = [c for c in schema.constraints if c.constraint_type == "PRIMARY KEY"]

    for pk in pk_constraints:
        tbl = schema.get_table(pk.table_schema, pk.table_name)
        if not tbl:
            continue

        fqn = f"{pk.table_schema}.{pk.table_name}"

        for col_name in pk.columns:
            col = next((c for c in tbl.columns if c.name == col_name), None)
            if not col:
                continue

            seq_name = None
            is_sequence_backed = False

            # Check for identity column
            if col.identity:
                is_sequence_backed = True
                seq_name = f"{tbl.table_name}_{col_name}_seq (identity)"

            # Check for nextval() default
            elif col.default_expr and "nextval(" in col.default_expr.lower():
                is_sequence_backed = True
                m = re.search(r"nextval\('([^']+)'", col.default_expr)
                seq_name = m.group(1) if m else col.default_expr

            if is_sequence_backed:
                findings.append(
                    Finding(
                        severity=Severity.CRITICAL,
                        check_name=check_name,
                        category=category,
                        title=f"PK column '{fqn}.{col_name}' uses a standard sequence",
                        detail=(
                            f"Primary key column '{col_name}' on table '{fqn}' is backed by "
                            f"sequence '{seq_name or 'identity column'}'. In a multi-master setup, "
                            "standard sequences will produce conflicting values across nodes. "
                            "Must migrate to pgEdge snowflake sequences."
                        ),
                        object_name=fqn,
                        remediation=(
                            f"Convert '{fqn}.{col_name}' to use the pgEdge snowflake extension "
                            "for globally unique ID generation. See: pgEdge snowflake documentation."
                        ),
                        metadata={"column": col_name, "sequence": seq_name},
                    )
                )
    return findings


def check_foreign_keys(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Analyze FOREIGN KEY constraints and report replication-related findings.
    
    Produces WARNING findings for constraints that use ON DELETE or ON UPDATE CASCADE, describing the risk that cascaded actions are executed locally on each node and can lead to conflicts in multi-master setups. If any foreign keys are present, also emits a CONSIDER finding summarizing the total count of foreign keys and recommending that referenced tables be included in the replication set and that replication ordering preserve referential integrity.
    
    Returns:
        list[Finding]: Findings describing CASCADE-related warnings and a summary CONSIDER finding when foreign keys exist.
    """
    findings: list[Finding] = []
    fk_constraints = [c for c in schema.constraints if c.constraint_type == "FOREIGN KEY"]

    cascade_fks = []
    for fk in fk_constraints:
        fqn = f"{fk.table_schema}.{fk.table_name}"
        ref_fqn = f"{fk.ref_schema}.{fk.ref_table}" if fk.ref_table else "unknown"

        if fk.on_delete == "CASCADE" or fk.on_update == "CASCADE":
            cascade_fks.append(fk)
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=check_name,
                    category=category,
                    title=f"CASCADE foreign key '{fk.name}' on '{fqn}'",
                    detail=(
                        f"Foreign key '{fk.name}' on '{fqn}' references '{ref_fqn}' with "
                        f"ON DELETE {fk.on_delete} / ON UPDATE {fk.on_update}. CASCADE actions are "
                        "executed locally on each node, meaning the cascaded changes happen "
                        "independently on provider and subscriber, which can lead to conflicts "
                        "in a multi-master setup."
                    ),
                    object_name=fqn,
                    remediation=(
                        "Review CASCADE behavior. In multi-master, consider handling cascades "
                        "in application logic or ensuring operations flow through a single node."
                    ),
                    metadata={"constraint": fk.name, "references": ref_fqn},
                )
            )

    if fk_constraints:
        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=check_name,
                category=category,
                title=f"Database has {len(fk_constraints)} foreign key constraint(s)",
                detail=(
                    f"Found {len(fk_constraints)} foreign key constraints. Ensure all referenced tables "
                    "are included in the replication set, and that replication ordering will "
                    "satisfy referential integrity."
                ),
                object_name="(database)",
                remediation="Ensure all FK-related tables are in the same replication set.",
                metadata={"fk_count": len(fk_constraints), "cascade_count": len(cascade_fks)},
            )
        )

    return findings


def check_deferrable_constraints(
    schema: ParsedSchema, check_name: str, category: str
) -> list[Finding]:
    """
    Flag deferrable PRIMARY KEY and UNIQUE constraints that Spock will skip during conflict resolution.
    
    For each constraint in the schema that is DEFERRABLE and of type PRIMARY KEY or UNIQUE, produces a Finding describing the replication risk (conflicts on the constraint may not be detected during replication apply), suggested remediation to make the constraint non-deferrable when possible, and metadata about the constraint. PRIMARY KEY constraints are reported with higher severity than UNIQUE constraints. Each Finding's metadata contains "constraint_type" and "initially_deferred".
     
    Returns:
        list[Finding]: A list of Findings, one per deferrable PRIMARY KEY or UNIQUE constraint.
    """
    findings: list[Finding] = []

    for con in schema.constraints:
        if con.constraint_type not in ("PRIMARY KEY", "UNIQUE"):
            continue
        if not con.deferrable:
            continue

        fqn = f"{con.table_schema}.{con.table_name}"
        con_label = con.constraint_type
        severity = Severity.CRITICAL if con.constraint_type == "PRIMARY KEY" else Severity.WARNING

        findings.append(
            Finding(
                severity=severity,
                check_name=check_name,
                category=category,
                title=f"Deferrable {con_label} '{con.name}' on '{fqn}'",
                detail=(
                    f"Table '{fqn}' has a DEFERRABLE {con_label} constraint "
                    f"'{con.name}' (initially {'DEFERRED' if con.initially_deferred else 'IMMEDIATE'}). "
                    "Spock's conflict resolution checks indimmediate on indexes via "
                    "IsIndexUsableForInsertConflict() and silently SKIPS deferrable "
                    "indexes. This means conflicts on this constraint will NOT be "
                    "detected during replication apply, potentially causing "
                    "duplicate key violations or data inconsistencies."
                ),
                object_name=f"{fqn}.{con.name}",
                remediation=(
                    f"If possible, make the constraint non-deferrable:\n"
                    f"  ALTER TABLE {fqn} ALTER CONSTRAINT {con.name} NOT DEFERRABLE;\n"
                    "If deferral is required by the application, be aware that Spock "
                    "will not use this constraint for conflict detection."
                ),
                metadata={
                    "constraint_type": con_label,
                    "initially_deferred": con.initially_deferred,
                },
            )
        )

    return findings


def check_exclusion_constraints(
    schema: ParsedSchema, check_name: str, category: str
) -> list[Finding]:
    """
    Identify exclusion constraints in the schema and report them as findings because exclusion constraints are evaluated locally and can lead to conflicts or data inconsistencies in multi-master topologies.
    
    Returns:
        list[Finding]: A Finding for each exclusion constraint, describing the constraint, affected object, risk, and suggested remediation.
    """
    findings: list[Finding] = []

    for con in schema.constraints:
        if con.constraint_type != "EXCLUDE":
            continue

        fqn = f"{con.table_schema}.{con.table_name}"
        findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name=check_name,
                category=category,
                title=f"Exclusion constraint '{con.name}' on '{fqn}'",
                detail=(
                    f"Table '{fqn}' has exclusion constraint '{con.name}'. "
                    "Exclusion constraints are evaluated locally on each node. In a "
                    "multi-master topology, two nodes could independently accept rows "
                    "that would violate the exclusion constraint if evaluated globally, "
                    "leading to replication conflicts or data inconsistencies."
                ),
                object_name=f"{fqn}.{con.name}",
                remediation=(
                    "Review whether this exclusion constraint can be replaced with "
                    "application-level logic, or ensure that only one node writes data "
                    "that could conflict under this constraint."
                ),
            )
        )

    return findings


def check_missing_fk_indexes(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify foreign key constraints whose referenced columns lack a supporting index.
    
    Parameters:
        schema (ParsedSchema): Parsed database schema to analyze.
    
    Returns:
        list[Finding]: A list of Findings for each foreign key whose referenced columns are not covered by an index (includes constraint name, columns, object name, severity, detail, remediation, and metadata).
    """
    findings: list[Finding] = []
    fk_constraints = [c for c in schema.constraints if c.constraint_type == "FOREIGN KEY"]

    for fk in fk_constraints:
        if not fk.columns:
            continue

        fqn = f"{fk.table_schema}.{fk.table_name}"
        fk_cols = fk.columns

        # Check if any index covers these FK columns as a prefix
        indexes = schema.get_indexes_for_table(fk.table_schema, fk.table_name)
        # Also check PK/UNIQUE constraints (they create implicit indexes)
        pk_uk_cols = [
            c.columns
            for c in schema.constraints
            if c.table_schema == fk.table_schema
            and c.table_name == fk.table_name
            and c.constraint_type in ("PRIMARY KEY", "UNIQUE")
        ]

        has_index = False
        for idx in indexes:
            if idx.columns[: len(fk_cols)] == fk_cols:
                has_index = True
                break

        if not has_index:
            for pk_cols in pk_uk_cols:
                if pk_cols[: len(fk_cols)] == fk_cols:
                    has_index = True
                    break

        if not has_index:
            col_list = ", ".join(fk_cols)
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=check_name,
                    category=category,
                    title=f"No index on FK columns '{fqn}' ({col_list})",
                    detail=(
                        f"Foreign key constraint '{fk.name}' on '{fqn}' references "
                        f"columns ({col_list}) that have no supporting index. Without "
                        "an index, DELETE and UPDATE on the referenced (parent) table "
                        "require a sequential scan of the child table while holding a "
                        "lock. In multi-master replication, this causes longer lock "
                        "hold times and increases the likelihood of conflicts."
                    ),
                    object_name=fqn,
                    remediation=(f"Create an index:\n  CREATE INDEX ON {fqn} ({col_list});"),
                    metadata={"constraint": fk.name, "columns": fk_cols},
                )
            )

    return findings


def check_unlogged_tables(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify UNLOGGED tables in the parsed schema and produce findings for each because UNLOGGED tables are not written to the WAL and cannot be replicated.
    
    Parameters:
        schema (ParsedSchema): Parsed schema to scan for tables.
        check_name (str): Name to assign to each produced Finding.
        category (str): Category to assign to each produced Finding.
    
    Returns:
        list[Finding]: A list of Finding objects (severity WARNING), one for each UNLOGGED table found, containing title, detail, object_name, and remediation.
    """
    findings: list[Finding] = []

    for tbl in schema.tables:
        if not tbl.unlogged:
            continue
        fqn = f"{tbl.schema_name}.{tbl.table_name}"
        findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name=check_name,
                category=category,
                title=f"UNLOGGED table '{fqn}'",
                detail=(
                    f"Table '{fqn}' is UNLOGGED. Unlogged tables are not written to the "
                    "write-ahead log and therefore cannot be replicated by Spock. Data in "
                    "this table will exist only on the local node."
                ),
                object_name=fqn,
                remediation=(
                    f"If this table needs to be replicated, convert it: "
                    f"ALTER TABLE {fqn} SET LOGGED;"
                ),
            )
        )

    return findings


def check_large_objects(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """Large object (OID column) usage — logical decoding does not support LOBs.

    Note: In analyze mode we can only detect OID-type columns. We cannot count
    actual large objects in pg_largeobject_metadata without a live connection.
    """
    findings: list[Finding] = []

    for tbl in schema.tables:
        fqn = f"{tbl.schema_name}.{tbl.table_name}"
        for col in tbl.columns:
            if col.data_type.lower() == "oid":
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=check_name,
                        category=category,
                        title=f"OID column '{fqn}.{col.name}' may reference large objects",
                        detail=(
                            f"Column '{col.name}' on table '{fqn}' uses the OID data type, "
                            "which is commonly used to reference large objects. If used for LOB "
                            "references, these will not replicate through logical decoding."
                        ),
                        object_name=f"{fqn}.{col.name}",
                        remediation=(
                            "If this column references large objects, migrate to LOLOR or "
                            "BYTEA. LOLOR requires lolor.node to be set uniquely per node "
                            "and its tables added to a replication set. "
                            "If the column is used for other purposes, this finding can be ignored."
                        ),
                    )
                )

    return findings


def check_column_defaults(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify columns with volatile DEFAULT expressions (e.g., now(), random()) that may produce different values on different nodes.
    
    Parameters:
        schema (ParsedSchema): Parsed schema to analyze.
        check_name (str): Name of the check to attribute to findings.
        category (str): Category for the generated findings.
    
    Returns:
        list[Finding]: Findings for columns that use volatile defaults. Each finding's metadata includes the original default expression under the "default_expr" key.
    """
    findings: list[Finding] = []

    for tbl in schema.tables:
        fqn = f"{tbl.schema_name}.{tbl.table_name}"
        for col in tbl.columns:
            if not col.default_expr:
                continue
            if col.generated_expr:
                continue  # handled by generated_columns check

            default_lower = col.default_expr.lower()

            # Skip nextval defaults (handled by sequence_pks)
            if "nextval(" in default_lower:
                continue

            is_volatile = any(pat in default_lower for pat in _VOLATILE_PATTERNS)
            if not is_volatile:
                continue

            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=check_name,
                    category=category,
                    title=f"Volatile default on '{fqn}.{col.name}'",
                    detail=(
                        f"Column '{col.name}' on table '{fqn}' has a volatile default: "
                        f"{col.default_expr}. In multi-master replication, if a row is inserted "
                        "without specifying this column, each node could compute a different "
                        "default value. However, Spock replicates the actual inserted value, "
                        "so this is only an issue if the same row is independently inserted "
                        "on multiple nodes."
                    ),
                    object_name=f"{fqn}.{col.name}",
                    remediation=(
                        "Ensure the application always provides an explicit value for this column, "
                        "or accept that conflict resolution may be needed for concurrent inserts."
                    ),
                    metadata={"default_expr": col.default_expr},
                )
            )

    return findings


def check_numeric_columns(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify numeric columns whose names suggest accumulator/counter semantics and report findings useful for Delta-Apply evaluation.
    
    Returns:
        list[Finding]: Findings for each matching column. For nullable numeric candidate columns a `WARNING` finding is produced advising to add a NOT NULL constraint; for non-nullable candidates a `CONSIDER` finding is produced suggesting evaluation for Delta-Apply configuration. Each finding includes the column name, data type, nullability, and remediation guidance.
    """
    findings: list[Finding] = []

    for tbl in schema.tables:
        fqn = f"{tbl.schema_name}.{tbl.table_name}"
        for col in tbl.columns:
            data_type_lower = col.data_type.lower().strip()
            # Normalize common type aliases
            if data_type_lower not in _NUMERIC_TYPES:
                continue

            col_lower = col.name.lower()
            is_suspect = any(pat in col_lower for pat in _NUMERIC_SUSPECT_PATTERNS)
            if not is_suspect:
                continue

            if not col.not_null:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=check_name,
                        category=category,
                        title=f"Delta-Apply candidate '{fqn}.{col.name}' allows NULL",
                        detail=(
                            f"Column '{col.name}' on table '{fqn}' is numeric ({col.data_type}) "
                            "and its name suggests it may be an accumulator or counter. "
                            "If configured for Delta-Apply in Spock, the column MUST have a "
                            "NOT NULL constraint. The Spock apply worker "
                            "(spock_apply_heap.c:613-627) checks this and will reject "
                            "delta-apply on nullable columns."
                        ),
                        object_name=f"{fqn}.{col.name}",
                        remediation=(
                            f"If this column will use Delta-Apply, add a NOT NULL constraint:\n"
                            f"  ALTER TABLE {fqn} ALTER COLUMN {col.name} SET NOT NULL;\n"
                            "Ensure existing rows have no NULL values first."
                        ),
                        metadata={"column": col.name, "data_type": col.data_type, "nullable": True},
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.CONSIDER,
                        check_name=check_name,
                        category=category,
                        title=f"Potential Delta-Apply column: '{fqn}.{col.name}' ({col.data_type})",
                        detail=(
                            f"Column '{col.name}' on table '{fqn}' is numeric ({col.data_type}) "
                            "and its name suggests it may be an accumulator or counter. In "
                            "multi-master replication, concurrent updates to such columns can "
                            "cause conflicts. Delta-Apply can resolve this by applying the "
                            "delta (change) rather than the absolute value. This column has a "
                            "NOT NULL constraint, so it meets the Delta-Apply prerequisite."
                        ),
                        object_name=f"{fqn}.{col.name}",
                        remediation=(
                            "Investigate whether this column receives concurrent "
                            "increment/decrement updates from multiple nodes. If so, "
                            "configure it for Delta-Apply in Spock."
                        ),
                        metadata={
                            "column": col.name,
                            "data_type": col.data_type,
                            "nullable": False,
                        },
                    )
                )

    return findings


def check_multiple_unique_indexes(
    schema: ParsedSchema, check_name: str, category: str
) -> list[Finding]:
    """
    Identify tables that have more than one unique index and produce findings describing potential replication conflict-resolution ambiguity.
    
    For each table with multiple unique indexes or constraints, a Finding with severity `CONSIDER` is created. The finding's metadata contains `unique_index_count` (number of unique indexes) and `indexes` (list of unique index/constraint names).
    
    Returns:
        list[Finding]: Findings for tables that have more than one unique index.
    """
    findings: list[Finding] = []

    # Count unique indexes per table (explicit indexes + PK/UNIQUE constraints)
    table_unique: dict[tuple[str, str], list[str]] = {}

    for idx in schema.indexes:
        if idx.is_unique:
            key = (idx.table_schema, idx.table_name)
            table_unique.setdefault(key, []).append(idx.name)

    for con in schema.constraints:
        if con.constraint_type in ("PRIMARY KEY", "UNIQUE"):
            key = (con.table_schema, con.table_name)
            table_unique.setdefault(key, []).append(con.name)

    for (s, n), index_names in table_unique.items():
        if len(index_names) <= 1:
            continue
        fqn = f"{s}.{n}"
        idx_count = len(index_names)
        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=check_name,
                category=category,
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


def check_enum_types(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Report ENUM types that may require coordinated DDL changes across nodes.
    
    For each ENUM type in the parsed schema this function produces a finding that
    calls attention to ALTER TYPE ... ADD VALUE being a DDL operation that must be
    applied in a coordinated way across all nodes to avoid type mismatches.
    
    Returns:
        list[Finding]: One Finding per ENUM type. Each Finding uses severity
            `Severity.CONSIDER`, sets `object_name` to the type's fully-qualified
            name, and includes `metadata` with `label_count` and `labels` (up to 20
            label values).
    """
    findings: list[Finding] = []

    for enum in schema.enum_types:
        fqn = f"{enum.schema_name}.{enum.type_name}"
        labels = enum.labels

        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=check_name,
                category=category,
                title=f"ENUM type '{fqn}' ({len(labels)} values)",
                detail=(
                    f"ENUM type '{fqn}' has {len(labels)} values: "
                    f"{', '.join(labels[:10])}"
                    f"{'...' if len(labels) > 10 else ''}. "
                    "In multi-master replication, ALTER TYPE ... ADD VALUE is a DDL "
                    "change that must be applied on all nodes. Spock can replicate DDL "
                    "through the ddl_sql replication set, but ENUM modifications must "
                    "be coordinated carefully to avoid type mismatches during apply."
                ),
                object_name=fqn,
                remediation=(
                    "Plan ENUM modifications to be applied through Spock's DDL "
                    "replication (spock.replicate_ddl) to ensure all nodes stay in sync. "
                    "Alternatively, consider using a lookup table instead of ENUMs for "
                    "values that change frequently."
                ),
                metadata={"label_count": len(labels), "labels": labels[:20]},
            )
        )

    return findings


def check_generated_columns(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify STORED generated columns that may produce divergent values when replicated.
    
    Scans the provided ParsedSchema for columns with a stored generation expression and produces a Finding for each such column describing the replication risk and remediation. Each Finding uses severity `Severity.CONSIDER` and includes metadata with `gen_type: "STORED"` and the original generation `expression`.
    
    Parameters:
        schema (ParsedSchema): Parsed schema produced from a PostgreSQL dump to analyze.
        check_name (str): Name to assign to each Finding's check_name field.
        category (str): Category to assign to each Finding's category field.
    
    Returns:
        list[Finding]: A list of Findings, one per STORED generated column detected.
    """
    findings: list[Finding] = []

    for tbl in schema.tables:
        fqn = f"{tbl.schema_name}.{tbl.table_name}"
        for col in tbl.columns:
            if not col.generated_expr:
                continue

            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=check_name,
                    category=category,
                    title=f"Generated column '{fqn}.{col.name}' (STORED)",
                    detail=(
                        f"Column '{col.name}' on table '{fqn}' is a STORED generated column "
                        f"with expression: {col.generated_expr}. Generated columns are recomputed on the "
                        "subscriber side. If the expression depends on functions or data that "
                        "differs across nodes, values may diverge."
                    ),
                    object_name=f"{fqn}.{col.name}",
                    remediation=(
                        "Verify the generation expression produces identical results on all nodes. "
                        "Avoid expressions that depend on volatile functions or node-local state."
                    ),
                    metadata={"gen_type": "STORED", "expression": col.generated_expr},
                )
            )

    return findings


def check_rules(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Report table-level rules that may interfere with logical replication by producing findings for each rule.
    
    Each finding describes the rule, its event, remediation suggestions, and includes metadata with the rule's event and whether it is an INSTEAD rule. INSTEAD rules are reported with higher severity (WARNING); non-INSTEAD rules use CONSIDER severity.
    
    Parameters:
        schema (ParsedSchema): Parsed schema to inspect for table rules.
        check_name (str): Name used to label the produced findings.
        category (str): Category used to group the produced findings.
    
    Returns:
        list[Finding]: Findings for every rule found in the schema; each finding's metadata contains keys `event` and `is_instead`.
    """
    findings: list[Finding] = []

    for rule in schema.rules:
        fqn = f"{rule.schema_name}.{rule.table_name}"
        severity = Severity.WARNING if rule.is_instead else Severity.CONSIDER

        findings.append(
            Finding(
                severity=severity,
                check_name=check_name,
                category=category,
                title=f"{'INSTEAD ' if rule.is_instead else ''}Rule '{rule.rule_name}' on '{fqn}' ({rule.event})",
                detail=(
                    f"Table '{fqn}' has {'an INSTEAD' if rule.is_instead else 'a'} rule "
                    f"'{rule.rule_name}' on {rule.event} events. "
                    "Rules rewrite queries before execution, which means the WAL "
                    "records the rewritten operations, not the original SQL. On the "
                    "subscriber side, the Spock apply worker replays the row-level "
                    "changes from WAL, and the subscriber's rules will also fire on "
                    "the applied changes — potentially causing double-application or "
                    "unexpected side effects."
                    + (
                        " INSTEAD rules are particularly dangerous as they completely "
                        "replace the original operation."
                        if rule.is_instead
                        else ""
                    )
                ),
                object_name=f"{fqn}.{rule.rule_name}",
                remediation=(
                    "Consider converting rules to triggers (which can be controlled "
                    "via session_replication_role), or disable rules on subscriber "
                    "nodes. Review whether the rule's effect should apply on both "
                    "provider and subscriber."
                ),
                metadata={"event": rule.event, "is_instead": rule.is_instead},
            )
        )

    return findings


def check_inheritance(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Identify tables that use traditional PostgreSQL table inheritance and report replication risks.
    
    For each table that inherits from one or more parent tables, produces a WARNING Finding per parent describing that logical replication treats each table independently and that queries against the parent that rely on child data may behave differently across nodes.
    
    Returns:
        findings (list[Finding]): A list of Finding objects (one per parent relationship) describing the inheritance relationship and suggested remediation.
    """
    findings: list[Finding] = []

    for tbl in schema.tables:
        if not tbl.inherits:
            continue
        child_fqn = f"{tbl.schema_name}.{tbl.table_name}"
        for parent in tbl.inherits:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    check_name=check_name,
                    category=category,
                    title=f"Table inheritance: '{child_fqn}' inherits from '{parent}'",
                    detail=(
                        f"Table '{child_fqn}' uses traditional table inheritance from "
                        f"'{parent}'. Logical replication does not replicate through "
                        "inheritance hierarchies — each table is replicated independently. "
                        "Queries against the parent that include child data via inheritance "
                        "may behave differently across nodes."
                    ),
                    object_name=child_fqn,
                    remediation=(
                        "Consider migrating from table inheritance to declarative partitioning "
                        "(if appropriate) or separate standalone tables."
                    ),
                    metadata={"parent": parent},
                )
            )

    return findings


def check_installed_extensions(
    schema: ParsedSchema, check_name: str, category: str
) -> list[Finding]:
    """
    Audit installed extensions and report known Spock compatibility issues.
    
    Produces a Finding for each installed extension that has a documented compatibility note
    ( severity set to `WARNING` for selected extensions or `INFO` otherwise ), and appends a
    single `CONSIDER` Finding that lists all installed extensions.
    
    Returns:
        list[Finding]: Findings for each extension with known issues and a CONSIDER Finding summarizing all installed extensions.
    """
    findings: list[Finding] = []

    for ext in schema.extensions:
        extname = ext.name.lower()
        if extname in _EXTENSION_KNOWN_ISSUES:
            severity = Severity.WARNING if extname in _EXTENSION_WARNING_NAMES else Severity.INFO
            findings.append(
                Finding(
                    severity=severity,
                    check_name=check_name,
                    category=category,
                    title=f"Extension '{ext.name}'",
                    detail=_EXTENSION_KNOWN_ISSUES[extname],
                    object_name=ext.name,
                    remediation=_EXTENSION_KNOWN_ISSUES[extname]
                    if severity != Severity.INFO
                    else "",
                    metadata={"schema": ext.schema_name},
                )
            )

    if schema.extensions:
        ext_list = [e.name for e in schema.extensions]
        findings.append(
            Finding(
                severity=Severity.CONSIDER,
                check_name=check_name,
                category=category,
                title=f"Installed extensions: {len(schema.extensions)}",
                detail="Extensions: " + ", ".join(ext_list),
                object_name="(extensions)",
                remediation="Ensure all extensions are installed at identical versions on every node.",
                metadata={"extensions": ext_list},
            )
        )

    return findings


def check_sequence_audit(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Create findings for every sequence in the parsed schema, reporting sequence properties, ownership, and recommending migration to a globally-unique ID strategy (e.g., pgEdge snowflake).
    
    Parameters:
        schema (ParsedSchema): Parsed database schema containing sequences to inspect.
        check_name (str): Check identifier to assign to each Finding.
        category (str): Category to assign to each Finding.
    
    Returns:
        list[Finding]: Findings for each sequence with severity WARNING and metadata including data type, start, increment, cycle, and owner table/column.
    """
    findings: list[Finding] = []

    for seq in schema.sequences:
        fqn = f"{seq.schema_name}.{seq.sequence_name}"
        is_owned = bool(seq.owned_by_table)
        ownership = (
            f"owned by {seq.owned_by_table}.{seq.owned_by_column}"
            if is_owned
            else "not owned by any column"
        )

        findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name=check_name,
                category=category,
                title=f"Sequence '{fqn}' ({seq.data_type}, {ownership})",
                detail=(
                    f"Sequence '{fqn}': type={seq.data_type}, "
                    f"start={seq.start_value}, increment={seq.increment}, "
                    f"cycle={'yes' if seq.cycle else 'no'}, "
                    f"{ownership}. Standard sequences produce overlapping values in "
                    "multi-master setups. Must migrate to pgEdge snowflake sequences "
                    "or implement another globally-unique ID strategy."
                ),
                object_name=fqn,
                remediation=(
                    f"Migrate sequence '{fqn}' to use pgEdge snowflake for globally "
                    "unique ID generation across all cluster nodes."
                ),
                metadata={
                    "data_type": seq.data_type,
                    "start": seq.start_value,
                    "increment": seq.increment,
                    "cycle": seq.cycle,
                    "owner_table": seq.owned_by_table,
                    "owner_column": seq.owned_by_column,
                },
            )
        )

    return findings


def check_sequence_data_types(
    schema: ParsedSchema, check_name: str, category: str
) -> list[Finding]:
    """
    Identify sequences defined with small integer types (smallint or integer) that may reach their maximum value more quickly in multi-master deployments.
    
    For each sequence using `smallint` or `integer`, produces a WARNING Finding describing the data type, observed max value, and recommendation to migrate to `bigint`. Each Finding's metadata includes `data_type`, `max_value`, and `increment`.
    
    Returns:
        list[Finding]: Findings for sequences that may overflow under multi-master (smallint/integer).
    """
    findings: list[Finding] = []

    type_maxes = {"smallint": 32767, "integer": 2147483647}

    for seq in schema.sequences:
        dt = seq.data_type.lower()
        if dt not in type_maxes:
            continue

        fqn = f"{seq.schema_name}.{seq.sequence_name}"
        type_max = type_maxes[dt]
        max_value = seq.max_value or type_max

        findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name=check_name,
                category=category,
                title=f"Sequence '{fqn}' uses {dt} (max {type_max:,})",
                detail=(
                    f"Sequence '{fqn}' is defined as {dt} with max value "
                    f"{max_value:,}. In a multi-master setup with pgEdge Snowflake "
                    "sequences, the ID space is partitioned across nodes and includes "
                    "a node identifier component. Smaller integer types can exhaust "
                    "their range much faster. Consider upgrading to bigint."
                ),
                object_name=fqn,
                remediation=(
                    "Alter the column and sequence to use bigint:\n"
                    "  ALTER TABLE ... ALTER COLUMN ... TYPE bigint;\n"
                    "This allows room for Snowflake-style globally unique IDs."
                ),
                metadata={
                    "data_type": dt,
                    "max_value": max_value,
                    "increment": seq.increment,
                },
            )
        )

    return findings


def check_pg_version(schema: ParsedSchema, check_name: str, category: str) -> list[Finding]:
    """
    Assess the PostgreSQL version extracted from the parsed dump and produce findings describing compatibility with Spock 5.
    
    This check inspects schema.pg_version (the dump header's "Dumped from database version" string). It emits:
    - a WARNING finding if the version string is missing or cannot be parsed,
    - a CRITICAL finding if the major PostgreSQL version is not in the supported set,
    - an INFO finding if the major version is supported.
    
    Parameters:
        schema (ParsedSchema): Parsed schema object; the check reads `schema.pg_version`.
    
    Returns:
        list[Finding]: Findings describing compatibility status. When the version is parsed, findings include metadata with keys `major` (int) and `version` (raw version string).
    """
    findings: list[Finding] = []
    version_str = schema.pg_version

    if not version_str or version_str == "unknown":
        findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name=check_name,
                category=category,
                title="PostgreSQL version could not be determined from dump header",
                detail=(
                    "The pg_dump file does not contain a recognizable "
                    "'Dumped from database version' header comment. "
                    "Cannot assess PostgreSQL version compatibility."
                ),
                object_name="pg_version",
                remediation="Verify the PostgreSQL version manually.",
            )
        )
        return findings

    # Extract major version number
    m = re.match(r"(\d+)", version_str)
    if not m:
        findings.append(
            Finding(
                severity=Severity.WARNING,
                check_name=check_name,
                category=category,
                title=f"Unrecognized PostgreSQL version: {version_str}",
                detail=f"Could not parse major version from '{version_str}'.",
                object_name="pg_version",
                remediation="Verify the PostgreSQL version manually.",
            )
        )
        return findings

    major = int(m.group(1))

    if major not in SUPPORTED_PG_MAJORS:
        findings.append(
            Finding(
                severity=Severity.CRITICAL,
                check_name=check_name,
                category=category,
                title=f"PostgreSQL {major} is not supported by Spock 5",
                detail=(
                    f"Dump was taken from PostgreSQL {major} ({version_str}). "
                    f"Spock 5 supports PostgreSQL versions: "
                    f"{', '.join(str(v) for v in sorted(SUPPORTED_PG_MAJORS))}. "
                    "A PostgreSQL upgrade is required before Spock can be installed."
                ),
                object_name="pg_version",
                remediation=(
                    f"Upgrade PostgreSQL to version "
                    f"{max(SUPPORTED_PG_MAJORS)} (recommended) or any of: "
                    f"{', '.join(str(v) for v in sorted(SUPPORTED_PG_MAJORS))}."
                ),
                metadata={"major": major, "version": version_str},
            )
        )
    else:
        findings.append(
            Finding(
                severity=Severity.INFO,
                check_name=check_name,
                category=category,
                title=f"PostgreSQL {major} is supported by Spock 5",
                detail=f"Dump was taken from PostgreSQL {version_str}, which is compatible with Spock 5.",
                object_name="pg_version",
                metadata={"major": major, "version": version_str},
            )
        )

    return findings