"""Tests for mm_ready.analyzer — static check functions against ParsedSchema."""

from __future__ import annotations

from mm_ready.analyzer import (
    check_column_defaults,
    check_deferrable_constraints,
    check_enum_types,
    check_exclusion_constraints,
    check_foreign_keys,
    check_generated_columns,
    check_inheritance,
    check_installed_extensions,
    check_large_objects,
    check_missing_fk_indexes,
    check_multiple_unique_indexes,
    check_numeric_columns,
    check_pg_version,
    check_primary_keys,
    check_rules,
    check_sequence_audit,
    check_sequence_data_types,
    check_sequence_pks,
    check_unlogged_tables,
    run_analyze,
)
from mm_ready.models import Severity
from mm_ready.schema_parser import (
    ColumnDef,
    ConstraintDef,
    EnumTypeDef,
    ExtensionDef,
    IndexDef,
    ParsedSchema,
    RuleDef,
    SequenceDef,
    TableDef,
)

CN = "test_check"
CAT = "schema"


# ---------------------------------------------------------------------------
# check_primary_keys
# ---------------------------------------------------------------------------


class TestCheckPrimaryKeys:
    def test_table_without_pk(self):
        schema = ParsedSchema()
        schema.tables.append(TableDef(schema_name="public", table_name="orders"))
        findings = check_primary_keys(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING
        assert "orders" in findings[0].title

    def test_table_with_pk(self):
        schema = ParsedSchema()
        schema.tables.append(TableDef(schema_name="public", table_name="users"))
        schema.constraints.append(
            ConstraintDef(
                name="users_pkey",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="users",
                columns=["id"],
            )
        )
        findings = check_primary_keys(schema, CN, CAT)
        assert len(findings) == 0

    def test_partitioned_table_skipped(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="events",
                partition_by="RANGE (created_at)",
            )
        )
        findings = check_primary_keys(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_sequence_pks
# ---------------------------------------------------------------------------


class TestCheckSequencePks:
    def test_nextval_pk(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="users",
                columns=[
                    ColumnDef(
                        name="id",
                        data_type="integer",
                        default_expr="nextval('users_id_seq'::regclass)",
                    )
                ],
            )
        )
        schema.constraints.append(
            ConstraintDef(
                name="users_pkey",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="users",
                columns=["id"],
            )
        )
        findings = check_sequence_pks(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CRITICAL

    def test_identity_pk(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="items",
                columns=[ColumnDef(name="id", data_type="integer", identity="BY DEFAULT")],
            )
        )
        schema.constraints.append(
            ConstraintDef(
                name="items_pkey",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="items",
                columns=["id"],
            )
        )
        findings = check_sequence_pks(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CRITICAL

    def test_non_sequence_pk(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="lookups",
                columns=[ColumnDef(name="code", data_type="text")],
            )
        )
        schema.constraints.append(
            ConstraintDef(
                name="lookups_pkey",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="lookups",
                columns=["code"],
            )
        )
        findings = check_sequence_pks(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_foreign_keys
# ---------------------------------------------------------------------------


class TestCheckForeignKeys:
    def test_cascade_fk(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="fk1",
                constraint_type="FOREIGN KEY",
                table_schema="public",
                table_name="orders",
                columns=["user_id"],
                ref_schema="public",
                ref_table="users",
                ref_columns=["id"],
                on_delete="CASCADE",
            )
        )
        findings = check_foreign_keys(schema, CN, CAT)
        # One CASCADE warning + one summary CONSIDER
        cascade = [f for f in findings if f.severity == Severity.WARNING]
        summary = [f for f in findings if f.severity == Severity.CONSIDER]
        assert len(cascade) == 1
        assert len(summary) == 1

    def test_no_cascade_fk(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="fk1",
                constraint_type="FOREIGN KEY",
                table_schema="public",
                table_name="orders",
                columns=["user_id"],
                ref_schema="public",
                ref_table="users",
                ref_columns=["id"],
            )
        )
        findings = check_foreign_keys(schema, CN, CAT)
        # Only summary, no CASCADE warning
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_no_fks(self):
        schema = ParsedSchema()
        findings = check_foreign_keys(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_deferrable_constraints
# ---------------------------------------------------------------------------


class TestCheckDeferrableConstraints:
    def test_deferrable_pk_is_critical(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="pk",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="t",
                columns=["id"],
                deferrable=True,
                initially_deferred=True,
            )
        )
        findings = check_deferrable_constraints(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CRITICAL

    def test_deferrable_unique_is_warning(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="uq",
                constraint_type="UNIQUE",
                table_schema="public",
                table_name="t",
                columns=["email"],
                deferrable=True,
            )
        )
        findings = check_deferrable_constraints(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_non_deferrable_ignored(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="pk",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="t",
                columns=["id"],
            )
        )
        findings = check_deferrable_constraints(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_exclusion_constraints
# ---------------------------------------------------------------------------


class TestCheckExclusionConstraints:
    def test_exclusion_found(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="no_overlap",
                constraint_type="EXCLUDE",
                table_schema="public",
                table_name="bookings",
            )
        )
        findings = check_exclusion_constraints(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_no_exclusions(self):
        schema = ParsedSchema()
        findings = check_exclusion_constraints(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_missing_fk_indexes
# ---------------------------------------------------------------------------


class TestCheckMissingFkIndexes:
    def test_fk_without_index(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="fk1",
                constraint_type="FOREIGN KEY",
                table_schema="public",
                table_name="orders",
                columns=["user_id"],
                ref_schema="public",
                ref_table="users",
                ref_columns=["id"],
            )
        )
        findings = check_missing_fk_indexes(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_fk_with_covering_index(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="fk1",
                constraint_type="FOREIGN KEY",
                table_schema="public",
                table_name="orders",
                columns=["user_id"],
                ref_schema="public",
                ref_table="users",
                ref_columns=["id"],
            )
        )
        schema.indexes.append(
            IndexDef(
                name="idx_user_id",
                table_schema="public",
                table_name="orders",
                columns=["user_id"],
            )
        )
        findings = check_missing_fk_indexes(schema, CN, CAT)
        assert len(findings) == 0

    def test_fk_covered_by_pk(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="fk1",
                constraint_type="FOREIGN KEY",
                table_schema="public",
                table_name="orders",
                columns=["id"],
                ref_schema="public",
                ref_table="users",
                ref_columns=["id"],
            )
        )
        schema.constraints.append(
            ConstraintDef(
                name="pk",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="orders",
                columns=["id"],
            )
        )
        findings = check_missing_fk_indexes(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_unlogged_tables
# ---------------------------------------------------------------------------


class TestCheckUnloggedTables:
    def test_unlogged_found(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="cache",
                unlogged=True,
            )
        )
        findings = check_unlogged_tables(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_logged_table_ignored(self):
        schema = ParsedSchema()
        schema.tables.append(TableDef(schema_name="public", table_name="users"))
        findings = check_unlogged_tables(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_large_objects
# ---------------------------------------------------------------------------


class TestCheckLargeObjects:
    def test_oid_column(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="docs",
                columns=[ColumnDef(name="data_oid", data_type="oid")],
            )
        )
        findings = check_large_objects(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_no_oid_columns(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="docs",
                columns=[ColumnDef(name="data", data_type="bytea")],
            )
        )
        findings = check_large_objects(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_column_defaults
# ---------------------------------------------------------------------------


class TestCheckColumnDefaults:
    def test_volatile_now(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="events",
                columns=[ColumnDef(name="created_at", data_type="timestamp", default_expr="now()")],
            )
        )
        findings = check_column_defaults(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_nextval_skipped(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="t",
                columns=[
                    ColumnDef(
                        name="id", data_type="integer", default_expr="nextval('t_id_seq'::regclass)"
                    )
                ],
            )
        )
        findings = check_column_defaults(schema, CN, CAT)
        assert len(findings) == 0

    def test_non_volatile_skipped(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="t",
                columns=[ColumnDef(name="status", data_type="text", default_expr="'active'")],
            )
        )
        findings = check_column_defaults(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_numeric_columns
# ---------------------------------------------------------------------------


class TestCheckNumericColumns:
    def test_nullable_counter(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="stats",
                columns=[ColumnDef(name="total_count", data_type="integer", not_null=False)],
            )
        )
        findings = check_numeric_columns(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_not_null_counter(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="stats",
                columns=[ColumnDef(name="balance", data_type="numeric", not_null=True)],
            )
        )
        findings = check_numeric_columns(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_non_suspect_name_ignored(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="users",
                columns=[ColumnDef(name="age", data_type="integer", not_null=True)],
            )
        )
        findings = check_numeric_columns(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_multiple_unique_indexes
# ---------------------------------------------------------------------------


class TestCheckMultipleUniqueIndexes:
    def test_multiple_unique(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="pk",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="users",
                columns=["id"],
            )
        )
        schema.constraints.append(
            ConstraintDef(
                name="uq_email",
                constraint_type="UNIQUE",
                table_schema="public",
                table_name="users",
                columns=["email"],
            )
        )
        findings = check_multiple_unique_indexes(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_single_unique_ok(self):
        schema = ParsedSchema()
        schema.constraints.append(
            ConstraintDef(
                name="pk",
                constraint_type="PRIMARY KEY",
                table_schema="public",
                table_name="users",
                columns=["id"],
            )
        )
        findings = check_multiple_unique_indexes(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_enum_types
# ---------------------------------------------------------------------------


class TestCheckEnumTypes:
    def test_enum_found(self):
        schema = ParsedSchema()
        schema.enum_types.append(
            EnumTypeDef(
                schema_name="public",
                type_name="status",
                labels=["active", "inactive"],
            )
        )
        findings = check_enum_types(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_no_enums(self):
        schema = ParsedSchema()
        findings = check_enum_types(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_generated_columns
# ---------------------------------------------------------------------------


class TestCheckGeneratedColumns:
    def test_generated_column(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="products",
                columns=[
                    ColumnDef(name="total", data_type="numeric", generated_expr="price + tax")
                ],
            )
        )
        findings = check_generated_columns(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER

    def test_no_generated(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="t",
                columns=[ColumnDef(name="x", data_type="integer")],
            )
        )
        findings = check_generated_columns(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_rules
# ---------------------------------------------------------------------------


class TestCheckRules:
    def test_instead_rule_is_warning(self):
        schema = ParsedSchema()
        schema.rules.append(
            RuleDef(
                schema_name="public",
                table_name="important",
                rule_name="no_delete",
                event="DELETE",
                is_instead=True,
            )
        )
        findings = check_rules(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_non_instead_rule_is_consider(self):
        schema = ParsedSchema()
        schema.rules.append(
            RuleDef(
                schema_name="public",
                table_name="audit",
                rule_name="log_update",
                event="UPDATE",
                is_instead=False,
            )
        )
        findings = check_rules(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.CONSIDER


# ---------------------------------------------------------------------------
# check_inheritance
# ---------------------------------------------------------------------------


class TestCheckInheritance:
    def test_inheritance_found(self):
        schema = ParsedSchema()
        schema.tables.append(
            TableDef(
                schema_name="public",
                table_name="child",
                inherits=["public.parent"],
            )
        )
        findings = check_inheritance(schema, CN, CAT)
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_no_inheritance(self):
        schema = ParsedSchema()
        schema.tables.append(TableDef(schema_name="public", table_name="standalone"))
        findings = check_inheritance(schema, CN, CAT)
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_installed_extensions
# ---------------------------------------------------------------------------


class TestCheckInstalledExtensions:
    def test_known_warning_extension(self):
        schema = ParsedSchema()
        schema.extensions.append(ExtensionDef(name="timescaledb"))
        findings = check_installed_extensions(schema, CN, "extensions")
        warnings = [f for f in findings if f.severity == Severity.WARNING]
        assert len(warnings) == 1

    def test_known_info_extension(self):
        schema = ParsedSchema()
        schema.extensions.append(ExtensionDef(name="pg_trgm"))
        findings = check_installed_extensions(schema, CN, "extensions")
        info = [f for f in findings if f.severity == Severity.INFO]
        assert len(info) == 1

    def test_summary_always_present(self):
        schema = ParsedSchema()
        schema.extensions.append(ExtensionDef(name="plpgsql"))
        findings = check_installed_extensions(schema, CN, "extensions")
        consider = [f for f in findings if f.severity == Severity.CONSIDER]
        assert len(consider) == 1

    def test_no_extensions(self):
        schema = ParsedSchema()
        findings = check_installed_extensions(schema, CN, "extensions")
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_sequence_audit
# ---------------------------------------------------------------------------


class TestCheckSequenceAudit:
    def test_sequences_found(self):
        schema = ParsedSchema()
        schema.sequences.append(
            SequenceDef(
                schema_name="public",
                sequence_name="users_id_seq",
                data_type="bigint",
                owned_by_table="public.users",
                owned_by_column="id",
            )
        )
        findings = check_sequence_audit(schema, CN, "sequences")
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING
        assert "owned by" in findings[0].detail

    def test_unowned_sequence(self):
        schema = ParsedSchema()
        schema.sequences.append(
            SequenceDef(
                schema_name="public",
                sequence_name="global_seq",
            )
        )
        findings = check_sequence_audit(schema, CN, "sequences")
        assert "not owned" in findings[0].detail

    def test_no_sequences(self):
        schema = ParsedSchema()
        findings = check_sequence_audit(schema, CN, "sequences")
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_sequence_data_types
# ---------------------------------------------------------------------------


class TestCheckSequenceDataTypes:
    def test_integer_sequence_warned(self):
        schema = ParsedSchema()
        schema.sequences.append(
            SequenceDef(
                schema_name="public",
                sequence_name="narrow_seq",
                data_type="integer",
            )
        )
        findings = check_sequence_data_types(schema, CN, "sequences")
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING

    def test_smallint_sequence_warned(self):
        schema = ParsedSchema()
        schema.sequences.append(
            SequenceDef(
                schema_name="public",
                sequence_name="tiny_seq",
                data_type="smallint",
            )
        )
        findings = check_sequence_data_types(schema, CN, "sequences")
        assert len(findings) == 1

    def test_bigint_sequence_ok(self):
        schema = ParsedSchema()
        schema.sequences.append(
            SequenceDef(
                schema_name="public",
                sequence_name="wide_seq",
                data_type="bigint",
            )
        )
        findings = check_sequence_data_types(schema, CN, "sequences")
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# check_pg_version
# ---------------------------------------------------------------------------


class TestCheckPgVersion:
    def test_unsupported_version(self):
        schema = ParsedSchema(pg_version="14.8")
        findings = check_pg_version(schema, CN, "config")
        assert len(findings) == 1
        assert findings[0].severity == Severity.CRITICAL

    def test_supported_version(self):
        schema = ParsedSchema(pg_version="17.0")
        findings = check_pg_version(schema, CN, "config")
        assert len(findings) == 1
        assert findings[0].severity == Severity.INFO

    def test_unknown_version(self):
        schema = ParsedSchema(pg_version="")
        findings = check_pg_version(schema, CN, "config")
        assert len(findings) == 1
        assert findings[0].severity == Severity.WARNING


# ---------------------------------------------------------------------------
# run_analyze orchestrator
# ---------------------------------------------------------------------------


class TestRunAnalyze:
    def test_report_structure(self, tmp_path):
        f = tmp_path / "dump.sql"
        f.write_text("-- Dumped from database version 17.0\n", encoding="utf-8")
        report = run_analyze(
            ParsedSchema(pg_version="17.0"),
            file_path=str(f),
        )
        assert report.scan_mode == "analyze"
        assert report.database == "dump"
        assert report.host == str(f)
        assert report.port == 0

    def test_active_and_skipped_counts(self, tmp_path):
        f = tmp_path / "dump.sql"
        f.write_text("-- Dumped from database version 17.0\n", encoding="utf-8")
        report = run_analyze(ParsedSchema(pg_version="17.0"), file_path=str(f))
        active = [r for r in report.results if not r.skipped]
        skipped = [r for r in report.results if r.skipped]
        assert len(active) == 19
        assert len(skipped) == 37

    def test_category_filtering(self, tmp_path):
        f = tmp_path / "dump.sql"
        f.write_text("", encoding="utf-8")
        report = run_analyze(
            ParsedSchema(),
            file_path=str(f),
            categories=["config"],
        )
        active = [r for r in report.results if not r.skipped]
        # Only pg_version is a config check in the active set
        assert len(active) == 1
        assert active[0].check_name == "pg_version"

    def test_skipped_checks_have_reason(self, tmp_path):
        f = tmp_path / "dump.sql"
        f.write_text("", encoding="utf-8")
        report = run_analyze(ParsedSchema(), file_path=str(f))
        skipped = [r for r in report.results if r.skipped]
        for r in skipped:
            assert r.skip_reason == "Requires live database connection"
