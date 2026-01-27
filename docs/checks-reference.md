# Checks Reference

Complete reference for all mm-ready checks. Each check is a `BaseCheck`
subclass auto-discovered at runtime from the `mm_ready/checks/` directory.

Checks are organized by category. Within each category, the mode column
indicates when the check runs:
- **scan** — Pre-Spock readiness assessment (default)
- **audit** — Post-Spock health check (requires Spock installed)

---

## Schema (22 checks)

### primary_keys

| | |
|---|---|
| **File** | `checks/schema/primary_keys.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Tables without primary keys — affects Spock replication behaviour |

Queries `pg_class` for user tables that have no primary key constraint.

Spock places tables without primary keys into the `default_insert_only`
replication set, where only INSERT and TRUNCATE operations are replicated.
UPDATE and DELETE are silently filtered out.

**Remediation:** Add a primary key if UPDATE/DELETE replication is needed. If
the table is genuinely insert-only (e.g. an event log), no action required.

---

### tables_update_delete_no_pk

| | |
|---|---|
| **File** | `checks/schema/identity_replica.py` |
| **Mode** | scan |
| **Severity** | CRITICAL |
| **Description** | Tables with UPDATE/DELETE activity but no primary key — operations silently dropped |

Queries `pg_stat_user_tables` for tables without primary keys that have
non-zero `n_tup_upd` or `n_tup_del` counters.

This is the most dangerous finding. These tables will be placed in the
`default_insert_only` replication set, and their UPDATE/DELETE operations
will be silently dropped on other nodes — causing data loss.

Tables that are insert-only (no updates/deletes) are reported as INFO instead.

**Remediation:** Add a primary key. Note: REPLICA IDENTITY FULL is NOT a
substitute — Spock uses replication sets, not replica identity, to determine
replication behavior.

---

### deferrable_constraints

| | |
|---|---|
| **File** | `checks/schema/deferrable_constraints.py` |
| **Mode** | scan |
| **Severity** | CRITICAL (PK) / WARNING (unique) |
| **Description** | Deferrable unique/PK constraints — silently skipped by Spock conflict resolution |

Queries `pg_constraint` for primary key and unique constraints where
`condeferrable = true`.

Spock's `IsIndexUsableForInsertConflict()` function skips deferrable indexes
during conflict detection. This means conflicts on deferrable constraints go
undetected, potentially causing duplicate key violations.

- **CRITICAL** for deferrable primary key constraints
- **WARNING** for deferrable unique constraints

**Remediation:** Make the constraint non-deferrable:
`ALTER TABLE t ALTER CONSTRAINT c NOT DEFERRABLE;`

---

### exclusion_constraints

| | |
|---|---|
| **File** | `checks/schema/exclusion_constraints.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Exclusion constraints — not enforceable across nodes |

Queries `pg_constraint` for exclusion constraints (`contype = 'x'`).

Exclusion constraints are evaluated locally on each node. Two nodes can
independently accept rows that violate the constraint globally.

**Remediation:** Replace with application-level logic, or ensure only one node
writes data that could conflict under this constraint.

---

### foreign_keys

| | |
|---|---|
| **File** | `checks/schema/foreign_keys.py` |
| **Mode** | scan |
| **Severity** | WARNING (CASCADE) / INFO (summary) |
| **Description** | Foreign key relationships — replication ordering and cross-node considerations |

Queries `pg_constraint` for foreign key constraints, including delete and
update action types.

CASCADE actions execute locally on each node independently, which can cause
conflicts in multi-master. Non-CASCADE foreign keys are reported as INFO for
awareness.

Action codes: `a`=NO ACTION, `r`=RESTRICT, `c`=CASCADE, `n`=SET NULL,
`d`=SET DEFAULT.

**Remediation:** Consider handling cascades in application logic or routing
cascade operations through a single node.

---

### sequence_pks

| | |
|---|---|
| **File** | `checks/schema/sequence_pks.py` |
| **Mode** | scan |
| **Severity** | CRITICAL |
| **Description** | Primary keys using standard sequences — must migrate to pgEdge snowflake |

Queries `pg_constraint` and `pg_attribute` to find primary key columns backed
by sequences (via `pg_get_serial_sequence()`) or identity columns
(`attidentity != ''`).

Standard sequences produce overlapping values when multiple nodes generate IDs
independently. Must migrate to pgEdge Snowflake for globally unique IDs.

**Remediation:** Convert the column to use the pgEdge snowflake extension.

---

### unlogged_tables

| | |
|---|---|
| **File** | `checks/schema/unlogged_tables.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | UNLOGGED tables — not written to WAL, cannot be replicated |

Queries `pg_class` for tables with `relpersistence = 'u'`.

UNLOGGED tables are not written to the write-ahead log and therefore cannot
be captured by logical decoding. Data exists only on the local node.

**Remediation:** Convert with `ALTER TABLE t SET LOGGED;` if replication is
needed.

---

### large_objects

| | |
|---|---|
| **File** | `checks/schema/large_objects.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Large object (LOB) usage — logical decoding does not support them |

Two queries:
1. Counts rows in `pg_largeobject_metadata`
2. Finds columns with `atttypid = 'oid'::regtype` that may reference LOBs

PostgreSQL's logical decoding cannot decode changes to large objects.

**Remediation:** Migrate to the LOLOR extension for replication-safe large
object management, or store binary data in BYTEA columns. See the
[lolor_check](#lolor_check) entry for LOLOR setup details.

---

### generated_columns

| | |
|---|---|
| **File** | `checks/schema/generated_columns.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | Generated/stored columns — replication behavior differences |

Queries `pg_attribute` for columns where `attgenerated != ''`, retrieving
generation expressions via `pg_get_expr()`.

Generated columns are recomputed on the subscriber side. If expressions depend
on volatile functions or node-local state, values may diverge across nodes.

Distinguishes STORED (`s`) and VIRTUAL (`v`) types.

**Remediation:** Ensure generation expressions produce identical results on all
nodes. Avoid volatile functions or node-local state in expressions.

---

### partitioned_tables

| | |
|---|---|
| **File** | `checks/schema/partitioned_tables.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | Partitioned tables — review partition strategy for Spock compatibility |

Queries `pg_partitioned_table` with partition count via `pg_inherits`.

Spock 5 supports partition replication, but partition structure must be
identical on all nodes. Maps strategies: `r`=RANGE, `l`=LIST, `h`=HASH.

**Remediation:** Ensure partition definitions are identical across nodes. Plan
partition maintenance as a coordinated cluster operation.

**Important:** Spock's AutoDDL handles `ATTACH PARTITION` (adds the new
partition to the replication set) but does NOT handle `DETACH PARTITION`.
Detached partitions remain in the replication set and must be manually removed:
```sql
SELECT spock.repset_remove_table('default', 'schema.detached_partition');
```

---

### inheritance

| | |
|---|---|
| **File** | `checks/schema/inheritance.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Table inheritance (non-partition) — not well supported in logical replication |

Queries `pg_inherits` for inheritance relationships, excluding partitioned
tables (`relkind = 'p'`).

Logical replication does not replicate through inheritance hierarchies. Each
table is replicated independently.

**Remediation:** Migrate to declarative partitioning or separate standalone
tables.

---

### column_defaults

| | |
|---|---|
| **File** | `checks/schema/column_defaults.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Volatile column defaults — may differ across nodes |

Queries `pg_attrdef` for column default expressions via `pg_get_expr()`.

Detects volatile patterns: `now()`, `current_timestamp`, `current_date`,
`clock_timestamp()`, `statement_timestamp()`, `transaction_timestamp()`,
`timeofday()`, `random()`, `gen_random_uuid()`, `uuid_generate_*`,
`pg_current_xact_id()`.

Spock replicates actual values, so this is only an issue if the same row is
inserted independently on multiple nodes (each generating its own default).

**Remediation:** Ensure the application provides explicit values, or accept
that conflict resolution may be needed.

---

### numeric_columns

| | |
|---|---|
| **File** | `checks/schema/numeric_columns.py` |
| **Mode** | scan |
| **Severity** | WARNING (nullable) / CONSIDER (NOT NULL) |
| **Description** | Numeric columns that may be Delta-Apply candidates |

Queries numeric-type columns with names matching suspect patterns: `count`,
`total`, `sum`, `balance`, `quantity`, `amount`, etc.

Spock's Delta-Apply conflict resolution (for counters and accumulators)
requires columns to have a NOT NULL constraint (verified in
`spock_apply_heap.c:613-627`).

- **WARNING** if column allows NULL (must add NOT NULL for Delta-Apply)
- **INFO** if column has NOT NULL (investigate whether it's a counter)

**Remediation:** Add NOT NULL constraint if needed, then configure for
Delta-Apply in Spock.

---

### multiple_unique_indexes

| | |
|---|---|
| **File** | `checks/schema/multiple_unique_indexes.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | Tables with multiple unique indexes — affects conflict resolution |

Queries `pg_index` for tables with more than one unique index.

When `check_all_uc_indexes` is enabled, the apply worker uses the first
matching unique index for conflict detection, which may differ per node.

**Remediation:** Review whether all unique indexes are necessary for conflict
detection.

---

### enum_types

| | |
|---|---|
| **File** | `checks/schema/enum_types.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | ENUM types — DDL changes require multi-node coordination |

Queries `pg_type` and `pg_enum` for all enum types and their labels.

`ALTER TYPE ... ADD VALUE` is DDL that must be applied on all nodes, either
through Spock's DDL replication (`spock.replicate_ddl`) or manually.

**Remediation:** Use Spock DDL replication for enum modifications, or consider
a lookup table for frequently changing values.

---

### rules

| | |
|---|---|
| **File** | `checks/schema/rules.py` |
| **Mode** | scan |
| **Severity** | WARNING (INSTEAD rules) / CONSIDER (other rules) |
| **Description** | Rules on tables — can cause unexpected behaviour with logical replication |

Queries `pg_rewrite` for non-return rules on tables.

Rules rewrite queries before execution; the WAL records the rewritten
operations. The subscriber's rules will also fire, potentially causing
double-application. INSTEAD rules are particularly dangerous.

**Remediation:** Convert rules to triggers (controllable via
`session_replication_role`) or disable rules on subscriber nodes.

---

### row_level_security

| | |
|---|---|
| **File** | `checks/schema/row_level_security.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Row-level security policies — apply worker runs as superuser, bypasses RLS |

Queries tables with `relrowsecurity = true`, including policy count.

Spock's apply worker runs as superuser and bypasses all RLS policies. All
replicated rows are applied regardless of policies on the subscriber.

**Remediation:** Use replication sets for data filtering instead of relying on
RLS for subscriber-side restrictions.

---

### event_triggers

| | |
|---|---|
| **File** | `checks/schema/event_triggers.py` |
| **Mode** | scan |
| **Severity** | WARNING (REPLICA) / CONSIDER (ALWAYS) / INFO (ORIGIN, DISABLED) |
| **Description** | Event triggers — fire on DDL events, interact with Spock DDL replication |

Queries `pg_event_trigger` for event triggers and their enabled status.

Spock's apply worker sets `session_replication_role = 'replica'`
(confirmed in `spock_apply.c:3742`). Event trigger enabled modes:

- **ALWAYS** (`A`): INFO — Correct for DDL-automation triggers (e.g. audit
  logging, auto-partitioning). These need to fire during DDL replay to keep
  schemas consistent. Side-effect triggers (notifications, external calls)
  should use ORIGIN mode instead.
- **REPLICA** (`R`): WARNING — Fires only during apply, not on direct DDL.
  This is rarely the desired behavior.
- **ORIGIN** (`O`): INFO — Default mode, fires on direct DDL only.
- **DISABLED** (`D`): INFO — Never fires.

**Remediation:** Use ORIGIN mode for most triggers. Use ALWAYS only for
DDL-automation triggers that must fire during Spock DDL replay.

---

### notify_listen

| | |
|---|---|
| **File** | `checks/schema/notify_listen.py` |
| **Mode** | scan |
| **Severity** | WARNING (functions) / CONSIDER (pg_stat_statements) |
| **Description** | LISTEN/NOTIFY usage — notifications are not replicated |

Two queries:
1. Searches function source code (`prosrc`) for `NOTIFY` / `pg_notify` calls
2. Optionally searches `pg_stat_statements` for NOTIFY patterns

LISTEN/NOTIFY is not replicated by logical replication. Notifications only fire
on the originating node.

**Remediation:** Ensure listeners connect to all nodes, or implement an
application-level notification mechanism.

---

### tablespace_usage

| | |
|---|---|
| **File** | `checks/schema/tablespace_usage.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | Non-default tablespace usage — tablespaces must exist on all nodes |

Queries `pg_class` for objects with `reltablespace != 0`.

Tablespaces are local to each PostgreSQL instance. The same tablespace names
must exist on all nodes (they can point to different physical paths).

**Remediation:** Create matching tablespace names on all Spock nodes before
initializing replication.

---

### temp_tables

| | |
|---|---|
| **File** | `checks/schema/temp_tables.py` |
| **Mode** | scan |
| **Severity** | INFO |
| **Description** | Functions creating temporary tables — session-local, never replicated |

Searches function source code (`prosrc`) for `CREATE TEMP TABLE` or
`CREATE TEMPORARY TABLE` patterns, filtering to functions and procedures
(`prokind IN ('f', 'p')`).

Temp tables are session-scoped and not replicated. Flagged for awareness only.

**Remediation:** No action needed if temp table usage is intentional and
node-local.

---

### missing_fk_indexes

| | |
|---|---|
| **File** | `checks/schema/missing_fk_indexes.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Foreign key columns without indexes — slow cascades and lock contention |

Queries `pg_constraint` for foreign key constraints and cross-references with
`pg_index` to find referencing-side columns that lack a supporting index.

Missing indexes on FK columns cause sequential scans during CASCADE operations
and hold locks longer. In multi-master, this is amplified — concurrent deletes
on different nodes both trigger cascades, increasing lock contention and
conflict probability.

**Remediation:** Create an index on the referencing column(s):
```sql
CREATE INDEX ON referencing_table (fk_column);
```

---

## Replication (12 checks)

### wal_level

| | |
|---|---|
| **File** | `checks/replication/wal_level.py` |
| **Mode** | scan |
| **Severity** | CRITICAL |
| **Description** | wal_level must be 'logical' for Spock replication |

Executes `SHOW wal_level;`.

Spock requires `wal_level = 'logical'` to enable logical decoding of the WAL.
This is a standard PostgreSQL setting that should be configured before
installing Spock.

**Remediation:**
```sql
ALTER SYSTEM SET wal_level = 'logical';
-- Restart PostgreSQL
```

---

### max_replication_slots

| | |
|---|---|
| **File** | `checks/replication/max_replication_slots.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Sufficient replication slots for Spock node connections |

Queries `SHOW max_replication_slots` and counts existing slots in
`pg_replication_slots`.

An N-node cluster needs at least N-1 slots per node, plus headroom.
Warns if the setting is below 10.

**Remediation:** Set `max_replication_slots` to at least 10 in
`postgresql.conf` and restart.

---

### max_worker_processes

| | |
|---|---|
| **File** | `checks/replication/max_worker_processes.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Sufficient worker processes for Spock background workers |

Executes `SHOW max_worker_processes;`.

Spock uses multiple background workers: supervisor, writer, and manager per
subscription. Warns if below 16.

**Remediation:** Set `max_worker_processes` to at least 16 and restart.

---

### max_wal_senders

| | |
|---|---|
| **File** | `checks/replication/max_wal_senders.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Sufficient WAL senders for Spock logical replication |

Queries `current_setting('max_wal_senders')` and counts active senders in
`pg_stat_replication`.

Each Spock subscription requires a WAL sender process. Warns if below 10.

**Remediation:** Set `max_wal_senders` to at least 10 and restart.

---

### database_encoding

| | |
|---|---|
| **File** | `checks/replication/database_encoding.py` |
| **Mode** | scan |
| **Severity** | CONSIDER (non-UTF8) / INFO (UTF-8) |
| **Description** | Database encoding — all Spock nodes must use the same encoding |

Queries the current database for encoding, collation, and ctype.

Spock requires the same encoding on all nodes. UTF-8 is the most common and
portable choice but is not strictly required.

**Remediation:** Ensure all nodes use the same encoding. Prefer UTF-8 for new
installations.

---

### multiple_databases

| | |
|---|---|
| **File** | `checks/replication/multiple_databases.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | More than one user database — Spock supports one DB per instance |

Queries `pg_database` for non-template databases other than `postgres`.

pgEdge Spock officially supports one database per PostgreSQL instance.

**Remediation:** Separate databases into individual PostgreSQL instances.

---

### hba_config

| | |
|---|---|
| **File** | `checks/replication/hba_config.py` |
| **Mode** | scan |
| **Severity** | WARNING (no entries) / CONSIDER (cannot read) / INFO (entries found) |
| **Description** | pg_hba.conf must allow replication connections between nodes |

Queries `pg_hba_file_rules` (PostgreSQL 15+) for replication database entries.

Requires superuser or `pg_read_all_settings` privilege to read
`pg_hba_file_rules`. If the view is not accessible, reports an INFO with
guidance to check manually.

**Remediation:** Add replication entries:
```
host replication spock_user 0.0.0.0/0 scram-sha-256
```

---

### repset_membership

| | |
|---|---|
| **File** | `checks/replication/repset_membership.py` |
| **Mode** | audit |
| **Severity** | WARNING |
| **Description** | Tables not in any Spock replication set |

Queries `spock.repset_table` to find user tables that are not members of any
replication set. Skips if the `spock` schema does not exist.

Tables not in a replication set will not be replicated at all.

**Remediation:**
```sql
SELECT spock.repset_add_table('default', 'schema.table');
```

---

### subscription_health

| | |
|---|---|
| **File** | `checks/replication/sub_health.py` |
| **Mode** | audit |
| **Severity** | CRITICAL (disabled) / WARNING (inactive slot) / INFO (no subscriptions) |
| **Description** | Health of Spock subscriptions |

Queries `spock.subscription` for all subscriptions and
`pg_replication_slots` for slot activity status.

- **CRITICAL** if a subscription is disabled
- **WARNING** if a replication slot is inactive

**Remediation:** Re-enable with
`SELECT spock.alter_subscription_enable('name');` and check network/provider
status for inactive slots.

---

### conflict_log

| | |
|---|---|
| **File** | `checks/replication/conflict_log.py` |
| **Mode** | audit |
| **Severity** | WARNING |
| **Description** | Spock conflict history analysis |

Queries `spock.conflict_history` (if the table exists), grouping conflicts by
table, type, and resolution method with counts and last occurrence.

**Remediation:** Review conflict patterns and adjust conflict resolution
strategy or data access patterns.

---

### exception_log

| | |
|---|---|
| **File** | `checks/replication/exception_log.py` |
| **Mode** | audit |
| **Severity** | CRITICAL |
| **Description** | Spock exception (apply error) log analysis |

Queries `spock.exception_log` (if the table exists), grouping exceptions by
origin node, table, and error message.

Each exception represents a row that could not be applied — causing data
divergence between nodes.

**Remediation:** Review `exception_log_detail` for full row data. Resolve the
underlying issue and manually fix affected rows.

---

### stale_replication_slots

| | |
|---|---|
| **File** | `checks/replication/stale_replication_slots.py` |
| **Mode** | audit |
| **Severity** | CRITICAL (>1 GB retained) / WARNING (>100 MB) / INFO (healthy) |
| **Description** | Inactive replication slots retaining WAL — can cause disk exhaustion |

Queries `pg_replication_slots` for inactive slots and uses
`pg_wal_lsn_diff()` to calculate the amount of WAL retained by each.

Inactive slots prevent WAL segments from being recycled. In a busy cluster,
this can quickly exhaust disk space and cause PostgreSQL to shut down.

- **CRITICAL** if retained WAL exceeds 1 GB
- **WARNING** if retained WAL exceeds 100 MB
- **INFO** if slots are healthy or no inactive slots found

**Remediation:** Investigate why the slot is inactive. If the subscriber is
permanently gone, drop the slot:
```sql
SELECT pg_drop_replication_slot('slot_name');
```

---

## Config (8 checks)

### pg_version

| | |
|---|---|
| **File** | `checks/config/pg_version.py` |
| **Mode** | scan |
| **Severity** | CRITICAL (unsupported) / INFO (supported) |
| **Description** | PostgreSQL version compatibility with Spock 5 |

Executes `SELECT version(), current_setting('server_version_num')::int;`.

Spock 5 supports PostgreSQL **15, 16, 17, 18**. PG 18 was added in Spock
5.0.3 (confirmed via `src/compat/18/` in the Spock source).

**Remediation:** Upgrade to a supported PostgreSQL version.

---

### track_commit_timestamp

| | |
|---|---|
| **File** | `checks/config/track_commit_ts.py` |
| **Mode** | scan |
| **Severity** | CRITICAL |
| **Description** | track_commit_timestamp must be on for Spock conflict resolution |

Executes `SHOW track_commit_timestamp;`.

Required for Spock's last-update-wins conflict resolution. This is a standard
PostgreSQL setting that should be configured before installing Spock.

**Remediation:**
```sql
ALTER SYSTEM SET track_commit_timestamp = on;
-- Restart PostgreSQL
```

---

### parallel_apply

| | |
|---|---|
| **File** | `checks/config/parallel_apply.py` |
| **Mode** | scan |
| **Severity** | WARNING / CONSIDER / INFO |
| **Description** | Parallel apply worker configuration for Spock performance |

Queries multiple settings: `max_logical_replication_workers`,
`max_sync_workers_per_subscription`, `max_worker_processes`,
`max_parallel_workers`.

- **WARNING** if `max_logical_replication_workers` < 4
- **INFO** if `max_sync_workers_per_subscription` < 2
- **INFO** summary of all parallel-related parameters

**Remediation:** Set `max_logical_replication_workers >= 4` and
`max_sync_workers_per_subscription` to 2-4.

---

### shared_preload_libraries

| | |
|---|---|
| **File** | `checks/config/shared_preload.py` |
| **Mode** | audit |
| **Severity** | CRITICAL |
| **Description** | shared_preload_libraries must include 'spock' |

Executes `SHOW shared_preload_libraries;` and checks whether `spock` appears
in the value.

**Remediation:** Add `spock` to `shared_preload_libraries` and restart
PostgreSQL.

---

### spock_gucs

| | |
|---|---|
| **File** | `checks/config/spock_gucs.py` |
| **Mode** | audit |
| **Severity** | WARNING / INFO |
| **Description** | Spock-specific GUC settings |

Queries `current_setting()` for Spock GUC parameters:
- `spock.conflict_resolution` — recommended: `last_update_wins`
- `spock.save_resolutions` — recommended: `on`
- `spock.enable_ddl_replication` — recommended: `on` (AutoDDL)
- `spock.include_ddl_repset` — recommended: `on` (auto-add tables to repsets)
- `spock.allow_ddl_from_functions` — recommended: `on` (capture DDL inside functions)

Reports INFO if GUCs are unavailable (Spock may not be loaded). Note: AutoDDL
captures DDL classified as `LOGSTMT_DDL` by PostgreSQL. TRUNCATE
(`LOGSTMT_MISC`) and VACUUM/ANALYZE (`LOGSTMT_ALL`) are NOT captured.

**Remediation:** `ALTER SYSTEM SET spock.conflict_resolution = 'last_update_wins';`

---

### timezone_config

| | |
|---|---|
| **File** | `checks/config/timezone_config.py` |
| **Mode** | scan |
| **Severity** | WARNING (non-UTC) / CONSIDER (log_timezone) / INFO (UTC) |
| **Description** | Timezone settings — UTC recommended for consistent commit timestamps |

Queries `current_setting('timezone')` and `current_setting('log_timezone')`.

Spock's last-update-wins conflict resolution compares commit timestamps. If
nodes are in different timezones and timezone-aware timestamps are not handled
consistently, conflict resolution may produce unexpected results. UTC is
recommended for all nodes.

**Remediation:**
```sql
ALTER SYSTEM SET timezone = 'UTC';
ALTER SYSTEM SET log_timezone = 'UTC';
-- Reload configuration
SELECT pg_reload_conf();
```

---

### idle_transaction_timeout

| | |
|---|---|
| **File** | `checks/config/idle_tx_timeout.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | Idle-in-transaction timeout — long-idle transactions block VACUUM and cause bloat |

Queries `idle_in_transaction_session_timeout` and `idle_session_timeout`.

Idle-in-transaction connections hold transaction IDs (XIDs) that prevent
VACUUM from reclaiming dead tuples. In multi-master, WAL accumulation is
amplified across nodes.

**Remediation:**
```sql
ALTER SYSTEM SET idle_in_transaction_session_timeout = '300s';
ALTER SYSTEM SET idle_session_timeout = '3600s';
SELECT pg_reload_conf();
```

---

### pg_minor_version

| | |
|---|---|
| **File** | `checks/config/pg_minor_version.py` |
| **Mode** | audit |
| **Severity** | INFO |
| **Description** | PostgreSQL minor version — all nodes should run the same minor version |

Queries `current_setting('server_version')` and
`current_setting('server_version_num')`.

While Spock does not strictly require identical minor versions, running
different minor versions across nodes can introduce subtle behavioral
differences. Keeping all nodes on the same minor version is a best practice.

**Remediation:** Plan a coordinated minor version upgrade across all nodes.

---

## Extensions (4 checks)

### installed_extensions

| | |
|---|---|
| **File** | `checks/extensions/installed_extensions.py` |
| **Mode** | scan |
| **Severity** | WARNING (problematic) / CONSIDER (summary) / INFO (compatible) |
| **Description** | Installed extensions with Spock compatibility notes |

Queries `pg_extension` for all installed extensions with versions and schemas.

Maintains a compatibility map for known extensions:
- **PostGIS**: Supported (ensure identical versions on all nodes)
- **pg_partman**: Partition management must be coordinated
- **TimescaleDB**: WARNING — has own replication, may conflict
- **Citus**: WARNING — distributed architecture, incompatible
- **lo**: WARNING — consider LOLOR instead

**Remediation:** Ensure all extensions are installed at identical versions on
every node.

---

### snowflake_check

| | |
|---|---|
| **File** | `checks/extensions/snowflake_check.py` |
| **Mode** | scan |
| **Severity** | WARNING (unavailable or node not set) / CONSIDER (available but not installed) / INFO (properly configured) |
| **Description** | pgEdge snowflake extension availability and node configuration |

Three checks:
1. Check if installed via `pg_extension`
2. Check if available via `pg_available_extensions`
3. If installed, verify `snowflake.node` is configured via `current_setting()`

The Snowflake extension is required for globally unique ID generation in
multi-master setups. Each node must have a unique `snowflake.node` value.
For standard pgEdge clusters (n1–n9), this is set automatically. For larger
clusters or non-standard naming, it must be set manually.

**Remediation:** Install the snowflake extension package, then:
```sql
CREATE EXTENSION snowflake;
ALTER SYSTEM SET snowflake.node = <unique_id>;
-- Restart PostgreSQL
```

---

### pg_stat_statements_check

| | |
|---|---|
| **File** | `checks/extensions/pgstat_statements.py` |
| **Mode** | scan |
| **Severity** | WARNING (installed but not queryable) / CONSIDER (not installed but available) / INFO (other states) |
| **Description** | pg_stat_statements availability for SQL pattern analysis |

Checks whether `pg_stat_statements` is installed and queryable.

Several SQL pattern checks depend on `pg_stat_statements`. The extension must
be in `shared_preload_libraries` (not just created) to function.

**Remediation:**
```sql
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
-- Restart PostgreSQL
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

---

### lolor_check

| | |
|---|---|
| **File** | `checks/extensions/lolor_check.py` |
| **Mode** | scan |
| **Severity** | WARNING (LOs exist, LOLOR missing) / INFO (other states) |
| **Description** | LOLOR extension for large object replication |

Checks for large objects in `pg_largeobject_metadata` and OID-type columns,
then checks whether the LOLOR extension is installed and properly configured.

Standard PostgreSQL logical decoding does not support large objects. The LOLOR
extension provides replication-safe large object management by intercepting
LO operations and routing them through replicatable tables.

If LOLOR is installed, verifies that `lolor.node` is set (each node needs a
unique value from 1 to 2^28).

**Remediation:**
```sql
CREATE EXTENSION lolor;
ALTER SYSTEM SET lolor.node = <unique_id>;  -- unique per node
-- Restart PostgreSQL
-- Add LOLOR tables to replication set:
SELECT spock.repset_add_table('default', 'lolor.pg_largeobject');
SELECT spock.repset_add_table('default', 'lolor.pg_largeobject_metadata');
```

---

## SQL Patterns (5 checks)

All SQL pattern checks query `pg_stat_statements` for problematic query
patterns. They gracefully skip if `pg_stat_statements` is not available.

### truncate_cascade

| | |
|---|---|
| **File** | `checks/sql_patterns/truncate_cascade.py` |
| **Mode** | scan |
| **Severity** | WARNING (CASCADE) / CONSIDER (RESTART IDENTITY) |
| **Description** | TRUNCATE CASCADE and RESTART IDENTITY replication caveats |

Two pattern searches in `pg_stat_statements`:
1. `TRUNCATE ... CASCADE` — The publisher encodes the CASCADE flag, but the
   subscriber hardcodes `DROP_RESTRICT` (`spock_apply.c:1707`). This means
   CASCADE is silently ignored on subscribers — only the explicitly named
   table is truncated. TRUNCATE is replicated via replication sets, NOT
   via AutoDDL (TRUNCATE is `LOGSTMT_MISC`, not `LOGSTMT_DDL`).
2. `TRUNCATE ... RESTART IDENTITY` — IS replicated (`spock_apply.c:1708`
   passes `restart_seqs`), which resets sequences on the subscriber, risking
   ID collisions with standard sequences.

**Remediation:**
- CASCADE: List all dependent tables explicitly in the TRUNCATE statement
- RESTART IDENTITY: Avoid with standard sequences, or switch to Snowflake IDs

---

### ddl_statements

| | |
|---|---|
| **File** | `checks/sql_patterns/ddl_statements.py` |
| **Mode** | scan |
| **Severity** | INFO |
| **Description** | DDL statements found in query history |

Searches `pg_stat_statements` for CREATE, ALTER, and DROP patterns targeting
tables, indexes, views, functions, procedures, triggers, types, sequences,
and schemas.

DDL is not automatically replicated by default. Spock's AutoDDL feature
(`spock.enable_ddl_replication=on`) captures DDL classified as `LOGSTMT_DDL`
by PostgreSQL. This includes CREATE/ALTER/DROP TABLE, INDEX, VIEW, FUNCTION,
SEQUENCE, as well as CLUSTER and REINDEX.

AutoDDL does **NOT** capture:
- TRUNCATE (`LOGSTMT_MISC` — replicated via replication sets)
- VACUUM, ANALYZE (`LOGSTMT_ALL` — must run independently on each node)

**Remediation:** Enable AutoDDL:
```sql
ALTER SYSTEM SET spock.enable_ddl_replication = on;
ALTER SYSTEM SET spock.include_ddl_repset = on;
```
Or use `spock.replicate_ddl_command()` for manual DDL propagation.

---

### advisory_locks

| | |
|---|---|
| **File** | `checks/sql_patterns/advisory_locks.py` |
| **Mode** | scan |
| **Severity** | CONSIDER |
| **Description** | Advisory lock usage — locks are node-local |

Searches `pg_stat_statements` for `pg_advisory_lock` and
`pg_try_advisory_lock` patterns.

Advisory locks are node-local and provide no cross-node coordination.

**Remediation:** Implement a distributed locking mechanism if locks are used
for application-level coordination.

---

### concurrent_indexes

| | |
|---|---|
| **File** | `checks/sql_patterns/concurrent_indexes.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | CREATE INDEX CONCURRENTLY — must be created manually on each node |

Searches `pg_stat_statements` for `CREATE INDEX CONCURRENTLY` patterns.

Concurrent index creation cannot be replicated via DDL replication due to its
multi-transaction nature.

**Remediation:** Execute `CREATE INDEX CONCURRENTLY` manually on each node.

---

### temp_table_queries

| | |
|---|---|
| **File** | `checks/sql_patterns/temp_table_queries.py` |
| **Mode** | scan |
| **Severity** | INFO |
| **Description** | CREATE TEMP TABLE in SQL — session-local, not replicated |

Searches `pg_stat_statements` for `CREATE TEMP TABLE` and
`CREATE TEMPORARY TABLE` patterns.

Temporary tables are session-scoped and not replicated. Usually expected
behavior — flagged for awareness only.

**Remediation:** No action needed unless temp tables are expected to persist
across nodes.

---

## Functions (3 checks)

### stored_procedures

| | |
|---|---|
| **File** | `checks/functions/stored_procedures.py` |
| **Mode** | scan |
| **Severity** | CONSIDER / INFO |
| **Description** | Stored procedures/functions with write operations or DDL |

Queries `pg_proc` for functions written in plpgsql, sql, plpython3u, plperl,
or plv8. Extracts full function definitions and searches for write operation
patterns: INSERT, UPDATE, DELETE, TRUNCATE, CREATE, ALTER, DROP, EXECUTE,
PERFORM.

Write operations inside functions are replicated as row-level WAL changes, but
side effects (DDL, NOTIFY, advisory locks, temp tables, external calls) are
not replicated.

**Remediation:** Review functions for non-replicated side effects.

---

### trigger_functions

| | |
|---|---|
| **File** | `checks/functions/trigger_functions.py` |
| **Mode** | scan |
| **Severity** | WARNING (ALWAYS/REPLICA) / INFO (ORIGIN/DISABLED) |
| **Description** | Trigger enabled modes — ENABLE REPLICA and ENABLE ALWAYS fire during Spock apply |

Queries all triggers (excluding internal) with timing, event, function name,
and enabled status.

Spock apply workers run with `session_replication_role = 'replica'` (confirmed
in `spock_apply.c:3742`):
- **ALWAYS** (`A`): Fires on all sessions including apply — WARNING
- **REPLICA** (`R`): Fires during apply — WARNING
- **ORIGIN** (`O`): Default, fires on non-replica sessions only — INFO
- **DISABLED** (`D`): Never fires — INFO

**Remediation:** Use ORIGIN mode for most triggers. Only use REPLICA or ALWAYS
when the trigger must fire during replication apply.

---

### views_audit

| | |
|---|---|
| **File** | `checks/functions/views.py` |
| **Mode** | scan |
| **Severity** | WARNING (materialized views) / CONSIDER (regular views) |
| **Description** | Views and materialized views — refresh coordination |

Two queries:
1. Materialized views with sizes
2. Count of regular views

Materialized views are not replicated. Each node maintains its own copy and
REFRESH must be coordinated independently.

**Remediation:** Coordinate `REFRESH MATERIALIZED VIEW` across nodes. Ensure
regular view definitions are identical via DDL replication.

---

## Sequences (2 checks)

### sequence_audit

| | |
|---|---|
| **File** | `checks/sequences/sequence_audit.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Full sequence inventory with types, ownership, and migration needs |

Queries `pg_sequence` with ownership via `pg_depend`, including data types,
start/increment/min/max values, and cycle flag.

Standard sequences produce overlapping values in multi-master. All sequences
need a migration plan to pgEdge Snowflake or an alternative globally-unique
ID strategy.

Includes metadata about ownership (owned by table.column or standalone).

**Remediation:** Migrate to pgEdge Snowflake for globally unique IDs.

---

### sequence_data_types

| | |
|---|---|
| **File** | `checks/sequences/sequence_data_types.py` |
| **Mode** | scan |
| **Severity** | WARNING |
| **Description** | Sequence data types — smallint/integer may overflow faster in multi-master |

Queries `pg_sequence` for data types, min/max values, start, and increment.

With Snowflake-style sequences, the ID space is partitioned across nodes with
a node identifier component. Smaller types exhaust their range much faster:
- `smallint`: max 32,767
- `integer`: max 2,147,483,647

**Remediation:** Alter columns and sequences to use `bigint` for
Snowflake-compatible globally unique IDs.
