"""Parse a pg_dump --schema-only SQL file into an in-memory schema model."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ColumnDef:
    name: str
    data_type: str
    not_null: bool = False
    default_expr: str | None = None
    identity: str | None = None  # "ALWAYS" or "BY DEFAULT"
    generated_expr: str | None = None  # GENERATED ALWAYS AS (...) STORED


@dataclass
class ConstraintDef:
    name: str
    constraint_type: str  # PRIMARY KEY, UNIQUE, FOREIGN KEY, EXCLUDE, CHECK
    table_schema: str
    table_name: str
    columns: list[str] = field(default_factory=list)
    # FK-specific
    ref_schema: str = ""
    ref_table: str = ""
    ref_columns: list[str] = field(default_factory=list)
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    # Deferrable
    deferrable: bool = False
    initially_deferred: bool = False


@dataclass
class IndexDef:
    name: str
    table_schema: str
    table_name: str
    columns: list[str] = field(default_factory=list)
    is_unique: bool = False
    index_method: str = "btree"


@dataclass
class SequenceDef:
    schema_name: str
    sequence_name: str
    data_type: str = "bigint"
    start_value: int | None = None
    increment: int = 1
    min_value: int | None = None
    max_value: int | None = None
    cycle: bool = False
    owned_by_table: str | None = None
    owned_by_column: str | None = None


@dataclass
class TableDef:
    schema_name: str
    table_name: str
    columns: list[ColumnDef] = field(default_factory=list)
    unlogged: bool = False
    inherits: list[str] = field(default_factory=list)
    partition_by: str | None = None


@dataclass
class ExtensionDef:
    name: str
    schema_name: str = "public"


@dataclass
class EnumTypeDef:
    schema_name: str
    type_name: str
    labels: list[str] = field(default_factory=list)


@dataclass
class RuleDef:
    schema_name: str
    table_name: str
    rule_name: str
    event: str  # INSERT, UPDATE, DELETE, SELECT
    is_instead: bool = False


@dataclass
class ParsedSchema:
    """Complete in-memory representation of a pg_dump schema."""

    pg_version: str = ""
    tables: list[TableDef] = field(default_factory=list)
    constraints: list[ConstraintDef] = field(default_factory=list)
    indexes: list[IndexDef] = field(default_factory=list)
    sequences: list[SequenceDef] = field(default_factory=list)
    extensions: list[ExtensionDef] = field(default_factory=list)
    enum_types: list[EnumTypeDef] = field(default_factory=list)
    rules: list[RuleDef] = field(default_factory=list)

    def get_table(self, schema: str, name: str) -> TableDef | None:
        for t in self.tables:
            if t.schema_name == schema and t.table_name == name:
                return t
        return None

    def get_constraints_for_table(
        self, schema: str, name: str, con_type: str | None = None
    ) -> list[ConstraintDef]:
        result = [c for c in self.constraints if c.table_schema == schema and c.table_name == name]
        if con_type:
            result = [c for c in result if c.constraint_type == con_type]
        return result

    def get_indexes_for_table(self, schema: str, name: str) -> list[IndexDef]:
        return [i for i in self.indexes if i.table_schema == schema and i.table_name == name]


# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

_EXCLUDED_SCHEMAS = frozenset({"pg_catalog", "information_schema", "spock", "pg_toast"})

RE_PG_VERSION = re.compile(r"--\s*Dumped from database version (\S+)")
RE_SET_SEARCH_PATH = re.compile(
    r"SELECT\s+pg_catalog\.set_config\(\s*'search_path'\s*,\s*'([^']*)'"
    r"|SET\s+search_path\s*=\s*(.+?)\s*;",
    re.IGNORECASE,
)

RE_CREATE_EXTENSION = re.compile(
    r"CREATE\s+EXTENSION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)"
    r"(?:\s+(?:WITH\s+)?SCHEMA\s+(\S+))?",
    re.IGNORECASE,
)

RE_CREATE_TABLE = re.compile(
    r"CREATE\s+(UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"([\w\"]+(?:\.[\w\"]+)?)\s*\(",
    re.IGNORECASE,
)

RE_CREATE_SEQUENCE = re.compile(
    r"CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\"]+(?:\.[\w\"]+)?)",
    re.IGNORECASE,
)

RE_ALTER_ADD_CONSTRAINT = re.compile(
    r"ALTER\s+TABLE\s+(?:ONLY\s+)?([\w\"]+(?:\.[\w\"]+)?)\s+"
    r"ADD\s+CONSTRAINT\s+([\w\"]+)\s+"
    r"(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|EXCLUDE|CHECK)",
    re.IGNORECASE,
)

RE_CREATE_INDEX = re.compile(
    r"CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\"]+)\s+"
    r"ON\s+(?:ONLY\s+)?([\w\"]+(?:\.[\w\"]+)?)",
    re.IGNORECASE,
)

RE_ALTER_SET_DEFAULT = re.compile(
    r"ALTER\s+TABLE\s+(?:ONLY\s+)?([\w\"]+(?:\.[\w\"]+)?)\s+"
    r"ALTER\s+COLUMN\s+([\w\"]+)\s+SET\s+DEFAULT\s+(.+?)\s*;",
    re.IGNORECASE,
)

RE_ALTER_ADD_IDENTITY = re.compile(
    r"ALTER\s+TABLE\s+(?:ONLY\s+)?([\w\"]+(?:\.[\w\"]+)?)\s+"
    r"ALTER\s+COLUMN\s+([\w\"]+)\s+ADD\s+GENERATED\s+(ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY",
    re.IGNORECASE,
)

RE_ALTER_SEQ_OWNED = re.compile(
    r"ALTER\s+SEQUENCE\s+([\w\"]+(?:\.[\w\"]+)?)\s+"
    r"OWNED\s+BY\s+([\w\"]+(?:\.[\w\"]+)?)\.([\w\"]+)",
    re.IGNORECASE,
)

RE_CREATE_TYPE_ENUM = re.compile(
    r"CREATE\s+TYPE\s+([\w\"]+(?:\.[\w\"]+)?)\s+AS\s+ENUM\s*\(",
    re.IGNORECASE,
)

RE_CREATE_RULE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?RULE\s+([\w\"]+)\s+AS\s+ON\s+(\w+)\s+"
    r"TO\s+([\w\"]+(?:\.[\w\"]+)?)\s+DO\s+(INSTEAD\s+)?",
    re.IGNORECASE,
)

RE_FK_REFERENCES = re.compile(
    r"REFERENCES\s+([\w\"]+(?:\.[\w\"]+)?)\s*\(([^)]+)\)",
    re.IGNORECASE,
)

RE_FK_ON_DELETE = re.compile(
    r"ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)", re.IGNORECASE
)
RE_FK_ON_UPDATE = re.compile(
    r"ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)", re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unquote(name: str) -> str:
    """Strip double-quote wrappers from identifiers."""
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1]
    return name


def _split_qualified(name: str, default_schema: str = "public") -> tuple[str, str]:
    """Split 'schema.name' or just 'name' into (schema, name)."""
    if "." in name:
        parts = name.split(".", 1)
        return _unquote(parts[0]), _unquote(parts[1])
    return default_schema, _unquote(name)


def _parse_column_list(text: str) -> list[str]:
    """Parse '(col1, col2)' or 'col1, col2' into a list of column names."""
    text = text.strip()
    if text.startswith("("):
        text = text[1:]
    if text.endswith(")"):
        text = text[:-1]
    return [_unquote(c.strip()) for c in text.split(",") if c.strip()]


def _extract_paren_content(text: str) -> str:
    """Extract content inside first balanced parentheses."""
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == "(":
            if depth == 0:
                start = i + 1
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and start >= 0:
                return text[start:i]
    return ""


# ---------------------------------------------------------------------------
# Column parser (within CREATE TABLE body)
# ---------------------------------------------------------------------------

_RE_COLUMN_LINE = re.compile(r"^\s*([\w\"]+)\s+(.+)$")

_RE_NOT_NULL = re.compile(r"\bNOT\s+NULL\b", re.IGNORECASE)
_RE_DEFAULT = re.compile(r"\bDEFAULT\s+(.+?)(?:\s+NOT\s+NULL|\s+NULL|\s*,?\s*$)", re.IGNORECASE)
_RE_GENERATED = re.compile(r"\bGENERATED\s+ALWAYS\s+AS\s*\((.+?)\)\s+STORED", re.IGNORECASE)
_RE_IDENTITY_INLINE = re.compile(
    r"\bGENERATED\s+(ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY", re.IGNORECASE
)

# Table-level constraint keywords to skip in column parsing
_TABLE_CONSTRAINT_KW = re.compile(
    r"^\s*(PRIMARY\s+KEY|UNIQUE|FOREIGN\s+KEY|EXCLUDE|CHECK|CONSTRAINT)\b",
    re.IGNORECASE,
)


def _parse_column(line: str) -> ColumnDef | None:
    """Parse a single column definition line inside CREATE TABLE."""
    line = line.rstrip(",").strip()
    if not line or line.startswith("--"):
        return None
    if _TABLE_CONSTRAINT_KW.match(line):
        return None

    m = _RE_COLUMN_LINE.match(line)
    if not m:
        return None

    name = _unquote(m.group(1))
    rest = m.group(2)

    # Skip if name looks like a keyword that starts a table constraint
    if name.upper() in ("PRIMARY", "UNIQUE", "FOREIGN", "EXCLUDE", "CHECK", "CONSTRAINT"):
        return None

    # Extract data type — everything up to NOT NULL / DEFAULT / GENERATED / comma
    type_end = len(rest)
    for pat in (_RE_NOT_NULL, _RE_DEFAULT, _RE_GENERATED, _RE_IDENTITY_INLINE):
        pm = pat.search(rest)
        if pm and pm.start() < type_end:
            type_end = pm.start()

    data_type = rest[:type_end].strip().rstrip(",").strip()

    not_null = bool(_RE_NOT_NULL.search(rest))

    default_expr = None
    dm = _RE_DEFAULT.search(rest)
    if dm:
        default_expr = dm.group(1).strip().rstrip(",").strip()

    generated_expr = None
    gm = _RE_GENERATED.search(rest)
    if gm:
        generated_expr = gm.group(1).strip()

    identity = None
    im = _RE_IDENTITY_INLINE.search(rest)
    if im:
        identity = im.group(1).upper().replace("  ", " ")

    return ColumnDef(
        name=name,
        data_type=data_type,
        not_null=not_null,
        default_expr=default_expr,
        identity=identity,
        generated_expr=generated_expr,
    )


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


def parse_dump(file_path: str) -> ParsedSchema:
    """Parse a pg_dump --schema-only SQL file into a ParsedSchema."""
    schema = ParsedSchema()
    current_search_path = "public"

    text = Path(file_path).read_text(encoding="utf-8", errors="replace")
    # Join all lines and split on semicolons to get complete statements
    statements = _split_statements(text, schema, current_search_path)

    # Process buffered statements
    for stmt_text, search_path in statements:
        _process_statement(stmt_text, search_path, schema)

    return schema


def _split_statements(text: str, schema: ParsedSchema, search_path: str) -> list[tuple[str, str]]:
    """Split SQL text into (statement, search_path_at_time) tuples.

    Also extracts pg_version from header comments and tracks search_path.
    """
    results: list[tuple[str, str]] = []
    buf: list[str] = []
    in_dollar_quote = False
    dollar_tag = ""

    for line in text.splitlines():
        stripped = line.strip()

        # Extract PG version from comment header
        if stripped.startswith("--"):
            if not schema.pg_version:
                vm = RE_PG_VERSION.match(stripped)
                if vm:
                    schema.pg_version = vm.group(1)
            continue

        # Skip blank lines
        if not stripped:
            continue

        # Track search_path changes
        sp_m = RE_SET_SEARCH_PATH.match(stripped)
        if sp_m:
            sp_val = sp_m.group(1) or sp_m.group(2)
            if sp_val:
                # Take first non-empty schema from path
                parts = [p.strip().strip("'\"") for p in sp_val.split(",")]
                for p in parts:
                    if p and p not in ("pg_catalog", ""):
                        search_path = p
                        break

        # Handle dollar-quoted strings
        if not in_dollar_quote:
            # Check for dollar-quote start
            dq_m = re.search(r"(\$[\w]*\$)", stripped)
            if dq_m:
                tag = dq_m.group(1)
                # Check if it opens and closes on the same line
                rest_after = stripped[dq_m.end() :]
                if tag in rest_after:
                    pass  # self-closing on same line
                else:
                    in_dollar_quote = True
                    dollar_tag = tag

        elif dollar_tag in stripped:
            in_dollar_quote = False
            dollar_tag = ""

        buf.append(line)

        # Statement ends at semicolon (outside dollar quotes)
        if not in_dollar_quote and stripped.endswith(";"):
            stmt = "\n".join(buf)
            results.append((stmt, search_path))
            buf = []

    # Flush any remaining buffer
    if buf:
        stmt = "\n".join(buf)
        results.append((stmt, search_path))

    return results


def _process_statement(stmt: str, search_path: str, schema: ParsedSchema) -> None:
    """Process a single complete SQL statement."""
    upper = stmt.upper().strip()

    # CREATE EXTENSION
    m = RE_CREATE_EXTENSION.search(stmt)
    if m and upper.lstrip().startswith("CREATE"):
        name = _unquote(m.group(1)).rstrip(";")
        ext_schema = _unquote(m.group(2)).rstrip(";") if m.group(2) else "public"
        if name.lower() not in _EXCLUDED_SCHEMAS:
            schema.extensions.append(ExtensionDef(name=name, schema_name=ext_schema))
        return

    # CREATE TYPE AS ENUM
    m = RE_CREATE_TYPE_ENUM.search(stmt)
    if m:
        s, n = _split_qualified(m.group(1), search_path)
        if s not in _EXCLUDED_SCHEMAS:
            content = _extract_paren_content(stmt[m.start() :])
            labels = [lbl.strip().strip("'") for lbl in content.split(",") if lbl.strip()]
            schema.enum_types.append(EnumTypeDef(schema_name=s, type_name=n, labels=labels))
        return

    # CREATE SEQUENCE
    m = RE_CREATE_SEQUENCE.search(stmt)
    if m and "CREATE SEQUENCE" in upper:
        s, n = _split_qualified(m.group(1), search_path)
        if s not in _EXCLUDED_SCHEMAS:
            seq = SequenceDef(schema_name=s, sequence_name=n)
            # Parse options from the statement
            as_m = re.search(r"\bAS\s+(smallint|integer|bigint)\b", stmt, re.IGNORECASE)
            if as_m:
                seq.data_type = as_m.group(1).lower()
            start_m = re.search(r"\bSTART\s+WITH\s+(\d+)", stmt, re.IGNORECASE)
            if start_m:
                seq.start_value = int(start_m.group(1))
            inc_m = re.search(r"\bINCREMENT\s+BY\s+(\d+)", stmt, re.IGNORECASE)
            if inc_m:
                seq.increment = int(inc_m.group(1))
            min_m = re.search(r"\bMINVALUE\s+(\d+)", stmt, re.IGNORECASE)
            if min_m:
                seq.min_value = int(min_m.group(1))
            max_m = re.search(r"\bMAXVALUE\s+(\d+)", stmt, re.IGNORECASE)
            if max_m:
                seq.max_value = int(max_m.group(1))
            if re.search(r"\bCYCLE\b", stmt, re.IGNORECASE) and not re.search(
                r"\bNO\s+CYCLE\b", stmt, re.IGNORECASE
            ):
                seq.cycle = True
            schema.sequences.append(seq)
        return

    # CREATE TABLE
    m = RE_CREATE_TABLE.search(stmt)
    if m and re.match(r"\s*CREATE\s", stmt, re.IGNORECASE):
        unlogged = bool(m.group(1))
        s, n = _split_qualified(m.group(2), search_path)
        if s in _EXCLUDED_SCHEMAS:
            return

        tbl = TableDef(schema_name=s, table_name=n, unlogged=unlogged)

        # Extract column body
        body = _extract_paren_content(stmt)
        if body:
            _parse_table_body(body, tbl, s, schema)

        # Check for INHERITS
        inh_m = re.search(r"\)\s*INHERITS\s*\(([^)]+)\)", stmt, re.IGNORECASE)
        if inh_m:
            tbl.inherits = [p.strip() for p in inh_m.group(1).split(",")]

        # Check for PARTITION BY
        part_m = re.search(
            r"\)\s*(?:INHERITS\s*\([^)]*\)\s*)?PARTITION\s+BY\s+(.+?)(?:\s*;|$)",
            stmt,
            re.IGNORECASE,
        )
        if part_m:
            tbl.partition_by = part_m.group(1).strip().rstrip(";").strip()

        schema.tables.append(tbl)
        return

    # ALTER TABLE ADD CONSTRAINT
    m = RE_ALTER_ADD_CONSTRAINT.search(stmt)
    if m:
        s, n = _split_qualified(m.group(1), search_path)
        if s in _EXCLUDED_SCHEMAS:
            return
        con_name = _unquote(m.group(2))
        con_type = m.group(3).upper().replace("  ", " ")

        con = ConstraintDef(
            name=con_name,
            constraint_type=con_type,
            table_schema=s,
            table_name=n,
        )

        # Extract columns from parentheses after constraint type keyword
        after_type = stmt[m.end() :]
        col_content = _extract_paren_content(after_type)
        if col_content and con_type != "EXCLUDE":
            con.columns = _parse_column_list(col_content)

        # FK specifics
        if con_type == "FOREIGN KEY":
            ref_m = RE_FK_REFERENCES.search(after_type)
            if ref_m:
                rs, rn = _split_qualified(ref_m.group(1), search_path)
                con.ref_schema = rs
                con.ref_table = rn
                con.ref_columns = _parse_column_list(ref_m.group(2))
            del_m = RE_FK_ON_DELETE.search(after_type)
            if del_m:
                con.on_delete = del_m.group(1).upper().replace("  ", " ")
            upd_m = RE_FK_ON_UPDATE.search(after_type)
            if upd_m:
                con.on_update = upd_m.group(1).upper().replace("  ", " ")

        # Deferrable
        if re.search(r"\bDEFERRABLE\b", stmt, re.IGNORECASE) and not re.search(
            r"\bNOT\s+DEFERRABLE\b", stmt, re.IGNORECASE
        ):
            con.deferrable = True
        if re.search(r"\bINITIALLY\s+DEFERRED\b", stmt, re.IGNORECASE):
            con.initially_deferred = True

        schema.constraints.append(con)
        return

    # CREATE INDEX
    m = RE_CREATE_INDEX.search(stmt)
    if m:
        is_unique = bool(m.group(1))
        idx_name = _unquote(m.group(2))
        s, n = _split_qualified(m.group(3), search_path)
        if s in _EXCLUDED_SCHEMAS:
            return

        idx = IndexDef(
            name=idx_name,
            table_schema=s,
            table_name=n,
            is_unique=is_unique,
        )

        # Extract method
        method_m = re.search(r"\bUSING\s+(\w+)", stmt, re.IGNORECASE)
        if method_m:
            idx.index_method = method_m.group(1).lower()

        # Extract columns
        after_table = stmt[m.end() :]
        col_content = _extract_paren_content(after_table)
        if col_content:
            idx.columns = _parse_column_list(col_content)

        schema.indexes.append(idx)
        return

    # ALTER TABLE SET DEFAULT (bind sequence to column)
    m = RE_ALTER_SET_DEFAULT.search(stmt)
    if m:
        s, n = _split_qualified(m.group(1), search_path)
        col = _unquote(m.group(2))
        default_expr = m.group(3).strip()
        if s not in _EXCLUDED_SCHEMAS:
            tbl = schema.get_table(s, n)
            if tbl:
                for c in tbl.columns:
                    if c.name == col:
                        c.default_expr = default_expr
                        break
        return

    # ALTER TABLE ADD GENERATED AS IDENTITY
    m = RE_ALTER_ADD_IDENTITY.search(stmt)
    if m:
        s, n = _split_qualified(m.group(1), search_path)
        col = _unquote(m.group(2))
        identity_type = m.group(3).upper().replace("  ", " ")
        if s not in _EXCLUDED_SCHEMAS:
            tbl = schema.get_table(s, n)
            if tbl:
                for c in tbl.columns:
                    if c.name == col:
                        c.identity = identity_type
                        break
        return

    # ALTER SEQUENCE OWNED BY
    m = RE_ALTER_SEQ_OWNED.search(stmt)
    if m:
        seq_s, seq_n = _split_qualified(m.group(1), search_path)
        tbl_s, tbl_n = _split_qualified(m.group(2), search_path)
        col = _unquote(m.group(3))
        for seq in schema.sequences:
            if seq.schema_name == seq_s and seq.sequence_name == seq_n:
                seq.owned_by_table = f"{tbl_s}.{tbl_n}"
                seq.owned_by_column = col
                break
        return

    # CREATE RULE
    m = RE_CREATE_RULE.search(stmt)
    if m:
        rule_name = _unquote(m.group(1))
        event = m.group(2).upper()
        s, n = _split_qualified(m.group(3), search_path)
        is_instead = bool(m.group(4))
        if s not in _EXCLUDED_SCHEMAS:
            schema.rules.append(
                RuleDef(
                    schema_name=s,
                    table_name=n,
                    rule_name=rule_name,
                    event=event,
                    is_instead=is_instead,
                )
            )
        return


def _parse_table_body(body: str, tbl: TableDef, search_path: str, schema: ParsedSchema) -> None:
    """Parse the body (between parens) of a CREATE TABLE statement."""
    # Split on commas, respecting parenthesis depth
    parts = _split_body_parts(body)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Check for inline table constraints (PRIMARY KEY, UNIQUE, etc.)
        if _TABLE_CONSTRAINT_KW.match(part):
            _parse_inline_constraint(part, tbl, search_path, schema)
            continue

        col = _parse_column(part)
        if col:
            tbl.columns.append(col)


def _split_body_parts(body: str) -> list[str]:
    """Split CREATE TABLE body on top-level commas."""
    parts = []
    depth = 0
    current: list[str] = []

    for ch in body:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current))

    return parts


def _parse_inline_constraint(
    text: str, tbl: TableDef, search_path: str, schema: ParsedSchema
) -> None:
    """Parse inline table-level constraints within CREATE TABLE body."""

    # CONSTRAINT name TYPE (cols)
    con_name_m = re.match(r"CONSTRAINT\s+([\w\"]+)\s+", text, re.IGNORECASE)
    if con_name_m:
        con_name = _unquote(con_name_m.group(1))
        rest = text[con_name_m.end() :]
    else:
        con_name = ""
        rest = text

    rest_upper = rest.upper().strip()

    if rest_upper.startswith("PRIMARY KEY"):
        col_content = _extract_paren_content(rest)
        schema.constraints.append(
            ConstraintDef(
                name=con_name,
                constraint_type="PRIMARY KEY",
                table_schema=tbl.schema_name,
                table_name=tbl.table_name,
                columns=_parse_column_list(col_content) if col_content else [],
            )
        )
    elif rest_upper.startswith("UNIQUE"):
        col_content = _extract_paren_content(rest)
        con = ConstraintDef(
            name=con_name,
            constraint_type="UNIQUE",
            table_schema=tbl.schema_name,
            table_name=tbl.table_name,
            columns=_parse_column_list(col_content) if col_content else [],
        )
        if re.search(r"\bDEFERRABLE\b", rest, re.IGNORECASE) and not re.search(
            r"\bNOT\s+DEFERRABLE\b", rest, re.IGNORECASE
        ):
            con.deferrable = True
        schema.constraints.append(con)
    elif rest_upper.startswith("FOREIGN KEY"):
        col_content = _extract_paren_content(rest)
        con = ConstraintDef(
            name=con_name,
            constraint_type="FOREIGN KEY",
            table_schema=tbl.schema_name,
            table_name=tbl.table_name,
            columns=_parse_column_list(col_content) if col_content else [],
        )
        ref_m = RE_FK_REFERENCES.search(rest)
        if ref_m:
            rs, rn = _split_qualified(ref_m.group(1), search_path)
            con.ref_schema = rs
            con.ref_table = rn
            con.ref_columns = _parse_column_list(ref_m.group(2))
        del_m = RE_FK_ON_DELETE.search(rest)
        if del_m:
            con.on_delete = del_m.group(1).upper()
        upd_m = RE_FK_ON_UPDATE.search(rest)
        if upd_m:
            con.on_update = upd_m.group(1).upper()
        schema.constraints.append(con)
