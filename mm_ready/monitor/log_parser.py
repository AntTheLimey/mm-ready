"""PostgreSQL log file parser for SQL pattern extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LogStatement:
    line_number: int
    timestamp: str
    statement: str
    duration_ms: float | None = None


@dataclass
class LogAnalysis:
    total_statements: int = 0
    ddl_statements: list[LogStatement] = field(default_factory=list)
    truncate_cascade: list[LogStatement] = field(default_factory=list)
    create_temp_table: list[LogStatement] = field(default_factory=list)
    advisory_locks: list[LogStatement] = field(default_factory=list)
    concurrent_indexes: list[LogStatement] = field(default_factory=list)
    other_notable: list[LogStatement] = field(default_factory=list)

    @property
    def has_findings(self) -> bool:
        return any(
            [
                self.ddl_statements,
                self.truncate_cascade,
                self.create_temp_table,
                self.advisory_locks,
                self.concurrent_indexes,
                self.other_notable,
            ]
        )


# Patterns for PostgreSQL log formats
_LOG_LINE_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}[.\d]*\s*\w*)\s+"
    r".*?(?:LOG|STATEMENT|ERROR):\s+(.*)",
    re.IGNORECASE,
)
_DURATION_PATTERN = re.compile(r"duration:\s+([\d.]+)\s+ms", re.IGNORECASE)
_STATEMENT_PATTERN = re.compile(r"(?:statement|execute\s+\w+):\s+(.*)", re.IGNORECASE)

# Patterns to look for
_DDL_PATTERN = re.compile(
    r"\b(CREATE|ALTER|DROP)\s+(TABLE|INDEX|VIEW|FUNCTION|PROCEDURE|TRIGGER|TYPE|SCHEMA|SEQUENCE)\b",
    re.IGNORECASE,
)
_TRUNCATE_CASCADE = re.compile(r"\bTRUNCATE\b.*\bCASCADE\b", re.IGNORECASE)
_TEMP_TABLE = re.compile(r"\bCREATE\s+(TEMP|TEMPORARY)\s+TABLE\b", re.IGNORECASE)
_ADVISORY_LOCK = re.compile(r"\bpg_(try_)?advisory_lock", re.IGNORECASE)
_CONCURRENT_INDEX = re.compile(r"\bCREATE\s+INDEX\s+CONCURRENTLY\b", re.IGNORECASE)


def parse_log_file(log_path: str) -> LogAnalysis:
    """Parse a PostgreSQL log file and extract notable SQL patterns.

    Supports standard PostgreSQL log format (log_line_prefix with timestamp).
    """
    path = Path(log_path)
    if not path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    analysis = LogAnalysis()
    current_stmt = ""
    current_ts = ""
    current_line = 0

    with open(path, errors="replace") as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip()

            # Try to match a log line with timestamp
            match = _LOG_LINE_PATTERN.match(line)
            if match:
                # Process previous statement if any
                if current_stmt:
                    _classify_statement(analysis, current_stmt, current_ts, current_line)

                current_ts = match.group(1)
                content = match.group(2)
                current_line = line_num

                # Extract statement
                stmt_match = _STATEMENT_PATTERN.match(content)
                if stmt_match:
                    current_stmt = stmt_match.group(1)
                    analysis.total_statements += 1
                else:
                    current_stmt = content
            elif current_stmt and line.startswith("\t"):
                # Continuation line
                current_stmt += " " + line.strip()

    # Process last statement
    if current_stmt:
        _classify_statement(analysis, current_stmt, current_ts, current_line)

    return analysis


def _classify_statement(analysis: LogAnalysis, stmt: str, ts: str, line: int):
    """Classify a SQL statement into relevant categories."""
    entry = LogStatement(line_number=line, timestamp=ts, statement=stmt[:500])

    if _DDL_PATTERN.search(stmt):
        # Check for concurrent index specifically
        if _CONCURRENT_INDEX.search(stmt):
            analysis.concurrent_indexes.append(entry)
        else:
            analysis.ddl_statements.append(entry)

    if _TRUNCATE_CASCADE.search(stmt):
        analysis.truncate_cascade.append(entry)

    if _TEMP_TABLE.search(stmt):
        analysis.create_temp_table.append(entry)

    if _ADVISORY_LOCK.search(stmt):
        analysis.advisory_locks.append(entry)
