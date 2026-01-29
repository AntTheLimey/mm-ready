"""CLI entry point for mm-ready."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from mm_ready import __version__

# File extensions per output format
_FORMAT_EXT = {"json": ".json", "markdown": ".md", "html": ".html"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mm-ready",
        description="Scan a PostgreSQL database for Spock 5 multi-master readiness.",
    )
    parser.add_argument("--version", action="version", version=f"mm-ready {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Available commands (default: scan)")

    # -- scan --
    scan_parser = subparsers.add_parser(
        "scan", help="Pre-Spock readiness scan (target: vanilla PostgreSQL)"
    )
    _add_connection_args(scan_parser)
    _add_output_args(scan_parser)
    scan_parser.add_argument(
        "--categories",
        help="Comma-separated list of check categories to run (default: all)",
    )
    scan_parser.add_argument("--verbose", "-v", action="store_true", help="Print progress")

    # -- audit --
    audit_parser = subparsers.add_parser(
        "audit", help="Post-Spock audit (target: database with Spock already installed)"
    )
    _add_connection_args(audit_parser)
    _add_output_args(audit_parser)
    audit_parser.add_argument(
        "--categories",
        help="Comma-separated list of check categories to run (default: all)",
    )
    audit_parser.add_argument("--verbose", "-v", action="store_true", help="Print progress")

    # -- monitor --
    mon_parser = subparsers.add_parser(
        "monitor", help="Observe SQL activity over a time window then report"
    )
    _add_connection_args(mon_parser)
    _add_output_args(mon_parser)
    mon_parser.add_argument(
        "--duration",
        type=int,
        default=3600,
        help="Observation duration in seconds (default: 3600)",
    )
    mon_parser.add_argument(
        "--log-file",
        help="Path to PostgreSQL log file for log-based observation",
    )
    mon_parser.add_argument("--verbose", "-v", action="store_true", help="Print progress")

    # -- analyze --
    analyze_parser = subparsers.add_parser(
        "analyze", help="Offline schema dump analysis (no database connection required)"
    )
    analyze_parser.add_argument(
        "--file", required=True, help="Path to pg_dump --schema-only SQL file"
    )
    _add_output_args(analyze_parser)
    analyze_parser.add_argument(
        "--categories",
        help="Comma-separated list of check categories to run (default: all)",
    )
    analyze_parser.add_argument("--verbose", "-v", action="store_true", help="Print progress")

    # -- list-checks --
    list_parser = subparsers.add_parser("list-checks", help="List all available checks")
    list_parser.add_argument(
        "--categories",
        help="Comma-separated list of categories to filter",
    )
    list_parser.add_argument(
        "--mode",
        choices=["scan", "audit", "all"],
        default="all",
        help="Filter checks by mode (default: all)",
    )

    return parser


def _add_connection_args(parser: argparse.ArgumentParser):
    grp = parser.add_argument_group("connection")
    grp.add_argument("--dsn", help="PostgreSQL connection URI (postgres://...)")
    grp.add_argument("--host", "-H", default=None, help="Database host")
    grp.add_argument("--port", "-p", type=int, default=5432, help="Database port (default: 5432)")
    grp.add_argument("--dbname", "-d", default=None, help="Database name")
    grp.add_argument("--user", "-U", default=None, help="Database user")
    grp.add_argument("--password", "-W", default=None, help="Database password")


def _add_output_args(parser: argparse.ArgumentParser):
    grp = parser.add_argument_group("output")
    grp.add_argument(
        "--format",
        "-f",
        choices=["json", "markdown", "html"],
        default="html",
        help="Report format (default: html)",
    )
    grp.add_argument("--output", "-o", help="Output file path (default: ./reports/<dbname>_<timestamp>.<ext>)")


def main(argv: list[str] | None = None):
    parser = build_parser()

    # Default to "scan" when no subcommand is given but arguments are present
    raw_args = argv if argv is not None else sys.argv[1:]
    known_commands = {"scan", "audit", "monitor", "analyze", "list-checks"}
    if raw_args and raw_args[0] not in known_commands and raw_args[0] not in ("--version", "--help", "-h"):
        raw_args = ["scan"] + list(raw_args)
    elif not raw_args:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(raw_args)

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "list-checks":
        _cmd_list_checks(args)
    elif args.command == "scan":
        _cmd_scan(args)
    elif args.command == "audit":
        _cmd_audit(args)
    elif args.command == "analyze":
        _cmd_analyze(args)
    elif args.command == "monitor":
        _cmd_monitor(args)


def _cmd_analyze(args):
    from mm_ready.analyzer import run_analyze
    from mm_ready.schema_parser import parse_dump

    if not os.path.isfile(args.file):
        print(f"Error: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    schema = parse_dump(args.file)
    categories = args.categories.split(",") if args.categories else None

    report = run_analyze(
        schema,
        file_path=args.file,
        categories=categories,
        verbose=args.verbose,
    )

    output = _render_report(report, args.format)
    _write_output(output, args, mode="analyze", dbname=report.database)


def _cmd_list_checks(args):
    from mm_ready.registry import discover_checks

    categories = args.categories.split(",") if args.categories else None
    mode = args.mode if args.mode != "all" else None
    checks = discover_checks(categories=categories, mode=mode)

    if not checks:
        print("No checks found.")
        return

    current_cat = None
    for check in checks:
        if check.category != current_cat:
            current_cat = check.category
            print(f"\n[{current_cat}]")
        mode_tag = f"[{check.mode}]" if check.mode != "scan" else ""
        print(f"  {check.name:30s} {mode_tag:8s} {check.description}")


def _cmd_scan(args):
    _run_mode(args, mode="scan")


def _cmd_audit(args):
    _run_mode(args, mode="audit")


def _run_mode(args, mode: str):
    import psycopg2
    from mm_ready.connection import connect
    from mm_ready.scanner import run_scan

    categories = args.categories.split(",") if args.categories else None

    try:
        conn = connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            dsn=args.dsn,
        )
    except psycopg2.OperationalError as e:
        error_msg = str(e).strip()
        print(f"Error: Could not connect to database.", file=sys.stderr)
        print(f"       {error_msg}", file=sys.stderr)
        if "no password supplied" in error_msg:
            print("\nHint: Use --password to provide a password, or set PGPASSWORD environment variable.", file=sys.stderr)
        elif "does not exist" in error_msg:
            print("\nHint: Check that the database name is correct.", file=sys.stderr)
        elif "Connection refused" in error_msg or "could not connect" in error_msg.lower():
            print(f"\nHint: Check that PostgreSQL is running on {args.host or 'localhost'}:{args.port or 5432}.", file=sys.stderr)
        sys.exit(1)

    try:
        report = run_scan(
            conn,
            host=args.host or "localhost",
            port=args.port,
            dbname=args.dbname or conn.info.dbname,
            categories=categories,
            mode=mode,
            verbose=args.verbose,
        )
    finally:
        conn.close()

    output = _render_report(report, args.format)
    _write_output(output, args, mode=mode, dbname=report.database)


def _cmd_monitor(args):
    import psycopg2
    from mm_ready.connection import connect
    from mm_ready.monitor.observer import run_monitor

    try:
        conn = connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            dsn=args.dsn,
        )
    except psycopg2.OperationalError as e:
        error_msg = str(e).strip()
        print(f"Error: Could not connect to database.", file=sys.stderr)
        print(f"       {error_msg}", file=sys.stderr)
        if "no password supplied" in error_msg:
            print("\nHint: Use --password to provide a password, or set PGPASSWORD environment variable.", file=sys.stderr)
        elif "does not exist" in error_msg:
            print("\nHint: Check that the database name is correct.", file=sys.stderr)
        elif "Connection refused" in error_msg or "could not connect" in error_msg.lower():
            print(f"\nHint: Check that PostgreSQL is running on {args.host or 'localhost'}:{args.port or 5432}.", file=sys.stderr)
        sys.exit(1)

    try:
        report = run_monitor(
            conn,
            host=args.host or "localhost",
            port=args.port,
            dbname=args.dbname or conn.info.dbname,
            duration=args.duration,
            log_file=args.log_file,
            verbose=args.verbose,
        )
    finally:
        conn.close()

    output = _render_report(report, args.format)
    _write_output(output, args, mode="monitor", dbname=report.database)


def _write_output(output: str, args, mode: str = "scan", dbname: str = ""):
    """Write report to file (with timestamped name) or stdout."""
    if args.output:
        path = _make_output_path(args.output, args.format, dbname)
    else:
        # Default: write to ./reports/<dbname>_<timestamp>.<ext>
        path = _make_default_output_path(args.format, dbname)

    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w") as f:
        f.write(output)
    print(f"Report written to {path}", file=sys.stderr)


def _make_default_output_path(fmt: str, dbname: str) -> str:
    """Generate a default output path: ./reports/<dbname>_<timestamp>.<ext>."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = _FORMAT_EXT.get(fmt, "")
    name = dbname or "mm-ready"
    return os.path.join("reports", f"{name}_{ts}{ext}")


def _make_output_path(user_path: str, fmt: str, dbname: str = "") -> str:
    """Insert a timestamp into the output filename.

    If the user provides a path like ``report.html``, the result is
    ``report_20260127_131504.html``.  If they provide a bare directory,
    the file is placed there with an auto-generated name.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = _FORMAT_EXT.get(fmt, "")
    name = dbname or "mm-ready"

    if os.path.isdir(user_path):
        return os.path.join(user_path, f"{name}_{ts}{ext}")

    base, existing_ext = os.path.splitext(user_path)
    if not existing_ext:
        existing_ext = ext
    return f"{base}_{ts}{existing_ext}"


def _render_report(report, fmt: str) -> str:
    if fmt == "json":
        from mm_ready.reporters.json_reporter import render
    elif fmt == "markdown":
        from mm_ready.reporters.markdown_reporter import render
    elif fmt == "html":
        from mm_ready.reporters.html_reporter import render
    else:
        raise ValueError(f"Unknown format: {fmt}")
    return render(report)
