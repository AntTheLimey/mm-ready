"""CLI entry point for mm-ready."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import TYPE_CHECKING

from mm_ready import __version__

if TYPE_CHECKING:
    from mm_ready.config import CheckConfig, ReportConfig
    from mm_ready.models import ScanReport

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
    _add_check_filter_args(scan_parser)
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
    _add_check_filter_args(audit_parser)
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
    _add_check_filter_args(mon_parser)
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
    _add_check_filter_args(analyze_parser)
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
    list_parser.add_argument(
        "--exclude",
        help="Comma-separated list of check names to exclude",
    )
    list_parser.add_argument(
        "--include-only",
        help="Comma-separated list of check names to show (whitelist mode)",
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
    """
    Add output-related CLI arguments to the given argument parser.

    This adds an "output" argument group with:
    - --format / -f: report format choice among "json", "markdown", and "html" (default: "html").
    - --output / -o: output file path pattern; if omitted a default path pattern is used (./reports/<dbname>_<timestamp>.<ext>).

    Parameters:
        parser (argparse.ArgumentParser): The argument parser to modify; arguments are added in-place.
    """
    grp = parser.add_argument_group("output")
    grp.add_argument(
        "--format",
        "-f",
        choices=["json", "markdown", "html"],
        default="html",
        help="Report format (default: html)",
    )
    grp.add_argument(
        "--output", "-o", help="Output file path (default: ./reports/<dbname>_<timestamp>.<ext>)"
    )
    grp.add_argument(
        "--no-todo",
        action="store_true",
        help="Omit the To Do list from the report",
    )
    grp.add_argument(
        "--todo-include-consider",
        action="store_true",
        help="Include CONSIDER severity items in the To Do list",
    )


def _add_check_filter_args(parser: argparse.ArgumentParser) -> None:
    """Add check filtering arguments (exclude, include-only, config)."""
    grp = parser.add_argument_group("check filtering")
    grp.add_argument(
        "--exclude",
        help="Comma-separated list of check names to exclude",
    )
    grp.add_argument(
        "--include-only",
        help="Comma-separated list of check names to run (whitelist mode)",
    )
    grp.add_argument(
        "--config",
        help="Path to configuration file (default: mm-ready.yaml)",
    )
    grp.add_argument(
        "--no-config",
        action="store_true",
        help="Skip loading configuration file",
    )


def main(argv: list[str] | None = None):
    """
    Entry point for the CLI: parse arguments, select a command, and dispatch to the corresponding handler.

    Parses argv (or sys.argv[1:] when argv is None), rewrites the arguments to default to the "scan" command when the first token is not a recognized subcommand or a top-level help/version flag, and invokes the appropriate command handler (_cmd_list_checks, _cmd_scan, _cmd_audit, _cmd_analyze, or _cmd_monitor). If no arguments are provided or no command is selected after parsing, prints help and exits with status code 1.

    Parameters:
        argv (list[str] | None): Optional list of command-line arguments to parse; when None, uses the process arguments (sys.argv[1:]).
    """
    parser = build_parser()

    # Default to "scan" when no subcommand is given but arguments are present
    raw_args = argv if argv is not None else sys.argv[1:]
    known_commands = {"scan", "audit", "monitor", "analyze", "list-checks"}
    if (
        raw_args
        and raw_args[0] not in known_commands
        and raw_args[0] not in ("--version", "--help", "-h")
    ):
        raw_args = ["scan", *list(raw_args)]
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

    # Load and merge configuration
    check_cfg, report_cfg = _load_and_merge_config(args, "analyze")

    report = run_analyze(
        schema,
        file_path=args.file,
        categories=categories,
        verbose=args.verbose,
        exclude=check_cfg.exclude,
        include_only=check_cfg.include_only,
    )

    output = _render_report(report, args.format, report_cfg)
    _write_output(output, args, mode="analyze", dbname=report.database)


def _cmd_list_checks(args):
    from mm_ready.registry import discover_checks

    categories = args.categories.split(",") if args.categories else None
    mode = args.mode if args.mode != "all" else None

    # Parse exclude/include-only
    exclude = None
    if getattr(args, "exclude", None):
        exclude = set(args.exclude.split(","))

    include_only = None
    if getattr(args, "include_only", None):
        include_only = set(args.include_only.split(","))

    checks = discover_checks(
        categories=categories, mode=mode, exclude=exclude, include_only=include_only
    )

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
    """
    Establishes a database connection, runs a scan in the specified mode, renders the resulting report, and writes the output.

    Parameters:
        args: argparse.Namespace with connection and output options. Expected attributes:
            - dsn, host, port, dbname, user, password: database connection parameters.
            - categories: comma-separated category list or None.
            - format: output format ("json", "markdown", "html").
            - verbose: verbosity flag.
            - output: optional output path.
            - exclude, include_only, config, no_config: check filtering options.
            - no_todo, todo_include_consider: report options.
        mode (str): Scan mode to run (e.g., "scan" or "audit").

    Behavior:
        - Parses categories from args.categories when present.
        - Loads configuration and merges with CLI arguments.
        - Attempts to connect to the database; on connection failure prints an error and contextual hints to stderr and exits with status 1.
        - Ensures the database connection is closed after the scan completes.
        - Renders the scan report into the requested format and writes it to the resolved output path (or stdout).
    """
    import psycopg2

    from mm_ready.connection import connect
    from mm_ready.scanner import run_scan

    categories = args.categories.split(",") if args.categories else None

    # Load and merge configuration
    check_cfg, report_cfg = _load_and_merge_config(args, mode)

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
        print("Error: Could not connect to database.", file=sys.stderr)
        print(f"       {error_msg}", file=sys.stderr)
        if "no password supplied" in error_msg:
            print(
                "\nHint: Use --password to provide a password, or set PGPASSWORD environment variable.",
                file=sys.stderr,
            )
        elif "does not exist" in error_msg:
            print("\nHint: Check that the database name is correct.", file=sys.stderr)
        elif "Connection refused" in error_msg or "could not connect" in error_msg.lower():
            print(
                f"\nHint: Check that PostgreSQL is running on {args.host or 'localhost'}:{args.port or 5432}.",
                file=sys.stderr,
            )
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
            exclude=check_cfg.exclude,
            include_only=check_cfg.include_only,
        )
    finally:
        conn.close()

    output = _render_report(report, args.format, report_cfg)
    _write_output(output, args, mode=mode, dbname=report.database)


def _cmd_monitor(args):
    """
    Run the "monitor" CLI command: connect to the database, perform monitoring, and write the rendered report.

    Attempts to establish a database connection using values from `args`; on connection failure prints a user-friendly error and exits. On success, runs the monitor observer to collect a report, closes the connection, renders the report in the requested format, and writes the output using the CLI output rules.

    Parameters:
        args: Namespace
            Parsed CLI arguments containing connection fields (`dsn`, `host`, `port`, `dbname`, `user`, `password`),
            monitor options (`duration`, `log_file`, `verbose`), and output options (`format`, `output`).
    """
    import psycopg2

    from mm_ready.connection import connect
    from mm_ready.monitor.observer import run_monitor

    # Load and merge configuration (check_cfg not used by monitor currently)
    _check_cfg, report_cfg = _load_and_merge_config(args, "monitor")

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
        print("Error: Could not connect to database.", file=sys.stderr)
        print(f"       {error_msg}", file=sys.stderr)
        if "no password supplied" in error_msg:
            print(
                "\nHint: Use --password to provide a password, or set PGPASSWORD environment variable.",
                file=sys.stderr,
            )
        elif "does not exist" in error_msg:
            print("\nHint: Check that the database name is correct.", file=sys.stderr)
        elif "Connection refused" in error_msg or "could not connect" in error_msg.lower():
            print(
                f"\nHint: Check that PostgreSQL is running on {args.host or 'localhost'}:{args.port or 5432}.",
                file=sys.stderr,
            )
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

    output = _render_report(report, args.format, report_cfg)
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


def _parse_csv_set(value: str | None) -> set[str] | None:
    """Parse a comma-separated string into a set, stripping whitespace."""
    if not value:
        return None
    items = {item.strip() for item in value.split(",") if item.strip()}
    return items or None


def _load_and_merge_config(args: argparse.Namespace, mode: str) -> tuple[CheckConfig, ReportConfig]:
    """Load config file and merge with CLI arguments."""
    from mm_ready.config import load_config, merge_cli_with_config

    # Load config (skip if --no-config)
    no_config = getattr(args, "no_config", False)
    config_path = getattr(args, "config", None)

    if no_config:
        config = None
    else:
        try:
            config = load_config(config_path, auto_discover=(config_path is None))
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    if config is None:
        # Create default config
        from mm_ready.config import Config

        config = Config()

    # Parse CLI args
    cli_exclude = _parse_csv_set(getattr(args, "exclude", None))
    cli_include_only = _parse_csv_set(getattr(args, "include_only", None))

    cli_no_todo = getattr(args, "no_todo", False)
    cli_todo_include_consider = getattr(args, "todo_include_consider", False)

    return merge_cli_with_config(
        config,
        mode,
        cli_exclude=cli_exclude,
        cli_include_only=cli_include_only,
        cli_no_todo=cli_no_todo,
        cli_todo_include_consider=cli_todo_include_consider,
    )


def _render_report(report: ScanReport, fmt: str, report_cfg: ReportConfig | None = None) -> str:
    if fmt == "json":
        from mm_ready.reporters.json_reporter import render

        return render(report)
    elif fmt == "markdown":
        from mm_ready.reporters.markdown_reporter import render

        return render(report)
    elif fmt == "html":
        from mm_ready.reporters.html_reporter import render

        return render(report, report_cfg=report_cfg)
    else:
        raise ValueError(f"Unknown format: {fmt}")
