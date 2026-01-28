"""Tests for mm_ready.cli — argument parsing and output path generation."""

from __future__ import annotations

import os
import re
import sys

import pytest

from mm_ready.cli import (
    build_parser,
    main,
    _make_default_output_path,
    _make_output_path,
)


class TestBuildParser:
    def test_returns_parser(self):
        parser = build_parser()
        assert parser is not None

    def test_format_default_is_html(self):
        parser = build_parser()
        args = parser.parse_args(["scan", "--host", "x"])
        assert args.format == "html"

    def test_subcommands_exist(self):
        parser = build_parser()
        for cmd in ["scan", "audit", "monitor", "list-checks"]:
            args = parser.parse_args([cmd] if cmd == "list-checks" else [cmd, "--host", "x"])
            assert args.command == cmd

    def test_analyze_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["analyze", "--file", "test.sql"])
        assert args.command == "analyze"
        assert args.file == "test.sql"
        assert args.format == "html"  # default

    def test_port_default(self):
        parser = build_parser()
        args = parser.parse_args(["scan", "--host", "x"])
        assert args.port == 5432

    def test_monitor_duration_default(self):
        parser = build_parser()
        args = parser.parse_args(["monitor", "--host", "x"])
        assert args.duration == 3600


class TestDefaultToScan:
    def test_host_arg_defaults_to_scan(self):
        """main() prepends 'scan' when first arg isn't a known command."""
        parser = build_parser()
        # Simulate what main() does
        raw_args = ["--host", "example.com"]
        known_commands = {"scan", "audit", "monitor", "analyze", "list-checks"}
        if raw_args and raw_args[0] not in known_commands and raw_args[0] not in ("--version", "--help", "-h"):
            raw_args = ["scan"] + list(raw_args)
        args = parser.parse_args(raw_args)
        assert args.command == "scan"
        assert args.host == "example.com"

    def test_no_args_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 1

    def test_help_not_prepended(self):
        """--help should not get 'scan' prepended."""
        raw_args = ["--help"]
        known_commands = {"scan", "audit", "monitor", "analyze", "list-checks"}
        should_prepend = (
            raw_args
            and raw_args[0] not in known_commands
            and raw_args[0] not in ("--version", "--help", "-h")
        )
        assert not should_prepend

    def test_version_not_prepended(self):
        raw_args = ["--version"]
        known_commands = {"scan", "audit", "monitor", "analyze", "list-checks"}
        should_prepend = (
            raw_args
            and raw_args[0] not in known_commands
            and raw_args[0] not in ("--version", "--help", "-h")
        )
        assert not should_prepend


class TestMakeDefaultOutputPath:
    def test_html_format(self):
        path = _make_default_output_path("html", "mydb")
        assert path.startswith("reports/mydb_")
        assert path.endswith(".html")

    def test_json_format(self):
        path = _make_default_output_path("json", "mydb")
        assert path.startswith("reports/mydb_")
        assert path.endswith(".json")

    def test_markdown_format(self):
        path = _make_default_output_path("markdown", "mydb")
        assert path.startswith("reports/mydb_")
        assert path.endswith(".md")

    def test_empty_dbname_uses_fallback(self):
        path = _make_default_output_path("html", "")
        assert "mm-ready_" in path

    def test_timestamp_format(self):
        path = _make_default_output_path("html", "db")
        # Should contain a timestamp like 20260127_143757
        match = re.search(r"\d{8}_\d{6}", path)
        assert match, f"No timestamp found in {path}"


class TestMakeOutputPath:
    def test_inserts_timestamp(self):
        path = _make_output_path("report.html", "html", "db")
        assert path.startswith("report_")
        assert path.endswith(".html")
        assert re.search(r"\d{8}_\d{6}", path)

    def test_no_extension_uses_format(self):
        path = _make_output_path("report", "json", "db")
        assert path.endswith(".json")

    def test_directory_path(self, tmp_path):
        path = _make_output_path(str(tmp_path), "html", "mydb")
        assert path.startswith(str(tmp_path))
        assert "mydb_" in path
        assert path.endswith(".html")

    def test_preserves_user_extension(self):
        path = _make_output_path("output.txt", "html", "db")
        assert path.endswith(".txt")
