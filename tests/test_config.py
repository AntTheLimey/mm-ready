"""Tests for configuration loading and merging."""

from __future__ import annotations

import tempfile

import pytest

from mm_ready.config import (
    CheckConfig,
    Config,
    ReportConfig,
    load_config,
    merge_cli_with_config,
)


class TestCheckConfig:
    """Tests for CheckConfig dataclass."""

    def test_defaults(self):
        cfg = CheckConfig()
        assert cfg.exclude == set()
        assert cfg.include_only is None

    def test_exclude_set(self):
        cfg = CheckConfig(exclude={"check_a", "check_b"})
        assert "check_a" in cfg.exclude
        assert "check_c" not in cfg.exclude


class TestReportConfig:
    """Tests for ReportConfig dataclass."""

    def test_defaults(self):
        cfg = ReportConfig()
        assert cfg.todo_list is True
        assert cfg.todo_include_consider is False

    def test_custom_values(self):
        cfg = ReportConfig(todo_list=False, todo_include_consider=True)
        assert cfg.todo_list is False
        assert cfg.todo_include_consider is True


class TestConfig:
    """Tests for Config dataclass."""

    def test_defaults(self):
        cfg = Config()
        assert cfg.global_checks.exclude == set()
        assert cfg.global_checks.include_only is None
        assert cfg.mode_checks == {}
        assert cfg.report.todo_list is True

    def test_get_check_config_global_only(self):
        cfg = Config(global_checks=CheckConfig(exclude={"check_a"}))
        result = cfg.get_check_config("scan")
        assert "check_a" in result.exclude

    def test_get_check_config_merges_exclude(self):
        cfg = Config(
            global_checks=CheckConfig(exclude={"global_check"}),
            mode_checks={"scan": CheckConfig(exclude={"scan_check"})},
        )
        result = cfg.get_check_config("scan")
        assert "global_check" in result.exclude
        assert "scan_check" in result.exclude

    def test_get_check_config_mode_include_only_overrides(self):
        cfg = Config(
            global_checks=CheckConfig(include_only={"global_a"}),
            mode_checks={"audit": CheckConfig(include_only={"audit_a"})},
        )
        result = cfg.get_check_config("audit")
        assert result.include_only == {"audit_a"}


class TestLoadConfig:
    """Tests for load_config function."""

    def test_returns_default_when_no_file(self):
        cfg = load_config(None, auto_discover=False)
        assert isinstance(cfg, Config)
        assert cfg.global_checks.exclude == set()

    def test_loads_yaml_file(self):
        yaml_content = """
checks:
  exclude:
    - check_a
    - check_b
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)
        assert "check_a" in cfg.global_checks.exclude
        assert "check_b" in cfg.global_checks.exclude

    def test_loads_include_only(self):
        yaml_content = """
checks:
  include_only:
    - primary_keys
    - foreign_keys
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)
        assert cfg.global_checks.include_only == {"primary_keys", "foreign_keys"}

    def test_loads_mode_specific_config(self):
        yaml_content = """
scan:
  checks:
    exclude:
      - scan_specific_check
audit:
  checks:
    exclude:
      - audit_specific_check
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)
        assert "scan_specific_check" in cfg.mode_checks["scan"].exclude
        assert "audit_specific_check" in cfg.mode_checks["audit"].exclude

    def test_loads_report_config(self):
        yaml_content = """
report:
  todo_list: false
  todo_include_consider: true
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(yaml_content)
            f.flush()
            cfg = load_config(f.name)
        assert cfg.report.todo_list is False
        assert cfg.report.todo_include_consider is True

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path.yaml")

    def test_empty_yaml_returns_defaults(self):
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write("")
            f.flush()
            cfg = load_config(f.name)
        assert cfg.global_checks.exclude == set()


class TestMergeCliWithConfig:
    """Tests for merge_cli_with_config function."""

    def test_cli_exclude_adds_to_config(self):
        config = Config(global_checks=CheckConfig(exclude={"config_check"}))
        check_cfg, _ = merge_cli_with_config(config, "scan", cli_exclude={"cli_check"})
        assert "config_check" in check_cfg.exclude
        assert "cli_check" in check_cfg.exclude

    def test_cli_include_only_overrides_config(self):
        config = Config(global_checks=CheckConfig(include_only={"config_check"}))
        check_cfg, _ = merge_cli_with_config(config, "scan", cli_include_only={"cli_only_check"})
        assert check_cfg.include_only == {"cli_only_check"}

    def test_cli_no_todo_overrides_config(self):
        config = Config(report=ReportConfig(todo_list=True))
        _, report_cfg = merge_cli_with_config(config, "scan", cli_no_todo=True)
        assert report_cfg.todo_list is False

    def test_cli_todo_include_consider_overrides_config(self):
        config = Config(report=ReportConfig(todo_include_consider=False))
        _, report_cfg = merge_cli_with_config(config, "scan", cli_todo_include_consider=True)
        assert report_cfg.todo_include_consider is True

    def test_mode_specific_exclude_merged(self):
        config = Config(
            global_checks=CheckConfig(exclude={"global"}),
            mode_checks={"scan": CheckConfig(exclude={"scan_mode"})},
        )
        check_cfg, _ = merge_cli_with_config(config, "scan", cli_exclude={"cli"})
        assert "global" in check_cfg.exclude
        assert "scan_mode" in check_cfg.exclude
        assert "cli" in check_cfg.exclude
