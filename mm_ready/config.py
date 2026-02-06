"""Configuration loading and management for mm-ready."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class CheckConfig:
    """Configuration for which checks to include/exclude."""

    exclude: set[str] = field(default_factory=set)
    include_only: set[str] | None = None  # None = no whitelist, run all minus exclude


@dataclass
class ReportConfig:
    """Configuration for report generation."""

    todo_list: bool = True
    todo_include_consider: bool = False


@dataclass
class Config:
    """Complete configuration for mm-ready."""

    global_checks: CheckConfig = field(default_factory=CheckConfig)
    mode_checks: dict[str, CheckConfig] = field(default_factory=dict)
    report: ReportConfig = field(default_factory=ReportConfig)

    def get_check_config(self, mode: str) -> CheckConfig:
        """Get merged check config for a specific mode.

        Mode-specific settings are merged with global settings:
        - exclude: union of global and mode-specific excludes
        - include_only: mode-specific overrides global if set
        """
        global_cfg = self.global_checks
        mode_cfg = self.mode_checks.get(mode, CheckConfig())

        # Merge excludes
        merged_exclude = global_cfg.exclude | mode_cfg.exclude

        # Mode-specific include_only takes precedence
        merged_include_only = (
            mode_cfg.include_only if mode_cfg.include_only is not None else global_cfg.include_only
        )

        return CheckConfig(exclude=merged_exclude, include_only=merged_include_only)


def find_config_file() -> str | None:
    """Search for mm-ready.yaml in cwd, then home dir.

    Returns:
        Path to config file if found, None otherwise.
    """
    # Check current directory
    cwd_config = Path.cwd() / "mm-ready.yaml"
    if cwd_config.is_file():
        return str(cwd_config)

    # Check home directory
    home_config = Path.home() / "mm-ready.yaml"
    if home_config.is_file():
        return str(home_config)

    return None


def load_config(config_path: str | None = None, auto_discover: bool = True) -> Config:
    """Load configuration from YAML file.

    Args:
        config_path: Explicit path to config file. If None and auto_discover is True,
                     searches default locations.
        auto_discover: If True and config_path is None, search for config file.

    Returns:
        Config object. Returns default config if no file found.
    """
    if config_path is None and auto_discover:
        config_path = find_config_file()

    if config_path is None:
        return Config()

    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        data = yaml.safe_load(f) or {}

    return _parse_config(data)


def _parse_config(data: dict) -> Config:
    """Parse YAML data into Config object."""
    config = Config()

    # Parse global checks config
    if "checks" in data:
        config.global_checks = _parse_check_config(data["checks"])

    # Parse mode-specific configs
    for mode in ("scan", "audit", "analyze", "monitor"):
        if mode in data and "checks" in data[mode]:
            config.mode_checks[mode] = _parse_check_config(data[mode]["checks"])

    # Parse report config
    if "report" in data:
        report_data = data["report"]
        config.report = ReportConfig(
            todo_list=report_data.get("todo_list", True),
            todo_include_consider=report_data.get("todo_include_consider", False),
        )

    return config


def _parse_check_config(data: dict) -> CheckConfig:
    """Parse check configuration section."""
    exclude = set(data.get("exclude", []))

    include_only = None
    if "include_only" in data:
        include_only = set(data["include_only"])

    return CheckConfig(exclude=exclude, include_only=include_only)


def merge_cli_with_config(
    config: Config,
    mode: str,
    cli_exclude: set[str] | None = None,
    cli_include_only: set[str] | None = None,
    cli_no_todo: bool = False,
    cli_todo_include_consider: bool = False,
) -> tuple[CheckConfig, ReportConfig]:
    """Merge CLI arguments with config file settings.

    CLI arguments take precedence over config file.

    Args:
        config: Loaded configuration.
        mode: Current execution mode (scan, audit, analyze, monitor).
        cli_exclude: Checks to exclude (from --exclude flag).
        cli_include_only: Checks to include only (from --include-only flag).
        cli_no_todo: Whether to disable To Do list (from --no-todo flag).
        cli_todo_include_consider: Include CONSIDER in To Do (from --todo-include-consider).

    Returns:
        Tuple of (CheckConfig, ReportConfig) with merged settings.
    """
    # Get mode-merged check config
    check_cfg = config.get_check_config(mode)

    # CLI exclude adds to config exclude
    if cli_exclude:
        check_cfg = CheckConfig(
            exclude=check_cfg.exclude | cli_exclude,
            include_only=check_cfg.include_only,
        )

    # CLI include_only completely overrides config
    if cli_include_only is not None:
        check_cfg = CheckConfig(
            exclude=check_cfg.exclude,
            include_only=cli_include_only,
        )

    # Report config - CLI overrides
    report_cfg = ReportConfig(
        todo_list=not cli_no_todo and config.report.todo_list,
        todo_include_consider=cli_todo_include_consider or config.report.todo_include_consider,
    )

    return check_cfg, report_cfg
