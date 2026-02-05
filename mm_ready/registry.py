"""Auto-discovery and registration of check modules."""

from __future__ import annotations

import contextlib
import importlib
import pkgutil
from pathlib import Path

from mm_ready.checks.base import BaseCheck


def discover_checks(
    categories: list[str] | None = None,
    mode: str | None = None,
) -> list[BaseCheck]:
    """Walk the checks/ package tree and instantiate all BaseCheck subclasses.

    Args:
        categories: If provided, only return checks whose category is in this list.
        mode: If provided, only return checks matching this mode ("scan" or "audit").
              Checks with mode="both" match either mode.

    Returns:
        List of instantiated check objects, sorted by category then name.
    """
    checks_package = importlib.import_module("mm_ready.checks")
    assert checks_package.__file__ is not None
    checks_dir = Path(checks_package.__file__).parent

    _import_submodules("mm_ready.checks", checks_dir)

    instances = []
    seen = set()
    for cls in _all_subclasses(BaseCheck):
        if cls in seen or not cls.name:
            continue
        seen.add(cls)
        if categories and cls.category not in categories:
            continue
        if mode and cls.mode != mode and cls.mode != "both":
            continue
        instances.append(cls())

    instances.sort(key=lambda c: (c.category, c.name))
    return instances


def _import_submodules(package_name: str, package_dir: Path):
    """Recursively import all submodules under a package directory."""
    for _importer, modname, _ispkg in pkgutil.walk_packages(
        path=[str(package_dir)],
        prefix=package_name + ".",
    ):
        with contextlib.suppress(Exception):
            importlib.import_module(modname)


def _all_subclasses(cls):
    """Recursively get all subclasses of a class."""
    result = []
    for sub in cls.__subclasses__():
        result.append(sub)
        result.extend(_all_subclasses(sub))
    return result
