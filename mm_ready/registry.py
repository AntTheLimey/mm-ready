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
    """
    Discover and instantiate all BaseCheck subclasses under the mm_ready.checks package, optionally filtering by category and mode.
    
    Parameters:
        categories (list[str] | None): If provided, only include checks whose `category` is in this list.
        mode (str | None): If provided, only include checks whose `mode` equals this value (e.g. "scan" or "audit"); checks with `mode == "both"` match any mode.
    
    Returns:
        list[BaseCheck]: Instantiated check objects, sorted by (category, name).
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
    """
    Recursively import all submodules in a package directory, ignoring import errors.
    
    Imports every module found under the given package directory using the package name as the import prefix. Any exception raised while importing an individual submodule is suppressed so discovery continues.
    
    Parameters:
        package_name (str): Dotted import path of the package (e.g., "mm_ready.checks") used as the import prefix.
        package_dir (Path): Filesystem path to the package directory to search for submodules.
    """
    for _importer, modname, _ispkg in pkgutil.walk_packages(
        path=[str(package_dir)],
        prefix=package_name + ".",
    ):
        with contextlib.suppress(Exception):
            importlib.import_module(modname)


def _all_subclasses(cls):
    """
    Collect all subclasses of a class recursively.
    
    Performs a depth-first traversal of the subclass hierarchy and returns every direct and indirect subclass of `cls` (does not include `cls` itself). The traversal order is depth-first: each discovered subclass is listed before its own subclasses.
    
    Parameters:
        cls (type): The base class whose subclasses will be discovered.
    
    Returns:
        list[type]: A list of subclass types found for `cls`, in depth-first order.
    """
    result = []
    for sub in cls.__subclasses__():
        result.append(sub)
        result.extend(_all_subclasses(sub))
    return result