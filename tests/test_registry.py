"""Tests for mm_ready.registry — check discovery and filtering."""

from __future__ import annotations

from mm_ready.registry import discover_checks


class TestDiscoverChecks:
    def test_returns_checks(self):
        checks = discover_checks()
        assert len(checks) > 0

    def test_all_checks_have_required_attrs(self):
        for check in discover_checks():
            assert check.name, f"Check {check!r} has empty name"
            assert check.category, f"Check {check.name} has empty category"
            assert check.description, f"Check {check.name} has empty description"
            assert check.mode in ("scan", "audit", "both"), (
                f"Check {check.name} has invalid mode: {check.mode}"
            )

    def test_no_duplicate_names(self):
        """
        Verifies that all discovered checks have unique names.

        Asserts that the list of check names returned by `discover_checks()` contains no duplicates; if duplicates exist the assertion message lists the duplicated names.
        """
        checks = discover_checks()
        names = [c.name for c in checks]
        assert len(names) == len(set(names)), (
            f"Duplicate names: {[n for n in names if names.count(n) > 1]}"
        )

    def test_sorted_by_category_then_name(self):
        checks = discover_checks()
        keys = [(c.category, c.name) for c in checks]
        assert keys == sorted(keys)

    def test_total_check_count(self):
        checks = discover_checks()
        assert len(checks) == 56


class TestModeFiltering:
    def test_scan_mode_includes_scan_and_both(self):
        checks = discover_checks(mode="scan")
        for check in checks:
            assert check.mode in ("scan", "both"), (
                f"Check {check.name} has mode={check.mode} but should be scan or both"
            )

    def test_audit_mode_includes_audit_and_both(self):
        checks = discover_checks(mode="audit")
        for check in checks:
            assert check.mode in ("audit", "both"), (
                f"Check {check.name} has mode={check.mode} but should be audit or both"
            )

    def test_scan_mode_count(self):
        checks = discover_checks(mode="scan")
        assert len(checks) == 48

    def test_audit_excludes_scan_only(self):
        scan_checks = discover_checks(mode="scan")
        audit_checks = discover_checks(mode="audit")
        scan_only_names = {c.name for c in scan_checks if c.mode == "scan"}
        audit_names = {c.name for c in audit_checks}
        assert scan_only_names.isdisjoint(audit_names)


class TestCategoryFiltering:
    def test_single_category(self):
        checks = discover_checks(categories=["schema"])
        assert all(c.category == "schema" for c in checks)
        assert len(checks) > 0

    def test_multiple_categories(self):
        checks = discover_checks(categories=["schema", "config"])
        assert all(c.category in ("schema", "config") for c in checks)

    def test_nonexistent_category(self):
        checks = discover_checks(categories=["nonexistent"])
        assert checks == []

    def test_category_and_mode_combined(self):
        checks = discover_checks(categories=["replication"], mode="audit")
        for check in checks:
            assert check.category == "replication"
            assert check.mode in ("audit", "both")


class TestExcludeFiltering:
    def test_exclude_single_check(self):
        all_checks = discover_checks()
        excluded_name = all_checks[0].name
        filtered = discover_checks(exclude={excluded_name})
        assert len(filtered) == len(all_checks) - 1
        assert excluded_name not in [c.name for c in filtered]

    def test_exclude_multiple_checks(self):
        all_checks = discover_checks()
        exclude_set = {all_checks[0].name, all_checks[1].name}
        filtered = discover_checks(exclude=exclude_set)
        assert len(filtered) == len(all_checks) - 2
        for name in exclude_set:
            assert name not in [c.name for c in filtered]

    def test_exclude_nonexistent_check(self):
        all_checks = discover_checks()
        filtered = discover_checks(exclude={"nonexistent_check"})
        assert len(filtered) == len(all_checks)

    def test_exclude_with_mode(self):
        scan_checks = discover_checks(mode="scan")
        excluded_name = scan_checks[0].name
        filtered = discover_checks(mode="scan", exclude={excluded_name})
        assert len(filtered) == len(scan_checks) - 1


class TestIncludeOnlyFiltering:
    def test_include_only_single_check(self):
        all_checks = discover_checks()
        target_name = all_checks[0].name
        filtered = discover_checks(include_only={target_name})
        assert len(filtered) == 1
        assert filtered[0].name == target_name

    def test_include_only_multiple_checks(self):
        all_checks = discover_checks()
        target_names = {all_checks[0].name, all_checks[1].name}
        filtered = discover_checks(include_only=target_names)
        assert len(filtered) == 2
        assert {c.name for c in filtered} == target_names

    def test_include_only_nonexistent_check(self):
        filtered = discover_checks(include_only={"nonexistent_check"})
        assert filtered == []

    def test_include_only_overrides_category(self):
        """include_only should work regardless of category."""
        all_checks = discover_checks()
        # Pick a check from a specific category
        schema_check = next(c for c in all_checks if c.category == "schema")
        # Try to filter by a different category but include_only this check
        filtered = discover_checks(categories=["config"], include_only={schema_check.name})
        # include_only takes precedence - we should get the check
        assert len(filtered) == 1
        assert filtered[0].name == schema_check.name

    def test_include_only_with_exclude(self):
        """Exclude should still apply even in include_only mode."""
        all_checks = discover_checks()
        target_names = {all_checks[0].name, all_checks[1].name}
        exclude_name = all_checks[0].name
        filtered = discover_checks(include_only=target_names, exclude={exclude_name})
        assert len(filtered) == 1
        assert filtered[0].name == all_checks[1].name
