"""Check shared_preload_libraries includes spock (audit mode only)."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class SharedPreloadCheck(BaseCheck):
    name = "shared_preload_libraries"
    category = "config"
    description = "shared_preload_libraries must include 'spock' for Spock operation"
    mode = "audit"

    def run(self, conn) -> list[Finding]:
        """
        Check that the PostgreSQL configuration parameter `shared_preload_libraries` includes the `spock` library and produce findings if it does not.

        Queries the server for `shared_preload_libraries`, parses the comma-separated value into a list, and returns a finding when `spock` is absent. The finding contains severity, explanatory detail, remediation instructions, and the current libraries in metadata.

        Returns:
            list[Finding]: A list of findings; contains a single CRITICAL Finding when `spock` is not present, otherwise an empty list.
        """
        with conn.cursor() as cur:
            cur.execute("SHOW shared_preload_libraries;")
            libs = cur.fetchone()[0]

        lib_list = [lib.strip() for lib in libs.split(",") if lib.strip()] if libs else []

        findings = []
        if "spock" not in lib_list:
            findings.append(
                Finding(
                    severity=Severity.CRITICAL,
                    check_name=self.name,
                    category=self.category,
                    title="'spock' not in shared_preload_libraries",
                    detail=(
                        f"shared_preload_libraries = '{libs}'. The 'spock' library must be "
                        "included for Spock to function. This requires a server restart."
                    ),
                    object_name="shared_preload_libraries",
                    remediation=(
                        "Add 'spock' to shared_preload_libraries in postgresql.conf and restart. "
                        "Example: shared_preload_libraries = 'spock'"
                    ),
                    metadata={"current_libs": lib_list},
                )
            )
        return findings
