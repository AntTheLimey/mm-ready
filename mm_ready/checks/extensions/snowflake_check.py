"""Check if the pgEdge snowflake extension is available."""

from mm_ready.checks.base import BaseCheck
from mm_ready.models import Finding, Severity


class SnowflakeExtensionCheck(BaseCheck):
    name = "snowflake_check"
    category = "extensions"
    description = "Check availability of pgEdge snowflake extension for unique ID generation"

    def run(self, conn) -> list[Finding]:
        # Check if installed
        """
        Check pgEdge Snowflake extension installation, availability, and the configured snowflake.node on the connected PostgreSQL server.
        
        This runs queries against the provided connection to determine whether the `snowflake` extension is installed, available, or missing. If installed, it also reads `snowflake.node` to verify a non-zero node identifier and produces findings based on those outcomes.
        
        Parameters:
        	conn: A DB connection object with a cursor() method (PEP-249-style) connected to the target PostgreSQL server.
        
        Returns:
        	list[Finding]: A list of Finding objects describing the result:
        		- INFO: extension installed and a non-zero `snowflake.node` value found (includes node and version metadata).
        		- WARNING: extension installed but `snowflake.node` is missing or set to "0".
        		- CONSIDER: extension available but not installed (includes available version metadata).
        		- CONSIDER: extension not available on the server.
        """
        with conn.cursor() as cur:
            cur.execute("""
                SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'snowflake';
            """)
            installed = cur.fetchone()

            # Check if available but not installed
            cur.execute("""
                SELECT name, default_version
                FROM pg_catalog.pg_available_extensions
                WHERE name = 'snowflake';
            """)
            available = cur.fetchone()

        findings = []
        if installed:
            # Check if snowflake.node is configured
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_setting('snowflake.node');")
                    node_val = cur.fetchone()[0]
            except Exception:
                node_val = None

            if not node_val or node_val == "0":
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        check_name=self.name,
                        category=self.category,
                        title=f"Snowflake installed (v{installed[0]}) but snowflake.node is not set",
                        detail=(
                            "The pgEdge snowflake extension is installed but snowflake.node "
                            "is not configured. Each node in the cluster must have a unique "
                            "snowflake.node value. For clusters using standard pgEdge node "
                            "naming (n1, n2, ... n9), this is set automatically. For clusters "
                            "with more than 9 nodes or non-standard naming, it must be set "
                            "manually."
                        ),
                        object_name="snowflake.node",
                        remediation=(
                            "Set a unique node identifier:\n"
                            "  ALTER SYSTEM SET snowflake.node = <unique_id>;\n"
                            "  -- Restart PostgreSQL"
                        ),
                        metadata={"version": installed[0]},
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity=Severity.INFO,
                        check_name=self.name,
                        category=self.category,
                        title=f"Snowflake extension is installed (v{installed[0]}, node={node_val})",
                        detail=(
                            "The pgEdge snowflake extension is installed and snowflake.node "
                            f"is set to {node_val}. Ensure this value is unique across all "
                            "cluster nodes."
                        ),
                        object_name="snowflake",
                        remediation="",
                        metadata={"version": installed[0], "node": node_val},
                    )
                )
        elif available:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title=f"Snowflake extension is available but not installed (v{available[1]})",
                    detail=(
                        "The pgEdge snowflake extension is available on this server but not yet "
                        "installed. It will be needed for multi-master safe sequence generation."
                    ),
                    object_name="snowflake",
                    remediation="Run: CREATE EXTENSION snowflake;",
                    metadata={"available_version": available[1]},
                )
            )
        else:
            findings.append(
                Finding(
                    severity=Severity.CONSIDER,
                    check_name=self.name,
                    category=self.category,
                    title="Snowflake extension is NOT available",
                    detail=(
                        "The pgEdge snowflake extension is not available on this server. "
                        "It is required for generating globally unique IDs in multi-master setups."
                    ),
                    object_name="snowflake",
                    remediation=(
                        "Install the pgEdge snowflake extension package on this PostgreSQL server."
                    ),
                )
            )
        return findings