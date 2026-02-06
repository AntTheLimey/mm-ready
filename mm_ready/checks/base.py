"""Base class for all compatibility checks."""

from __future__ import annotations

import abc

from mm_ready.models import Finding


class BaseCheck(abc.ABC):
    """Abstract base class for all Spock readiness checks.

    To create a new check, subclass this and implement `run()`.
    The registry auto-discovers all subclasses found in the checks/ directory.

    Attributes:
        name: Unique identifier for this check.
        category: Grouping category (schema, replication, config, etc.).
        description: Human-readable summary of what this check does.
        mode: When this check applies:
            "scan"  — pre-Spock readiness assessment (default)
            "audit" — post-Spock installation audit
            "both"  — runs in either mode
    """

    name: str = ""
    category: str = ""
    description: str = ""
    mode: str = "scan"

    @abc.abstractmethod
    def run(self, conn) -> list[Finding]:
        """Execute the check against the database connection.

        Args:
            conn: psycopg2 connection object.

        Returns:
            List of Finding objects. Empty list means the check passed.
        """
        ...

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not cls.name:
            cls.name = cls.__name__

    def __repr__(self):
        return f"<{self.__class__.__name__} [{self.category}] {self.name} ({self.mode})>"
