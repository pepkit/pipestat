"""PostgreSQL-backed tests for tests/test_status.py.

The shared test classes imported below are written once in tests/ and marked
`@pytest.mark.backends("file", "db")`; collected here, they run with backend="db".
"""

from ..test_status import TestStatus  # noqa: F401
