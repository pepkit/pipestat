"""PostgreSQL-backed tests for tests/test_timestamps.py.

The shared test classes imported below are written once in tests/ and marked
`@pytest.mark.backends("file", "db")`; collected here, they run with backend="db".
"""

from ..test_timestamps import TestTimeStamp  # noqa: F401
