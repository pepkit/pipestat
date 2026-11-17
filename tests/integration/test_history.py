"""PostgreSQL-backed tests for tests/test_history.py.

The shared test classes imported below are written once in tests/ and marked
`@pytest.mark.backends("file", "db")`; collected here, they run with backend="db".
"""

from ..test_history import TestRetrieveHistory  # noqa: F401
