"""Fixtures for integration tests that need a real PostgreSQL database.

Tests in this directory are skipped unless RUN_INTEGRATION_TESTS=true (see the gate in
tests/conftest.py). When the gate is on, the database must be reachable: if it is not, the
whole run stops with an error instead of quietly skipping tests.

Run them with tests/scripts/test-integration.sh, which starts a throwaway PostgreSQL in
Docker, sets the connection variables, and sets RUN_INTEGRATION_TESTS=true.
"""

import pytest

from ..conftest import DB_HOST, DB_PORT, DB_URL, RUN_INTEGRATION_TESTS


def _postgres_problem():
    """Return a description of why the test database can't be used, or None if it can."""
    try:
        import sqlmodel  # noqa: F401
        from sqlalchemy import create_engine, text
    except ImportError as e:
        return f"DB dependencies are not installed ({e}). Install with: uv pip install -e '.[dbbackend]'"
    engine = create_engine(DB_URL, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        return f"cannot connect to PostgreSQL at {DB_HOST}:{DB_PORT} ({type(e).__name__}: {e})"
    finally:
        engine.dispose()
    return None


@pytest.fixture(scope="session", autouse=True)
def require_postgres():
    """Stop the run loudly if integration tests are enabled but the database is unusable."""
    if not RUN_INTEGRATION_TESTS:
        return
    problem = _postgres_problem()
    if problem:
        pytest.exit(
            "RUN_INTEGRATION_TESTS=true but the test database is not usable: "
            f"{problem}\nStart one with tests/scripts/test-integration.sh "
            "(or tests/scripts/services.sh start, then export PIPESTAT_TEST_DB_PORT).",
            returncode=1,
        )
