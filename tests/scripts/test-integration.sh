#!/bin/bash
# Pipestat Integration Test Runner
# Starts a throwaway PostgreSQL in Docker, runs the WHOLE test suite against it (unit tests in
# tests/ plus the database tests in tests/integration/), then removes the database.
# Supports parallel runs: each run gets its own port and container.
#
# Usage:
#   ./tests/scripts/test-integration.sh                    # Install deps with uv, run all tests
#   ./tests/scripts/test-integration.sh -k "test_report"   # Extra args go to pytest
#   ./tests/scripts/test-integration.sh --no-install       # Skip the uv install step
#
# Python environment: uses the active virtualenv ($VIRTUAL_ENV) if there is one, otherwise
# ./.venv (created with `uv venv` if missing). Install step:
#   uv pip install -e ".[test,dbbackend,pephub]"

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

SERVICES_SCRIPT="$SCRIPT_DIR/services.sh"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse our flags (pass the rest to pytest)
SKIP_INSTALL=false
PYTEST_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--no-install" ]; then
        SKIP_INSTALL=true
    else
        PYTEST_ARGS+=("$arg")
    fi
done

# Pick the Python environment
if [ -z "${VIRTUAL_ENV:-}" ]; then
    if [ ! -d .venv ] && [ "$SKIP_INSTALL" = false ]; then
        uv venv -q .venv
    fi
    if [ -d .venv ]; then
        export VIRTUAL_ENV="$PROJECT_ROOT/.venv"
    fi
fi
if [ -n "${VIRTUAL_ENV:-}" ]; then
    PYTHON="$VIRTUAL_ENV/bin/python"
else
    PYTHON="python3"
fi

# Pick a random free port (configs are generated dynamically by tests/conftest.py)
export PIPESTAT_TEST_RUN_ID="$$"
export PIPESTAT_TEST_DB_PORT="${PIPESTAT_TEST_DB_PORT:-$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')}"
export PIPESTAT_TEST_CONTAINER="pipestat-db-test-${PIPESTAT_TEST_RUN_ID}"

SERVICES_STARTED=false

cleanup() {
    local exit_code=$?
    if [ "$SERVICES_STARTED" = true ]; then
        echo -e "\n${YELLOW}Cleaning up...${NC}"
        "$SERVICES_SCRIPT" stop
    fi
    exit $exit_code
}
trap cleanup EXIT INT TERM

echo -e "${GREEN}=== Pipestat Integration Tests (Run ID: $PIPESTAT_TEST_RUN_ID) ===${NC}"

# Step 1: Install pipestat with test, DB, and PEPhub extras
if [ "$SKIP_INSTALL" = false ]; then
    echo -e "\n${GREEN}Installing pipestat into ${VIRTUAL_ENV:-current environment} with uv...${NC}"
    uv pip install -q -e ".[test,dbbackend,pephub]"
fi

# Step 2: Start PostgreSQL
echo -e "\n${GREEN}Starting test database...${NC}"
"$SERVICES_SCRIPT" start
SERVICES_STARTED=true

# Step 3: Run all tests. With this set, tests/integration/ runs, and fails loudly if the
# database can't be reached.
export RUN_INTEGRATION_TESTS=true
echo -e "\n${GREEN}Running tests...${NC}"
"$PYTHON" -m pytest tests/ -x -vv "${PYTEST_ARGS[@]}"

echo -e "\n${GREEN}All tests passed!${NC}"
