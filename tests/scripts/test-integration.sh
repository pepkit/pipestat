#!/bin/bash
# Pipestat Integration Test Runner
# Handles Docker setup, dependency installation, test execution, and cleanup.
#
# Usage:
#   ./tests/scripts/test-integration.sh            # Run all tests (including DB)
#   ./tests/scripts/test-integration.sh -k "test_report"  # Run matching tests
#   ./tests/scripts/test-integration.sh --no-install       # Skip pip install

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
cd "$PROJECT_ROOT"

SERVICES_SCRIPT="$SCRIPT_DIR/services.sh"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

# Pick a random available port (configs are generated dynamically by conftest.py)
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

# Step 1: Install DB backend dependencies
if [ "$SKIP_INSTALL" = false ]; then
    echo -e "\n${GREEN}Installing pipestat with DB backend...${NC}"
    pip install -e ".[dbbackend]" -q
fi

# Step 2: Start PostgreSQL
echo -e "\n${GREEN}Starting test database...${NC}"
"$SERVICES_SCRIPT" start
SERVICES_STARTED=true

# Step 3: Run tests
echo -e "\n${GREEN}Running tests...${NC}"
python -m pytest tests/ -x -vv "${PYTEST_ARGS[@]}"

echo -e "\n${GREEN}All tests passed!${NC}"
