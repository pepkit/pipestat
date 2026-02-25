#!/bin/bash
# Test Services Management Script
# Manages PostgreSQL container required for pipestat integration tests.
#
# Usage:
#   ./tests/scripts/services.sh start   # Start PostgreSQL
#   ./tests/scripts/services.sh stop    # Stop PostgreSQL
#   ./tests/scripts/services.sh status  # Show service status

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parallel-safe configuration via environment variables
RUN_ID="${PIPESTAT_TEST_RUN_ID:-$$}"
CONTAINER_NAME="${PIPESTAT_TEST_CONTAINER:-pipestat-db-test-${RUN_ID}}"
DB_PORT="${PIPESTAT_TEST_DB_PORT:-5432}"

DB_USER="pipestatuser"
DB_PASS='shgfty^8922138$^!'
DB_NAME="pipestat-test"

# Export for child processes
export PIPESTAT_TEST_RUN_ID="$RUN_ID"
export PIPESTAT_TEST_CONTAINER="$CONTAINER_NAME"
export PIPESTAT_TEST_DB_PORT="$DB_PORT"

start_postgres() {
    echo "Starting PostgreSQL..."
    echo "  Container: $CONTAINER_NAME"
    echo "  Port: $DB_PORT"

    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

    docker run -d \
        --name "$CONTAINER_NAME" \
        -e POSTGRES_USER="$DB_USER" \
        -e POSTGRES_PASSWORD="$DB_PASS" \
        -e POSTGRES_DB="$DB_NAME" \
        -p "127.0.0.1:${DB_PORT}:5432" \
        --tmpfs /var/lib/postgresql/data \
        postgres:17

    echo "Waiting for PostgreSQL..."
    for i in $(seq 1 30); do
        if docker exec "$CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_NAME" 2>/dev/null; then
            echo "PostgreSQL is ready!"
            return 0
        fi
        sleep 1
    done
    echo "Failed to start PostgreSQL"
    docker logs "$CONTAINER_NAME"
    return 1
}

stop_db() {
    echo "Stopping database ($CONTAINER_NAME)..."
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true
}

case "${1:-}" in
    start)
        echo "=== Starting Test Services (Run ID: $RUN_ID) ==="
        start_postgres
        echo ""
        echo "To use manually:"
        echo "  export PIPESTAT_TEST_DB_PORT=$DB_PORT"
        echo "  export PIPESTAT_TEST_CONTAINER=$CONTAINER_NAME"
        ;;
    stop)
        echo "=== Stopping Test Services ==="
        stop_db
        ;;
    restart)
        echo "=== Restarting Test Services ==="
        stop_db
        start_postgres
        ;;
    status)
        echo "=== Test Services Status (Run ID: $RUN_ID) ==="
        docker ps -f "name=$CONTAINER_NAME" --format "DB: {{.Names}} | {{.Ports}} | {{.Status}}" 2>/dev/null || echo "DB: Not running"
        ;;
    logs)
        docker logs -f "$CONTAINER_NAME"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        echo ""
        echo "Environment variables for parallel execution:"
        echo "  PIPESTAT_TEST_RUN_ID    - Unique identifier (default: PID)"
        echo "  PIPESTAT_TEST_DB_PORT   - Database port (default: 5432)"
        echo "  PIPESTAT_TEST_CONTAINER - Container name (default: pipestat-db-test-\$RUN_ID)"
        exit 1
        ;;
esac
