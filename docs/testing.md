# Testing

## Install test dependencies

```bash
uv venv
uv pip install -e ".[test,dbbackend,pephub]"
```

## Unit tests

```bash
.venv/bin/python -m pytest tests
```

These need no services. Tests in `tests/integration/` are skipped. PEPhub tests use an in-memory fake client, so they run here too.

## Integration tests

Integration tests need PostgreSQL. One script starts a throwaway database in Docker, runs every test, and removes the database afterward:

```bash
tests/scripts/test-integration.sh
tests/scripts/test-integration.sh --no-install -k select   # skip install; extra args go to pytest
```

The script sets `RUN_INTEGRATION_TESTS=true`. When that is set and the database cannot be reached, the run stops with an error instead of skipping tests. Each run uses its own port and container name, so several runs can happen at once. CI runs this same script.

To manage the database yourself, use `tests/scripts/services.sh start|stop|status`.

## Live PEPhub tests

```bash
pytest tests/test_pephub.py --pephub-live
```

This talks to pephub.databio.org, needs `phc login`, and writes to a real PEP. It never runs in CI.
