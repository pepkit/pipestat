![Run pytests](https://github.com/pepkit/pipestat/workflows/Run%20pytests/badge.svg)
[![pypi-badge](https://img.shields.io/pypi/v/pipestat)](https://pypi.org/project/pipestat)

<img src="https://raw.githubusercontent.com/pepkit/pipestat/master/docs/img/pipestat_logo.svg?sanitize=true" alt="pipestat" height="70"/><br>

Pipestat is a Python package for managing pipeline results. It provides a standard API for reporting, storing, and retrieving outputs from any computational pipeline. Results are validated against a JSON Schema and stored in either a YAML file or a PostgreSQL database. A pipeline author defines outputs in a schema, then uses pipestat to report results as the pipeline runs. Downstream tools retrieve those results through the same API.

See [Pipestat documentation](https://pep.databio.org/pipestat/) for complete details.

## Quick Start

```python
import pipestat

psm = pipestat.PipestatManager(
    schema_path="output_schema.yaml",
    results_file_path="results.yaml",
    record_identifier="sample1",
)
psm.report(values={"accuracy": 0.95, "processing_time": 12.3})
psm.report(values={"output_file": {"path": "results/output.csv", "title": "Output CSV"}})
```

## Schema

Pipestat requires a schema that defines the results your pipeline reports. Here is a minimal example:

```yaml
pipeline_name: my_pipeline
samples:
  accuracy:
    type: number
    description: "Model accuracy score"
  processing_time:
    type: number
    description: "Processing time in seconds"
  output_file:
    type: object
    object_type: file
    properties:
      path: {type: string}
      title: {type: string}
    required: ["path", "title"]
```

Every result needs `type` and `description`. See `sample_output_schema_generic.yaml` in this repo for a fuller example with both sample and project result types.

### Generating a schema from existing results

If you already have a pipestat results file, you can auto-generate a schema:

```bash
pipestat infer-schema -f results.yaml -o schema.yaml
```

This inspects your results and produces a schema with the correct types. See `pipestat infer-schema --help` for options.

## Developer tests

Set up an environment with [uv](https://docs.astral.sh/uv/):

```bash
uv venv
uv pip install -e ".[test,dbbackend,pephub]"
```

Unit tests need no services. Run them with plain pytest:

```bash
.venv/bin/python -m pytest tests
```

Database tests live in `tests/integration/` and need PostgreSQL. They are skipped unless
`RUN_INTEGRATION_TESTS=true`. The easiest way to run everything (unit and integration) is
the runner script, which starts a throwaway PostgreSQL in Docker and removes it afterward:

```bash
tests/scripts/test-integration.sh              # installs with uv, then runs all tests
tests/scripts/test-integration.sh --no-install # skip the install step
tests/scripts/test-integration.sh -k select    # extra args go to pytest
```

If `RUN_INTEGRATION_TESTS=true` is set but the database can't be reached, the run stops with
an error instead of skipping tests. CI runs the same script.

PEPhub backend tests use an in-memory fake PEPhub client, so they need no network or login.
`pytest tests/test_pephub.py --pephub-live` runs them against the live pephub.databio.org
service instead; that needs `phc login` and writes to a real PEP.
