# CLAUDE.md

Pipestat standardizes reporting pipeline results to a YAML file, database, or PEPhub.
Define results in a schema, report them with `report()`, retrieve with `retrieve_one()`.
Backends: YAML file (default), PostgreSQL database, PEPhub.

## Key Terms

- **`pipeline_name`** — Identifies which pipeline produced the results. In files, it's the top-level YAML key. In databases, it's the table name. Set in the schema (required).
- **`record_identifier`** — Identifies a single unit of work (usually a sample). In files, it's a key under the pipeline. In databases, it's a row key scoped by `project_name`.
- **`project_name`** — Identifies a set of samples processed together. In file backends, each file is inherently one project so this is unused. In database backends, it's a required column that namespaces records to prevent collisions between projects sharing a table.
- **`pipeline_type`** — Either `"sample"` (per-sample results, default) or `"project"` (aggregate results). For project-level pipelines, `record_identifier` auto-defaults to `project_name`.

For copy-paste recipes covering common use cases, see [RECIPES.md](RECIPES.md).

## Minimal Example (file backend)

```python
import pipestat

psm = pipestat.PipestatManager.from_file_backend(
    "results.yaml",
    schema_path="output_schema.yaml",
)
psm.report(record_identifier="sample1", values={"number_of_things": 42})
result = psm.retrieve_one(record_identifier="sample1")
```

## Minimal Schema

`pipeline_name` is required -- the most common error is omitting it.

```yaml
pipeline_name: my_pipeline
samples:
  number_of_things:
    type: integer
    description: "Number of things"
```

JSON Schema format also works (with `properties.pipeline_name`). The flat format above is simpler.

## Manager Classes

- `PipestatManager(pipeline_type="sample")` -- most common, report per-sample results (default)
- `PipestatManager(pipeline_type="project")` -- project-level results, `record_identifier` auto-defaults to `project_name`
- `SamplePipestatManager` -- convenience wrapper, equivalent to `PipestatManager(pipeline_type="sample")`
- `ProjectPipestatManager` -- convenience wrapper, equivalent to `PipestatManager(pipeline_type="project")`
- `PipestatDualManager` -- holds both `.sample` and `.project` sub-managers

Classmethods for construction:
- `PipestatManager.from_file_backend(results_file_path, schema_path=, ...)` -- YAML file backend
- `PipestatManager.from_db_backend(config)` -- database backend (config has `database` section)
- `PipestatManager.from_pephub_backend(pephub_path)` -- PEPhub backend
- `PipestatManager.from_config(config)` -- generic, config determines backend

## Core Methods

- `report(values={"key": val}, record_identifier="id")` -- Store results. Returns `list[str]` of formatted strings, or `False` if exists and `force_overwrite` is disabled.
- `retrieve_one(record_identifier="id", result_identifier="key")` -- Get one record (dict), one result value (scalar), or specific keys (dict). Raises `RecordNotFoundError`.
- `select_records(filter_conditions=[...], columns=[...], limit=1000)` -- Query records. Returns `{"total_size": N, "page_size": N, "next_page_token": N, "records": [...]}`. Filter operators: eq, lt, ge, in, like. Combine with `bool_operator="OR"`.
- `set_status(status_identifier="running", record_identifier="id")` -- Set status (running/completed/failed/waiting/partial).
- `get_status(record_identifier="id")` -- Get current status string or None.
- `remove(record_identifier="id", result_identifier="key")` -- Remove a result or entire record.
- `summarize(output_dir="reports/", mode="table")` -- Generate HTML report. Modes: "table", "gallery".
- `table(output_dir="reports/")` -- Generate TSV stats and YAML objects files.

## Common Gotchas

- Schema must include `pipeline_name` as a string value (not a schema definition) -- `SchemaError` if missing
- `record_identifier` cannot be empty string -- raises `ValueError`
- For project-level pipelines, `record_identifier` auto-defaults to `project_name` (which defaults to `"project"`)
- `force_overwrite` defaults to `True` at manager level; it is a settable property
- `result_formatter` is also a settable property (not an `__init__` param)
- File/image results require `{"path": "...", "title": "..."}` dict format (`thumbnail_path` is optional for images)

## Schema-Free Mode

- `validate_results=False` skips schema validation; string paths with image extensions auto-wrap to `{"path": ..., "title": ...}`
- Can be combined with `schema_path=None` for fully schemaless operation
- `additional_properties=True` (default) allows reporting results not defined in schema even when validation is on
- Generate a schema from existing results: `pipestat infer-schema -f results.yaml -o schema.yaml`

## Development Commands

```
pip install -e .
pip install -r requirements/requirements-test.txt
pytest tests -x -vv
# DB tests need postgres: docker run -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres
```
