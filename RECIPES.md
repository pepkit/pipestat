# Pipestat Cookbook

Copy-paste recipes for common pipestat patterns. Each recipe is complete and runnable. For API details, see CLAUDE.md.

## Table of Contents

1. [Report a simple numeric result (file backend)](#recipe-1-report-a-simple-numeric-result-file-backend)
2. [Report an image or file result](#recipe-2-report-an-image-or-file-result)
3. [Retrieve results for one record](#recipe-3-retrieve-results-for-one-record)
4. [Query results with filter conditions](#recipe-4-query-results-with-filter-conditions)
5. [Set and check pipeline status](#recipe-5-set-and-check-pipeline-status)
6. [Use schema-free mode](#recipe-6-use-schema-free-mode)
7. [Report project-level results](#recipe-7-report-project-level-results)
8. [Use PipestatDualManager for dual sample+project reporting](#recipe-8-use-pipestatdualmanager-for-dual-sampleproject-reporting)
9. [Generate an HTML summary report](#recipe-9-generate-an-html-summary-report)
10. [Infer a schema from existing results](#recipe-10-infer-a-schema-from-existing-results)

---

## Recipe 1: Report a simple numeric result (file backend)

Create a schema, create a manager, report a result, and inspect the output file.

```python
import tempfile, os, yaml
import pipestat

# 1. Write a minimal schema (pipeline_name is REQUIRED)
schema = {
    "pipeline_name": "my_pipeline",
    "samples": {
        "count": {"type": "integer", "description": "Number of items"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()

# 2. Create a manager with file backend
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name
psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name,
)

# 3. Report a result
psm.report(record_identifier="sample1", values={"count": 42})

# 4. See what was written
with open(results_file) as f:
    print(f.read())

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** The results file is a YAML file with the structure `pipeline_name > sample > record_identifier > key: value`. The `pipeline_name` from the schema becomes the top-level key.

---

## Recipe 2: Report an image or file result

Images and files use a dict with `path` and `title` keys. For images, `thumbnail_path` is optional (falls back to `path`).

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "imaging_pipeline",
    "samples": {
        "qc_plot": {
            "type": "object",
            "object_type": "image",
            "properties": {
                "path": {"type": "string", "description": "Path to image file"},
                "thumbnail_path": {"type": "string", "description": "Path to thumbnail"},
                "title": {"type": "string", "description": "Display title"},
            },
            "required": ["path", "title"],
            "description": "QC plot image",
        },
        "output_csv": {
            "type": "object",
            "object_type": "file",
            "properties": {
                "path": {"type": "string", "description": "Path to file"},
                "title": {"type": "string", "description": "Display title"},
            },
            "required": ["path", "title"],
            "description": "Output data file",
        },
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name

psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name,
)

# Report an image (thumbnail_path is optional)
psm.report(record_identifier="sample1", values={
    "qc_plot": {"path": "plots/qc.png", "title": "QC Plot"},
})

# Report a file
psm.report(record_identifier="sample1", values={
    "output_csv": {"path": "results/data.csv", "title": "Output Data"},
})

print(psm.retrieve_one(record_identifier="sample1"))
os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** Image and file results are stored as dicts with `path` and `title`. The `object_type` field tells pipestat how to render them in HTML reports. `thumbnail_path` is optional for images -- it defaults to `path` when not provided.

---

## Recipe 3: Retrieve results for one record

Use `retrieve_one()` to get a full record, a single value, or specific keys.

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "test",
    "samples": {
        "count": {"type": "integer", "description": "A count"},
        "name": {"type": "string", "description": "A name"},
        "ratio": {"type": "number", "description": "A ratio"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name
psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name,
)
psm.report(record_identifier="sample1", values={"count": 42, "name": "foo", "ratio": 0.95})

# Full record as dict
record = psm.retrieve_one(record_identifier="sample1")
print(record)  # {"count": 42, "name": "foo", "ratio": 0.95, ...}

# Single value (unwrapped scalar)
count = psm.retrieve_one(record_identifier="sample1", result_identifier="count")
print(count)  # 42

# Multiple specific keys
subset = psm.retrieve_one(record_identifier="sample1", result_identifier=["count", "name"])
print(subset)  # {"count": 42, "name": "foo"}

# Handle missing records
from pipestat.exceptions import RecordNotFoundError
try:
    psm.retrieve_one(record_identifier="nonexistent")
except RecordNotFoundError as e:
    print(f"Caught: {e}")

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** `retrieve_one()` with no `result_identifier` returns the full record dict. With a string, it returns the unwrapped value. With a list, it returns a dict of just those keys.

---

## Recipe 4: Query results with filter conditions

Use `select_records()` to query across multiple records with filters.

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "test",
    "samples": {
        "count": {"type": "integer", "description": "A count"},
        "name": {"type": "string", "description": "A name"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name
psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name,
)

# Report several records
for i, name in enumerate(["alpha", "beta", "gamma", "delta"], start=1):
    psm.report(record_identifier=f"sample{i}", values={"count": i * 10, "name": name})

# All records (default limit=1000)
result = psm.select_records()
print(f"Total: {result['total_size']}")
for r in result["records"]:
    print(r)

# Filter: count >= 20
result = psm.select_records(
    filter_conditions=[{"key": "count", "operator": "ge", "value": 20}],
)
print(f"count >= 20: {len(result['records'])} records")

# Specific columns only
result = psm.select_records(columns=["count"])
print(result["records"])

# Multiple filters with OR
result = psm.select_records(
    filter_conditions=[
        {"key": "count", "operator": "eq", "value": 10},
        {"key": "count", "operator": "eq", "value": 40},
    ],
    bool_operator="OR",
)
print(f"count=10 OR count=40: {len(result['records'])} records")

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** `select_records()` returns a dict with `total_size`, `page_size`, `next_page_token`, and `records`. Filter operators are: `eq` (==), `lt` (<), `ge` (>=), `in` (membership), `like` (SQL LIKE pattern). Multiple filters default to AND; use `bool_operator="OR"` for OR logic.

---

## Recipe 5: Set and check pipeline status

Use `set_status()` and `get_status()` to track pipeline run state.

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "test",
    "samples": {
        "count": {"type": "integer", "description": "A count"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name
psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name,
)
psm.report(record_identifier="sample1", values={"count": 1})

# Set status
psm.set_status(record_identifier="sample1", status_identifier="running")
print(psm.get_status(record_identifier="sample1"))  # "running"

# Update status
psm.set_status(record_identifier="sample1", status_identifier="completed")
print(psm.get_status(record_identifier="sample1"))  # "completed"

# Clear status (removes flag files)
psm.clear_status(record_identifier="sample1")
print(psm.get_status(record_identifier="sample1"))  # None

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** Status is tracked via flag files in the same directory as the results file. Default statuses are: running, completed, failed, waiting, partial. Custom status schemas can define additional identifiers.

---

## Recipe 6: Use schema-free mode

Report arbitrary key-value pairs without a schema. File paths are auto-wrapped.

```python
import tempfile, os
import pipestat

results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name

# No schema, no validation
psm = pipestat.PipestatManager(
    results_file_path=results_file,
    pipeline_name="adhoc",
    validate_results=False,
)

# Report arbitrary values -- no schema needed
psm.report(record_identifier="sample1", values={
    "alignment_rate": 0.95,
    "read_count": 1000000,
    "label": "experiment_A",
})

# File paths with image extensions are auto-wrapped to {"path": ..., "title": ...}
psm.report(record_identifier="sample1", values={
    "qc_plot": "plots/qc.png",  # auto-wrapped because .png
})

print(psm.retrieve_one(record_identifier="sample1"))

# Later, generate a schema from the results file:
# CLI: pipestat infer-schema -f results.yaml -o inferred_schema.yaml
# Python:
from pipestat.infer import infer_schema
schema = infer_schema(results_file)
print(schema)

os.unlink(results_file)
```

**What happens:** With `validate_results=False` and no schema, pipestat stores any key-value pair. String values with image extensions (.png, .jpg, .svg, etc.) or file extensions (.csv, .tsv, .json, etc.) are automatically wrapped into `{"path": ..., "title": ...}` dicts. Use `pipestat infer-schema` to generate a schema from existing results.

---

## Recipe 7: Report project-level results

Use `pipeline_type="project"` for aggregate or project-wide results. The `record_identifier` auto-defaults to `project_name`.

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "aggregator",
    "project": {
        "total_reads": {"type": "integer", "description": "Total reads across all samples"},
        "summary_table": {"type": "string", "description": "Path to summary"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name

psm = pipestat.PipestatManager.from_file_backend(
    results_file,
    schema_path=schema_file.name,
    pipeline_type="project",
    project_name="my_project",
)

# record_identifier auto-defaults to project_name ("my_project")
psm.report(values={"total_reads": 5000000})
psm.report(values={"summary_table": "results/summary.tsv"})

# Retrieve -- record_identifier defaults to project_name
result = psm.retrieve_one()
print(result)

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** For project-level pipelines, the schema uses a `project` section instead of `samples`. The `record_identifier` automatically defaults to `project_name`, so you don't need to pass it to `report()` or `retrieve_one()`.

---

## Recipe 8: Use PipestatDualManager for dual sample+project reporting

Use `PipestatDualManager` when your pipeline reports results at both sample and project levels.

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "dual_pipeline",
    "samples": {
        "read_count": {"type": "integer", "description": "Reads per sample"},
    },
    "project": {
        "total_reads": {"type": "integer", "description": "Total reads"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name

# PipestatDualManager creates both .sample and .project sub-managers
dual = pipestat.PipestatDualManager(
    schema_path=schema_file.name,
    results_file_path=results_file,
    project_name="my_project",
)

# Report sample-level results via .sample
dual.sample.report(record_identifier="sample1", values={"read_count": 1000})
dual.sample.report(record_identifier="sample2", values={"read_count": 2000})

# Report project-level results via .project (record_identifier defaults to project_name)
dual.project.report(values={"total_reads": 3000})

print("Sample:", dual.sample.retrieve_one(record_identifier="sample1"))
print("Project:", dual.project.retrieve_one())

# Alternative: use a single PipestatManager with level= parameter
psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name, project_name="my_project",
)
psm.report(record_identifier="sample3", values={"read_count": 500}, level="sample")
psm.report(values={"total_reads": 3500}, level="project")

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** `PipestatDualManager` holds both a `SamplePipestatManager` (.sample) and a `ProjectPipestatManager` (.project). Alternatively, a single `PipestatManager` can switch levels per-call using the `level=` parameter on `report()`, `retrieve_one()`, `select_records()`, and `remove()`.

---

## Recipe 9: Generate an HTML summary report

Use `summarize()` for HTML reports and `table()` for TSV/YAML export.

```python
import tempfile, os, yaml
import pipestat

schema = {
    "pipeline_name": "reporter",
    "samples": {
        "score": {"type": "number", "description": "Quality score"},
        "label": {"type": "string", "description": "Sample label"},
    },
}
schema_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(schema, schema_file)
schema_file.close()
results_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name
output_dir = tempfile.mkdtemp()

psm = pipestat.PipestatManager.from_file_backend(
    results_file, schema_path=schema_file.name,
)

# Report results for several samples
for i in range(1, 6):
    psm.report(record_identifier=f"sample{i}", values={
        "score": i * 0.2,
        "label": f"batch_{i % 3}",
    })

# Generate HTML table report
report_path = psm.summarize(output_dir=output_dir)
print(f"Report: {report_path}")

# Generate image gallery report (useful when results include images)
# gallery_path = psm.summarize(output_dir=output_dir, mode="gallery")

# Generate TSV stats and YAML objects files
table_paths = psm.table(output_dir=output_dir)
print(f"Tables: {table_paths}")

os.unlink(schema_file.name)
os.unlink(results_file)
```

**What happens:** `summarize()` produces a browsable HTML report. The default mode is "table"; use `mode="gallery"` for image-centric reports. `table()` produces a TSV file of scalar results and a YAML file of complex (file/image/object) results. Pass `portable=True` to `summarize()` to create a ZIP archive with all figures embedded.

---

## Recipe 10: Infer a schema from existing results

Generate a schema from a results YAML file, either via CLI or Python.

```python
import tempfile, os, yaml
import pipestat
from pipestat.infer import infer_schema

# Create a results file with mixed types (simulating existing pipeline output)
results = {
    "my_pipeline": {
        "sample": {
            "sample1": {
                "read_count": 1000,
                "alignment_rate": 0.95,
                "label": "experiment_A",
                "qc_plot": {"path": "plots/qc.png", "title": "QC"},
            },
            "sample2": {
                "read_count": 2000,
                "alignment_rate": 0.88,
                "label": "experiment_B",
                "qc_plot": {"path": "plots/qc2.png", "title": "QC 2"},
            },
        }
    }
}
results_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(results, results_file)
results_file.close()

# Python API
schema = infer_schema(results_file.name)
print(yaml.dump(schema, default_flow_style=False))

# Write to file
output_schema = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False).name
infer_schema(results_file.name, output_file=output_schema)
print(f"Schema written to: {output_schema}")

# CLI equivalent:
# pipestat infer-schema -f results.yaml -o inferred_schema.yaml
# pipestat infer-schema -f results.yaml --level sample --strict

os.unlink(results_file.name)
os.unlink(output_schema)
```

**What happens:** `infer_schema()` reads a results YAML file, examines the values, and produces a schema. Integer, number, string, boolean, array, and object types are detected. File/image paths are detected by extension (.png, .jpg, .csv, etc.). Use `--strict` to error on type conflicts across records instead of using the most common type.

---

## Quick Reference

| Method | Purpose |
|--------|---------|
| `PipestatManager.from_file_backend(path, schema_path=)` | Create file-backed manager |
| `PipestatManager.from_config(config)` | Create manager from config (any backend) |
| `psm.report(record_identifier=, values=)` | Store results |
| `psm.retrieve_one(record_identifier=, result_identifier=)` | Get one record or value |
| `psm.select_records(filter_conditions=, columns=)` | Query multiple records |
| `psm.set_status(record_identifier=, status_identifier=)` | Set pipeline status |
| `psm.get_status(record_identifier=)` | Get pipeline status |
| `psm.remove(record_identifier=, result_identifier=)` | Remove result or record |
| `psm.summarize(output_dir=, mode=)` | Generate HTML report |
| `psm.table(output_dir=)` | Generate TSV/YAML summaries |
| `psm["id"]` / `psm["id"] = {...}` / `del psm["id"]` | Dict-style access shortcuts |
