![Run pytests](https://github.com/pepkit/pipestat/workflows/Run%20pytests/badge.svg)
[![codecov](https://codecov.io/gh/pepkit/pipestat/branch/master/graph/badge.svg?token=O07MXSQZ32)](https://codecov.io/gh/pepkit/pipestat)

<img src="https://raw.githubusercontent.com/pepkit/pipestat/master/docs/img/pipestat_logo.svg?sanitize=true" alt="pipestat" height="70"/><br>

# What is this?

Pipestat standardizes reporting of pipeline results. It provides 1) a standard specification for how pipeline outputs should be stored; and 2) an implementation to easily write results to that format from within Python or from the command line.

# How does it work?

A pipeline author defines all the outputs produced by a pipeline by writing a JSON-schema. The pipeline then uses pipestat to report pipeline outputs as the pipeline runs, either via the Python API or command line interface. The user configures results to be stored either in a [YAML-formatted file](https://yaml.org/spec/1.2/spec.html) or a [PostgreSQL database](https://www.postgresql.org/). The results are recorded according to the pipestat specification, in a standard, pipeline-agnostic way. This way, downstream software can use this specification to create universal tools for analyzing, monitoring, and visualizing pipeline results that will work with any pipeline or workflow.


# Quick start

## Install pipestat

```console
pip install pipestat
```

## Report result

From command line:

```console
pipestat report -f results.yaml -n namespace -r record_id -i result_name -v 1.1 -s schema.yaml
```

From Python:

```python
import pipestat

psm = pipestat.PipestatManager(namespace="namespace", results_file_path="results.yaml", schema_path="schema.yaml")
psm.report(record_identifier="record_id", values={"result_name": 1.1})
```
 
## Retrieve a result

From command line:

```console
pipestat retrieve -f results.yaml -n namespace -r record_id -i result_name
```

From Python:

```python
import pipestat

psm = pipestat.PipestatManager(namespace="namespace", results_file_path="results.yaml")
psm.retrieve(record_identifier="record_id", result_identifier="result_name")
```
 

