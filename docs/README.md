![Run pytests](https://github.com/pepkit/pipestat/workflows/Run%20pytests/badge.svg)
[![codecov](https://codecov.io/gh/pepkit/pipestat/branch/master/graph/badge.svg?token=O07MXSQZ32)](https://codecov.io/gh/pepkit/pipestat)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

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

## Set environment variables (optional)

```console
export PIPESTAT_RESULTS_SCHEMA=output_schema.yaml
export PIPESTAT_RECORD_IDENTIFIER=my_record
export PIPESTAT_RESULTS_FILE=results_file.yaml
```
Note: When setting environment variables as in the above example, you will need to provide an output_schema.yaml file in your current working directory with the following example data:
```yaml
title: An example Pipestat output schema
description: A pipeline that uses pipestat to report sample and project level results.
type: object
properties:
  pipeline_name: "default_pipeline_name"
  samples:
    type: object
    properties:
        result_name:
          type: string
          description: "ResultName"
```

## Pipeline results reporting and retrieval
### For these examples below, it is assumed that the proper environment variables (see above) have been set.

### Report a result

From command line:

```console
pipestat report -i result_name -v 1.1
```

From Python:

```python
import pipestat

psm = pipestat.PipestatManager()
psm.report(values={"result_name": 1.1})
```

### Retrieve a result

From command line:

```console
pipestat retrieve -r my_record
```

From Python:

```python
import pipestat

psm = pipestat.PipestatManager()
psm.retrieve_one(result_identifier="result_name")
```

## Pipeline status management

## Set status

From command line:

```console
pipestat status set running
```

From Python:

```python
import pipestat

psm = pipestat.PipestatManager()
psm.set_status(status_identifier="running")
```

## Get status

From command line:

```console
pipestat status get
```

From Python:

```python
import pipestat

psm = pipestat.PipestatManager()
psm.get_status()
```

## Is a `running` status stale?

When a scheduler kills a job (timeout, out of memory, `scancel`, node failure), the pipeline never gets to set its status to `failed`, so the status stays `running`. Use this feature if you are a tool or a person that sees a `running` status and needs to know whether the job behind it is really still alive on a cluster. For local runs there is nothing to check.

pipestat stores the id of the job that set a status: pass `job_id` to `set_status`, or leave it out and pipestat uses `$SLURM_JOB_ID` when set. The status itself is unchanged; the file backend writes the id to a `<flag>.job` file next to the flag, and the database backend to a `<table>_status_job` side table (PEPhub does not store it). Then `resolve_status` asks the scheduler about that job and reports `stale=True` when the status is `running` but the job is finished or gone. pipestat only reports; it never rewrites the status. The caller decides what to do, for example set it to `failed` or rerun.

```python
from pipestat import slurm_job_state

psm.set_status("running", "sample1", job_id="12345")  # or just $SLURM_JOB_ID
psm.get_status_job_id("sample1")  # "12345"
psm.resolve_status("sample1", job_state_fn=slurm_job_state)
# StatusResolution(record_identifier='sample1', status='running',
#                  job_id='12345', job_state='timeout', stale=True)
```

```console
pipestat status set running -r sample1 --job-id 12345
pipestat status get -r sample1 --check-job
{"record_identifier": "sample1", "status": "running", "job_id": "12345", "job_state": "timeout", "stale": true}
```

`slurm_job_state` asks `sacct`, then `squeue`, and returns one of `pending`, `running`, `completed`, `failed`, `cancelled`, `timeout`, `out_of_memory`, `node_fail`, `absent` (the scheduler does not know the job) or `unknown` (the scheduler could not be asked, which never counts as stale). For another scheduler, pass your own function from job id to one of these states.
