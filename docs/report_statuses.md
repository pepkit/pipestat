# Reporting record identifier/sample statuses

Ensure you set a directory to contain the status files if using a file backend:

```python
psm = PipestatManager(results_file_path="results.yaml",schema_path="output_schema.yaml", flag_file_dir="./flags/")

```

Now, when running your pipeline you can simply set the status based of the current record:

```python
psm.set_status(record_identifier="sample1", status_identifier="completed")
```

All statuses are defined in the schemas/status_schema.yaml file:

```yaml
running:
  description: "the pipeline is running"
  color: [30, 144, 255] # dodgerblue
completed:
  description: "the pipeline has completed"
  color: [50, 205, 50] # limegreen
failed:
  description: "the pipeline has failed"
  color: [220, 20, 60] # crimson
waiting:
  description: "the pipeline is waiting"
  color: [240, 230, 140] # khaki
partial:
  description: "the pipeline stopped before completion point"
  color: [169, 169, 169] # darkgray

```


### Coming from Looper? Make sure to set this flag directory in the looper config file:

``` 
pep_config: ./metadata/pep_config.yaml # pephub registry path or local path
output_dir: ./results
pipeline_interfaces:
  - pipeline/pipeline_interface.yaml
pipestat:
  project_name: count_lines
  results_file_path: results.yaml
  flag_file_dir: results/flags

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
