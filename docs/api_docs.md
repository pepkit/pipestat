Final targets: PipestatError, PipestatManager
<script>
document.addEventListener('DOMContentLoaded', (event) => {
  document.querySelectorAll('h3 code').forEach((block) => {
    hljs.highlightBlock(block);
  });
});
</script>

<style>
h3 .content { 
    padding-left: 22px;
    text-indent: -15px;
 }
h3 .hljs .content {
    padding-left: 20px;
    margin-left: 0px;
    text-indent: -15px;
    martin-bottom: 0px;
}
h4 .content, table .content, p .content, li .content { margin-left: 30px; }
h4 .content { 
    font-style: italic;
    font-size: 1em;
    margin-bottom: 0px;
}

</style>


# Package `pipestat` Documentation

## <a name="PipestatError"></a> Class `PipestatError`
Base exception type for this package


## <a name="PipestatManager"></a> Class `PipestatManager`
Pipestat standardizes reporting of pipeline results and pipeline status management. It formalizes a way for pipeline developers and downstream tools developers to communicate -- results produced by a pipeline can easily and reliably become an input for downstream analyses. A PipestatManager object exposes an API for interacting with the results and pipeline status and can be backed by either a YAML-formatted file or a database.


```python
def __init__(self, sample_name: Optional[str]=None, schema_path: Optional[str]=None, results_file_path: Optional[str]=None, database_only: Optional[bool]=True, config_file: Optional[str]=None, config_dict: Optional[dict]=None, flag_file_dir: Optional[str]=None, show_db_logs: bool=False, pipeline_type: Optional[str]=None, pipeline_name: Optional[str]='default_pipeline_name', result_formatter: staticmethod=<function default_formatter at 0x7f3c2fc69360>, multi_pipelines: bool=False)
```

Initialize the PipestatManager object
#### Parameters:

- `sample_name` (`str`):  record identifier to report for. Thiscreates a weak bound to the record, which can be overridden in this object method calls
- `schema_path` (`str`):  path to the output schema that formalizesthe results structure
- `results_file_path` (`str`):  YAML file to report into, if file isused as the object back-end
- `database_only` (`bool`):  whether the reported data should not bestored in the memory, but only in the database
- `config` (`str | dict`):  path to the configuration file or a mappingwith the config file content
- `flag_file_dir` (`str`):  path to directory containing flag files
- `show_db_logs` (`bool`):  Defaults to False, toggles showing database logs
- `pipeline_type` (`str`):  "sample" or "project"
- `result_formatter` (`str`):  function for formatting result
- `multi_pipelines` (`bool`):  allows for running multiple pipelines for one file backend




```python
def clear_status(self, *args, **kwargs)
```



```python
def config_path(self)
```

Config path. None if the config was not provided or if provided as a mapping of the config contents
#### Returns:

- `str`:  path to the provided config




```python
def count_records(self, *args, **kwargs)
```



```python
def data(self)
```

Data object
#### Returns:

- `yacman.YAMLConfigManager`:  the object that stores the reported data




```python
def db_url(self)
```

Database URL, generated based on config credentials
#### Returns:

- `str`:  database URL


#### Raises:

- `PipestatDatabaseError`:  if the object is not backed by a database




```python
def file(self)
```

File path that the object is reporting the results into
#### Returns:

- `str`:  file path that the object is reporting the results into




```python
def get_status(self, *args, **kwargs)
```



```python
def highlighted_results(self)
```

Highlighted results
#### Returns:

- `List[str]`:  a collection of highlighted results




```python
def pipeline_name(self)
```

Pipeline name
#### Returns:

- `str`:  Pipeline name




```python
def pipeline_type(self)
```

Pipeline type: "sample" or "project"
#### Returns:

- `str`:  pipeline type




```python
def process_schema(self, schema_path)
```



```python
def project_name(self)
```

Project name the object writes the results to
#### Returns:

- `str`:  project name the object writes the results to




```python
def record_count(self)
```

Number of records reported
#### Returns:

- `int`:  number of records reported




```python
def remove(self, *args, **kwargs)
```



```python
def report(self, *args, **kwargs)
```



```python
def result_schemas(self)
```

Result schema mappings
#### Returns:

- `dict`:  schemas that formalize the structure of each resultin a canonical jsonschema way




```python
def retrieve(self, *args, **kwargs)
```



```python
def sample_name(self)
```

Unique identifier of the record
#### Returns:

- `str`:  unique identifier of the record




```python
def schema(self)
```

Schema mapping
#### Returns:

- `dict`:  schema that formalizes the results structure




```python
def schema_path(self)
```

Schema path
#### Returns:

- `str`:  path to the provided schema




```python
def set_status(self, *args, **kwargs)
```



```python
def status_schema(self)
```

Status schema mapping
#### Returns:

- `dict`:  schema that formalizes the pipeline status structure




```python
def status_schema_source(self)
```

Status schema source
#### Returns:

- `dict`:  source of the schema that formalizesthe pipeline status structure




```python
def summarize(self, *args, **kwargs)
```



```python
def validate_schema(self) -> None
```

Check schema for any possible issues
#### Raises:

- `SchemaError`:  if any schema format issue is detected







*Version Information: `pipestat` v0.4.0, generated by `lucidoc` v0.4.4*
