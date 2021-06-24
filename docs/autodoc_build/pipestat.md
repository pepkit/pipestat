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

## <a name="PipestatManager"></a> Class `PipestatManager`
Pipestat standardizes reporting of pipeline results and pipeline status management. It formalizes a way for pipeline developers and downstream tools developers to communicate -- results produced by a pipeline can easily and reliably become an input for downstream analyses. The object exposes API for interacting with the results and pipeline status and can be backed by either a YAML-formatted file or a database.


```python
def __init__(self, namespace: Optional[str]=None, record_identifier: Optional[str]=None, schema_path: Optional[str]=None, results_file_path: Optional[str]=None, database_only: Optional[bool]=True, config: Union[str, dict, NoneType]=None, status_schema_path: Optional[str]=None, flag_file_dir: Optional[str]=None, custom_declarative_base: Optional[sqlalchemy.orm.decl_api.DeclarativeMeta]=None, show_db_logs: bool=False)
```

Initialize the object
#### Parameters:

- `namespace` (`str`):  namespace to report into. This will be the DBtable name if using DB as the object back-end
- `record_identifier` (`str`):  record identifier to report for. Thiscreates a weak bound to the record, which can be overriden in this object method calls
- `schema_path` (`str`):  path to the output schema that formalizesthe results structure
- `results_file_path` (`str`):  YAML file to report into, if file isused as the object back-end
- `database_only` (`bool`):  whether the reported data should not bestored in the memory, but only in the database
- `config` (`str | dict`):  path to the configuration file or a mappingwith the config file content
- `status_schema_path` (`str`):  path to the status schema that formalizesthe status flags structure
- `custom_declarative_base` (`sqlalchemy.orm.DeclarativeMeta`):  a declarative base touse for ORMs creation a new instance will be created if not provided




```python
def assert_results_defined(self, results: List[str]) -> None
```

Assert provided list of results is defined in the schema
#### Parameters:

- `results` (`List[str]`):  list of results tocheck for existence in the schema


#### Raises:

- `SchemaError`:  if any of the results is not defined in the schema




```python
def check_record_exists(self, record_identifier: str, table_name: str=None) -> bool
```

Check if the specified record exists in the table
#### Parameters:

- `record_identifier` (`str`):  record to check for
- `table_name` (`str`):  table name to check


#### Returns:

- `bool`:  whether the record exists in the table




```python
def check_result_exists(self, result_identifier: str, record_identifier: str=None) -> bool
```

Check if the result has been reported
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record
- `result_identifier` (`str`):  name of the result to check


#### Returns:

- `bool`:  whether the specified result has been reported for theindicated record in current namespace




```python
def check_which_results_exist(self, results: List[str], rid: Optional[str]=None, table_name: Optional[str]=None) -> List[str]
```

Check which results have been reported
#### Parameters:

- `rid` (`str`):  unique identifier of the record
- `results` (`List[str]`):  names of the results to check


#### Returns:

- `List[str]`:  whether the specified result has been reported for theindicated record in current namespace




```python
def clear_status(self, record_identifier: str=None, flag_names: List[str]=None) -> List[Optional[str]]
```

Remove status flags
#### Parameters:

- `record_identifier` (`str`):  name of the record to remove flags for
- `flag_names` (`Iterable[str]`):  Names of flags to remove, optional; ifunspecified, all schema-defined flag names will be used.


#### Returns:

- `List[str]`:  Collection of names of flags removed




```python
def config_path(self)
```

Config path. None if the config was not provided or if provided as a mapping of the config contents
#### Returns:

- `str`:  path to the provided config




```python
def data(self)
```

Data object
#### Returns:

- `yacman.YacAttMap`:  the object that stores the reported data




```python
def db_column_kwargs_by_result(self)
```

Database column key word arguments for every result, sourced from the results schema in the `db_column` section
#### Returns:

- `Dict[str, Any]`:  key word arguments for every result




```python
def db_column_relationships_by_result(self)
```

Database column relationships for every result, sourced from the results schema in the `relationship` section

*Note: this is an experimental feature*
#### Returns:

- `Dict[str, Dict[str, str]]`:  relationships for every result




```python
def db_url(self)
```

Database URL, generated based on config credentials
#### Returns:

- `str`:  database URL


#### Raises:

- `PipestatDatabaseError`:  if the object is not backed by a database




```python
def establish_db_connection(self) -> bool
```

Establish DB connection using the config data
#### Returns:

- `bool`:  whether the connection has been established successfully




```python
def file(self)
```

File path that the object is reporting the results into
#### Returns:

- `str`:  file path that the object is reporting the results into




```python
def get_orm(self, table_name: str=None) -> Any
```

Get an object relational mapper class
#### Parameters:

- `table_name` (`str`):  table name to get a class for


#### Returns:

- `Any`:  Object relational mapper class




```python
def get_status(self, record_identifier: str=None) -> Optional[str]
```

Get the current pipeline status
#### Returns:

- `str`:  status identifier, like 'running'




```python
def get_status_flag_path(self, status_identifier: str, record_identifier=None) -> str
```

Get the path to the status file flag
#### Parameters:

- `status_identifier` (`str`):  one of the defined status IDs in schema
- `record_identifier` (`str`):  unique record ID, optional ifspecified in the object constructor


#### Returns:

- `str`:  absolute path to the flag file or None if object isbacked by a DB




```python
def highlighted_results(self)
```

Highlighted results
#### Returns:

- `List[str]`:  a collection of highlighted results




```python
def is_db_connected(self) -> bool
```

Check whether a DB connection has been established
#### Returns:

- `bool`:  whether the connection has been established




```python
def namespace(self)
```

Namespace the object writes the results to
#### Returns:

- `str`:  namespace the object writes the results to




```python
def record_count(self)
```

Number of records reported
#### Returns:

- `int`:  number of records reported




```python
def record_identifier(self)
```

Unique identifier of the record
#### Returns:

- `str`:  unique identifier of the record




```python
def remove(self, record_identifier: str=None, result_identifier: str=None) -> bool
```

Remove a result.

If no result ID specified or last result is removed, the entire record
will be removed.
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record
- `result_identifier` (`str`):  name of the result to be removed or Noneif the record should be removed.


#### Returns:

- `bool`:  whether the result has been removed




```python
def report(self, values: Dict[str, Any], record_identifier: str=None, force_overwrite: bool=False, strict_type: bool=True, return_id: bool=False) -> Union[bool, int]
```

Report a result.
#### Parameters:

- `values` (`Dict[str, any]`):  dictionary of result-value pairs
- `record_identifier` (`str`):  unique identifier of the record, valuein 'record_identifier' column to look for to determine if the record already exists
- `force_overwrite` (`bool`):  whether to overwrite the existing record
- `strict_type` (`bool`):  whether the type of the reported values shouldremain as is. Pipestat would attempt to convert to the schema-defined one otherwise
- `return_id` (`bool`):  PostgreSQL IDs of the records that have beenupdated. Not available with results file as backend


#### Returns:

- `bool | int`:  whether the result has been reported or the ID ofthe updated record in the table, if requested




```python
def result_schemas(self)
```

Result schema mappings
#### Returns:

- `dict`:  schemas that formalize the structure of each resultin a canonical jsonschema way




```python
def retrieve(self, record_identifier: Optional[str]=None, result_identifier: Optional[str]=None) -> Union[Any, Dict[str, Any]]
```

Retrieve a result for a record.

If no result ID specified, results for the entire record will
be returned.
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record
- `result_identifier` (`str`):  name of the result to be retrieved


#### Returns:

- `any | Dict[str, any]`:  a single result or a mapping with all theresults reported for the record




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
def select(self, table_name: Optional[str]=None, columns: Optional[List[str]]=None, filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]]=None, json_filter_conditions: Optional[List[Tuple[str, str, str]]]=None, offset: Optional[int]=None, limit: Optional[int]=None) -> List[Any]
```

Perform a `SELECT` on the table
#### Parameters:

- `table_name` (`str`):  name of the table to SELECT from
- `columns` (`List[str]`):  columns to include in the result
- `filter_conditions` (`[(key,operator,value)]`): - eq for == - lt for < - ge for >= - in for in_ - like for like
- `json_filter_conditions` (`[(col,key,value)]`):  conditions for JSONB column toquery that include JSON column name, key withing the JSON object in that column and the value to check the identity against. Therefore only '==' is supported in non-nested checks, e.g. [("other", "genome", "hg38")]
- `offset` (`int`):  skip this number of rows
- `limit` (`int`):  include this number of rows




```python
def select_txt(self, filter_templ: Optional[str]='', filter_params: Optional[Dict[str, Any]]={}, table_name: Optional[str]=None, offset: Optional[int]=None, limit: Optional[int]=None) -> List[Any]
```

Execute a query with a textual filter. Returns all results.

To retrieve all table contents, leave the filter arguments out.
Table name defaults to the namespace
#### Parameters:

- `filter_templ` (`str`):  filter template with value placeholders,formatted as follows `id<:value and name=:name`
- `filter_params` (`Dict[str, Any]`):  a mapping keys specified in the `filter_templ`to parameters that are supposed to replace the placeholders
- `table_name` (`str`):  name of the table to query
- `offset` (`int`):  skip this number of rows
- `limit` (`int`):  include this number of rows


#### Returns:

- `List[Any]`:  a list of matched records




```python
def session(self)
```

Provide a transactional scope around a series of query operations.



```python
def set_status(self, status_identifier: str, record_identifier: str=None) -> None
```

Set pipeline run status.

The status identifier needs to match one of identifiers specified in
the status schema. A basic, ready to use, status schema is shipped with
 this package.
#### Parameters:

- `status_identifier` (`str`):  status to set, one of statuses definedin the status schema
- `record_identifier` (`str`):  record identifier to set thepipeline status for




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
def validate_schema(self) -> None
```

Check schema for any possible issues
#### Raises:

- `SchemaError`:  if any schema format issue is detected







*Version Information: `pipestat` v0.1.0-dev, generated by `lucidoc` v0.4.2*
