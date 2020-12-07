Final targets: PipestatManager
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
pipestat standardizes reporting of pipeline results. It formalizes a way for pipeline developers and downstream tools developers to communicate -- results produced by a pipeline can easily and reliably become an input for downstream analyses. The ovject exposes API for interacting with the results can be backed by either a YAML-formatted file or a PostgreSQL database.


```python
def __init__(self, name, record_identifier=None, schema_path=None, results_file=None, database_config=None, database_only=False)
```

Initialize the object
#### Parameters:

- `name` (`str`):  namespace to report into. This will be the DB tablename if using DB as the object back-end
- `record_identifier` (`str`):  record identifier to report for. Thiscreates a weak bound to the record, which can be overriden in this object method calls
- `schema_path` (`str`):  path to the output schema that formalizesthe results structure
- `results_file` (`str`):  YAML file to report into, if file is used asthe object back-end
- `database_config` (`str`):  DB login credentials to report into,if DB is used as the object back-end




```python
def check_connection(self)
```

Check whether a PostgreSQL connection has been established
#### Returns:

- `bool`:  whether the connection has been established




```python
def check_record_exists(self, record_identifier=None)
```

Check if the record exists
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record


#### Returns:

- `bool`:  whether the record exists




```python
def check_result_exists(self, result_identifier, record_identifier=None)
```

Check if the result has been reported
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record
- `result_identifier` (`str`):  name of the result to check


#### Returns:

- `bool`:  whether the specified result has been reported for theindicated record in current namespace




```python
def close_postgres_connection(self)
```

Close connection and remove client bound



```python
def data(self)
```

Data object
#### Returns:

- `yacman.YacAttMap`:  the object that stores the reported data




```python
def db_cursor(self)
```

Establish connection and get a PostgreSQL database cursor, commit and close the connection afterwards
#### Returns:

- `LoggingCursor`:  Database cursor object




```python
def establish_postgres_connection(self, suppress=False)
```

Establish PostgreSQL connection using the config data
#### Parameters:

- `suppress` (`bool`):  whether to suppress any connection errors


#### Returns:

- `bool`:  whether the connection has been established successfully




```python
def file(self)
```

File path that the object is reporting the results into
#### Returns:

- `str`:  file path that the object is reporting the results into




```python
def name(self)
```

Namespace the object writes the results to
#### Returns:

- `str`:  Namespace the object writes the results to




```python
def record_count(self)
```

Number of records reported
#### Returns:

- `int`:  number of records reported




```python
def record_identifier(self)
```

Namespace the object writes the results to
#### Returns:

- `str`:  Namespace the object writes the results to




```python
def remove(self, record_identifier=None, result_identifier=None)
```

Report a result.

If no result ID specified or last result is removed, the entire record
will be removed.
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record
- `result_identifier` (`str`):  name of the result to be removed or Noneif the record should be removed.


#### Returns:

- `bool`:  whether the result has been removed




```python
def report(self, values, record_identifier=None, force_overwrite=False, strict_type=True, return_id=False)
```

Report a result.
#### Parameters:

- `values` (`dict[str, any]`):  dictionary of result-value pairs
- `record_identifier` (`str`):  unique identifier of the record, value toin 'record_identifier' column to look for to determine if the record already exists
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
def retrieve(self, record_identifier=None, result_identifier=None)
```

Retrieve a result for a record.

If no result ID specified, results for the entire record will
be returned.
#### Parameters:

- `record_identifier` (`str`):  unique identifier of the record
- `result_identifier` (`str`):  name of the result to be retrieved


#### Returns:

- `any | dict[str, any]`:  a single result or a mapping with all theresults reported for the record




```python
def schema(self)
```

Schema mapping
#### Returns:

- `dict`:  schema that formalizes the results structure




```python
def select(self, columns=None, condition=None, condition_val=None)
```

Get all the contents from the selected table, possibly restricted by the provided condition.
#### Parameters:

- `columns` (`str | list[str]`):  columns to select
- `condition` (`str`):  condition to restrict the resultswith, will be appended to the end of the SELECT statement and safely populated with 'condition_val', for example: `"id=%s"`
- `condition_val` (`list`):  values to fill the placeholderin 'condition' with


#### Returns:

- `list[psycopg2.extras.DictRow]`:  all table contents




```python
def validate_schema(self)
```

Check schema for any possible issues
#### Raises:

- `SchemaError`:  if any schema format issue is detected







*Version Information: `pipestat` v0.0.1, generated by `lucidoc` v0.4.3*
