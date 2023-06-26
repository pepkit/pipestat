# Environment variables in pipestat

Both the command line interface (CLI) and Python API support a collection of environment variables, which can be used to configure pipestat actions.

Here is a list of the supported environment variables:

| Environment variable        | API argument       | Description                                                                                                                  |
|-----------------------------|--------------------|------------------------------------------------------------------------------------------------------------------------------|
| **PIPESTAT_PROJECT_NAME**   | project_name       | namespace to report into. This will be the DBtable name if using DB as the object back-end                                   |
| **PIPESTAT_SAMPLE_NAME**    | sample_name        | record identifier to report for. This creates a weak bound to the record, which can be overriden in this object method calls |
| **PIPESTAT_CONFIG**         | config             | path to the configuration file or a mapping with the config file content                                                     |
| **PIPESTAT_RESULTS_FILE**   | results_file_path  | YAML file to report into, if file is used as the object back-end                                                             |
| **PIPESTAT_RESULTS_SCHEMA** | schema_path        | path to the output schema that formalizes the results structure                                                              |
| **PIPESTAT_STATUS_SCHEMA**  | status_schema_path | path to the status schema that formalizes the status flags structure                                                         |
