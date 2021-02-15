# Environment variables in pipestat

Both the command line interface (CLI) and Python API support a collection of environment variables, which can be used to configure pipestat actions.

Here is a list of the supported environment variables:


| Environment variable       | API argument       | Description                                                                                                                 |
|----------------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------|
| **PIPESTAT_NAMESPACE**     | namespace          | namespace to report into. This will be the DBtable name if using DB as the object back-end                                  |
| **PIPESTAT_RECORD_ID**      | record_identifier  | record identifier to report for. Thiscreates a weak bound to the record, which can be overriden in this object method calls |
| **PIPESTAT_CONFIG**         | config             | path to the configuration file or a mappingwith the config file content                                                     |
| **PIPESTAT_RESULTS_FILE**   | results_file_path  | YAML file to report into, if file isused as the object back-end                                                             |
| **PIPESTAT_RESULTS_SCHEMA** | schema_path        | path to the output schema that formalizesthe results structure                                                              |
| **PIPESTAT_STATUS_SCHEMA**  | status_schema_path | path to the status schema that formalizes the status flags structure                                                        |