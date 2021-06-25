# Usage reference

Pipestat offers a CLI that can be access via the `pipestat` command in the shell. It offers complete control over reporting, inspecting, etc, via a series of subcommands.

Here you can see the command-line usage instructions for the main command and for each subcommand:
## `pipestat --help`
```console
version: 0.1.0
usage: pipestat [-h] [--version] [--silent] [--verbosity V] [--logdev]
                {report,inspect,remove,retrieve,status} ...

pipestat - report pipeline results

positional arguments:
  {report,inspect,remove,retrieve,status}
    report              Report a result.
    inspect             Inspect a database.
    remove              Remove a result.
    retrieve            Retrieve a result.
    status              Manage pipeline status.

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --silent              Silence logging. Overrides verbosity.
  --verbosity V         Set logging level (1-5 or logging module level name)
  --logdev              Expand content of logging message format.

Pipestat standardizes reporting of pipeline results and pipeline status
management. It formalizes a way for pipeline developers and downstream tools
developers to communicate -- results produced by a pipeline can easily and
reliably become an input for downstream analyses. The object exposes API for
interacting with the results and pipeline status and can be backed by either a
YAML-formatted file or a database.
```

## `pipestat report --help`
```console
usage: pipestat report [-h] [-n N] [-f F] [-c C] [-a] [-s S] [--status-schema ST]
                       [--flag-dir FD] -i I [-r R] -v V [-o] [-t]

Report a result.

optional arguments:
  -h, --help                   show this help message and exit
  -n N, --namespace N          Name of the pipeline to report result for. If not provided
                               'PIPESTAT_NAMESPACE' env var will be used. Currently not
                               set
  -f F, --results-file F       Path to the YAML file where the results will be stored.
                               This file will be used as pipestat backend and to restore
                               the reported results across sessions
  -c C, --config C             Path to the YAML configuration file. If not provided
                               'PIPESTAT_CONFIG' env var will be used. Currently not set
  -a, --database-only          Whether the reported data should not be stored in the
                               memory, only in the database.
  -s S, --schema S             Path to the schema that defines the results that can be
                               reported. If not provided 'PIPESTAT_RESULTS_SCHEMA' env var
                               will be used. Currently not set
  --status-schema ST           Path to the status schema. Default will be used if not
                               provided: /usr/local/lib/python3.9/site-
                               packages/pipestat/schemas/status_schema.yaml
  --flag-dir FD                Path to the flag directory in case YAML file is the
                               pipestat backend.
  -i I, --result-identifier I  ID of the result to report; needs to be defined in the
                               schema
  -r R, --record-identifier R  ID of the record to report the result for. If not provided
                               'PIPESTAT_RECORD_ID' env var will be used. Currently not
                               set
  -v V, --value V              Value of the result to report
  -o, --overwrite              Whether the result should override existing ones in case of
                               name clashes
  -t, --skip-convert           Whether skip result type conversion into the reqiuired
                               class in case it does not meet the schema requirements
```

## `pipestat inspect --help`
```console
usage: pipestat inspect [-h] [-n N] [-f F] [-c C] [-a] [-s S] [--status-schema ST]
                        [--flag-dir FD] [-d]

Inspect a database.

optional arguments:
  -h, --help              show this help message and exit
  -n N, --namespace N     Name of the pipeline to report result for. If not provided
                          'PIPESTAT_NAMESPACE' env var will be used. Currently not set
  -f F, --results-file F  Path to the YAML file where the results will be stored. This
                          file will be used as pipestat backend and to restore the
                          reported results across sessions
  -c C, --config C        Path to the YAML configuration file. If not provided
                          'PIPESTAT_CONFIG' env var will be used. Currently not set
  -a, --database-only     Whether the reported data should not be stored in the memory,
                          only in the database.
  -s S, --schema S        Path to the schema that defines the results that can be
                          reported. If not provided 'PIPESTAT_RESULTS_SCHEMA' env var will
                          be used. Currently not set
  --status-schema ST      Path to the status schema. Default will be used if not provided:
                          /usr/local/lib/python3.9/site-
                          packages/pipestat/schemas/status_schema.yaml
  --flag-dir FD           Path to the flag directory in case YAML file is the pipestat
                          backend.
  -d, --data              Whether to display the data
```

## `pipestat remove --help`
```console
usage: pipestat remove [-h] [-n N] [-f F] [-c C] [-a] [-s S] [--status-schema ST]
                       [--flag-dir FD] -i I [-r R]

Remove a result.

optional arguments:
  -h, --help                   show this help message and exit
  -n N, --namespace N          Name of the pipeline to report result for. If not provided
                               'PIPESTAT_NAMESPACE' env var will be used. Currently not
                               set
  -f F, --results-file F       Path to the YAML file where the results will be stored.
                               This file will be used as pipestat backend and to restore
                               the reported results across sessions
  -c C, --config C             Path to the YAML configuration file. If not provided
                               'PIPESTAT_CONFIG' env var will be used. Currently not set
  -a, --database-only          Whether the reported data should not be stored in the
                               memory, only in the database.
  -s S, --schema S             Path to the schema that defines the results that can be
                               reported. If not provided 'PIPESTAT_RESULTS_SCHEMA' env var
                               will be used. Currently not set
  --status-schema ST           Path to the status schema. Default will be used if not
                               provided: /usr/local/lib/python3.9/site-
                               packages/pipestat/schemas/status_schema.yaml
  --flag-dir FD                Path to the flag directory in case YAML file is the
                               pipestat backend.
  -i I, --result-identifier I  ID of the result to report; needs to be defined in the
                               schema
  -r R, --record-identifier R  ID of the record to report the result for. If not provided
                               'PIPESTAT_RECORD_ID' env var will be used. Currently not
                               set
```

## `pipestat retrieve --help`
```console
usage: pipestat retrieve [-h] [-n N] [-f F] [-c C] [-a] [-s S] [--status-schema ST]
                         [--flag-dir FD] -i I [-r R]

Retrieve a result.

optional arguments:
  -h, --help                   show this help message and exit
  -n N, --namespace N          Name of the pipeline to report result for. If not provided
                               'PIPESTAT_NAMESPACE' env var will be used. Currently not
                               set
  -f F, --results-file F       Path to the YAML file where the results will be stored.
                               This file will be used as pipestat backend and to restore
                               the reported results across sessions
  -c C, --config C             Path to the YAML configuration file. If not provided
                               'PIPESTAT_CONFIG' env var will be used. Currently not set
  -a, --database-only          Whether the reported data should not be stored in the
                               memory, only in the database.
  -s S, --schema S             Path to the schema that defines the results that can be
                               reported. If not provided 'PIPESTAT_RESULTS_SCHEMA' env var
                               will be used. Currently not set
  --status-schema ST           Path to the status schema. Default will be used if not
                               provided: /usr/local/lib/python3.9/site-
                               packages/pipestat/schemas/status_schema.yaml
  --flag-dir FD                Path to the flag directory in case YAML file is the
                               pipestat backend.
  -i I, --result-identifier I  ID of the result to report; needs to be defined in the
                               schema
  -r R, --record-identifier R  ID of the record to report the result for. If not provided
                               'PIPESTAT_RECORD_ID' env var will be used. Currently not
                               set
```

## `pipestat status --help`
```console
usage: pipestat status [-h] {set,get} ...

Manage pipeline status.

positional arguments:
  {set,get}
    set       Set status.
    get       Get status.

optional arguments:
  -h, --help  show this help message and exit
```

## `pipestat status get --help`
```console
usage: pipestat status get [-h] [-n N] [-f F] [-c C] [-a] [-s S] [--status-schema ST]
                           [--flag-dir FD] [-r R]

Get status.

optional arguments:
  -h, --help                   show this help message and exit
  -n N, --namespace N          Name of the pipeline to report result for. If not provided
                               'PIPESTAT_NAMESPACE' env var will be used. Currently not
                               set
  -f F, --results-file F       Path to the YAML file where the results will be stored.
                               This file will be used as pipestat backend and to restore
                               the reported results across sessions
  -c C, --config C             Path to the YAML configuration file. If not provided
                               'PIPESTAT_CONFIG' env var will be used. Currently not set
  -a, --database-only          Whether the reported data should not be stored in the
                               memory, only in the database.
  -s S, --schema S             Path to the schema that defines the results that can be
                               reported. If not provided 'PIPESTAT_RESULTS_SCHEMA' env var
                               will be used. Currently not set
  --status-schema ST           Path to the status schema. Default will be used if not
                               provided: /usr/local/lib/python3.9/site-
                               packages/pipestat/schemas/status_schema.yaml
  --flag-dir FD                Path to the flag directory in case YAML file is the
                               pipestat backend.
  -r R, --record-identifier R  ID of the record to report the result for. If not provided
                               'PIPESTAT_RECORD_ID' env var will be used. Currently not
                               set
```

## `pipestat status set --help`
```console
usage: pipestat status set [-h] [-n N] [-f F] [-c C] [-a] [-s S] [--status-schema ST]
                           [--flag-dir FD] [-r R]
                           status_identifier

Set status.

positional arguments:
  status_identifier            Status identifier to set.

optional arguments:
  -h, --help                   show this help message and exit
  -n N, --namespace N          Name of the pipeline to report result for. If not provided
                               'PIPESTAT_NAMESPACE' env var will be used. Currently not
                               set
  -f F, --results-file F       Path to the YAML file where the results will be stored.
                               This file will be used as pipestat backend and to restore
                               the reported results across sessions
  -c C, --config C             Path to the YAML configuration file. If not provided
                               'PIPESTAT_CONFIG' env var will be used. Currently not set
  -a, --database-only          Whether the reported data should not be stored in the
                               memory, only in the database.
  -s S, --schema S             Path to the schema that defines the results that can be
                               reported. If not provided 'PIPESTAT_RESULTS_SCHEMA' env var
                               will be used. Currently not set
  --status-schema ST           Path to the status schema. Default will be used if not
                               provided: /usr/local/lib/python3.9/site-
                               packages/pipestat/schemas/status_schema.yaml
  --flag-dir FD                Path to the flag directory in case YAML file is the
                               pipestat backend.
  -r R, --record-identifier R  ID of the record to report the result for. If not provided
                               'PIPESTAT_RECORD_ID' env var will be used. Currently not
                               set
```
