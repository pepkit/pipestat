# Pipestat configuration

Pipestat *requires* a few pieces of information to run:

- a **namespace** to write into, for example the name of the pipeline
- a path to the **schema** file that describes results that can be reported
- **backend info**: either path to a YAML-formatted file or pipestat config with PostgreSQL database login credentials

Apart from that, there are many other *optional* configuration points that have defaults. Please refer to the [environment variables reference](http://pipestat.databio.org/en/dev/env_vars/) to learn about the the optional configuration options and their meaning.

## Configuration sources

Pipestat configuration can come from 3 sources, with the following priority:

1. `PipestatManager` constructor
2. Pipestat configuration file
3. Environment variables
