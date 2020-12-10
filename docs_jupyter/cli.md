# Pipestat CLI

Before following this tutorial please make sure you're familiar with "Pipestat Python API" tutorial.

## Usage reference

To learn about the usage `pipestat` usage use `--help`/`-h` option on any level:


```bash
pipestat -h
```

    version: 0.0.1
    usage: pipestat [-h] [--version] [--silent] [--verbosity V] [--logdev]
                    {report,inspect,remove,retrieve} ...
    
    pipestat - report pipeline results
    
    positional arguments:
      {report,inspect,remove,retrieve}
        report              Report a result.
        inspect             Inspect a database.
        remove              Remove a result.
        retrieve            Retrieve a result.
    
    optional arguments:
      -h, --help            show this help message and exit
      --version             show program's version number and exit
      --silent              Silence logging. Overrides verbosity.
      --verbosity V         Set logging level (1-5 or logging module level name)
      --logdev              Expand content of logging message format.
    
    pipestat standardizes reporting of pipeline results. It formalizes a way for
    pipeline developers and downstream tools developers to communicate -- results
    produced by a pipeline can easily and reliably become an input for downstream
    analyses. The ovject exposes API for interacting with the results can be
    backed by either a YAML-formatted file or a PostgreSQL database.



```bash
pipestat report -h
```

    usage: pipestat report [-h] -n N (-f F | -c C | -a) -s S -i I -r R -v V [-o]
                           [-t]
    
    Report a result.
    
    optional arguments:
      -h, --help            show this help message and exit
      -n N, --namespace N   Name of the pipeline to report result for
      -f F, --results-file F
                            Path to the YAML file where the results will be
                            stored. This file will be used as pipestat backend and
                            to restore the reported results across sesssions
      -c C, --database-config C
                            Path to the YAML file with PostgreSQL database
                            configuration. Please refer to the documentation for
                            the file format requirements.
      -a, --database-only   Whether the reported data should not be stored in the
                            memory, only in the database.
      -s S, --schema S      Path to the schema that defines the results that can
                            be eported
      -i I, --result-identifier I
                            ID of the result to report; needs to be defined in the
                            schema
      -r R, --record-identifier R
                            ID of the record to report the result for
      -v V, --value V       Value of the result to report
      -o, --overwrite       Whether the result should override existing ones in
                            case of name clashes
      -t, --try-convert     Whether to try to convert the reported value into
                            reqiuired class in case it does not meet the schema
                            requirements



```bash
pipestat retrieve -h
```

    usage: pipestat retrieve [-h] -n N (-f F | -c C | -a) [-s S] -i I -r R
    
    Retrieve a result.
    
    optional arguments:
      -h, --help            show this help message and exit
      -n N, --namespace N   Name of the pipeline to report result for
      -f F, --results-file F
                            Path to the YAML file where the results will be
                            stored. This file will be used as pipestat backend and
                            to restore the reported results across sesssions
      -c C, --database-config C
                            Path to the YAML file with PostgreSQL database
                            configuration. Please refer to the documentation for
                            the file format requirements.
      -a, --database-only   Whether the reported data should not be stored in the
                            memory, only in the database.
      -s S, --schema S      Path to the schema that defines the results that can
                            be eported
      -i I, --result-identifier I
                            ID of the result to report; needs to be defined in the
                            schema
      -r R, --record-identifier R
                            ID of the record to report the result for



```bash
pipestat remove -h
```

    usage: pipestat remove [-h] -n N (-f F | -c C | -a) [-s S] -i I -r R
    
    Remove a result.
    
    optional arguments:
      -h, --help            show this help message and exit
      -n N, --namespace N   Name of the pipeline to report result for
      -f F, --results-file F
                            Path to the YAML file where the results will be
                            stored. This file will be used as pipestat backend and
                            to restore the reported results across sesssions
      -c C, --database-config C
                            Path to the YAML file with PostgreSQL database
                            configuration. Please refer to the documentation for
                            the file format requirements.
      -a, --database-only   Whether the reported data should not be stored in the
                            memory, only in the database.
      -s S, --schema S      Path to the schema that defines the results that can
                            be eported
      -i I, --result-identifier I
                            ID of the result to report; needs to be defined in the
                            schema
      -r R, --record-identifier R
                            ID of the record to report the result for



```bash
pipestat inspect -h
```

    usage: pipestat inspect [-h] -n N (-f F | -c C | -a) [-s S] [-d]
    
    Inspect a database.
    
    optional arguments:
      -h, --help            show this help message and exit
      -n N, --namespace N   Name of the pipeline to report result for
      -f F, --results-file F
                            Path to the YAML file where the results will be
                            stored. This file will be used as pipestat backend and
                            to restore the reported results across sesssions
      -c C, --database-config C
                            Path to the YAML file with PostgreSQL database
                            configuration. Please refer to the documentation for
                            the file format requirements.
      -a, --database-only   Whether the reported data should not be stored in the
                            memory, only in the database.
      -s S, --schema S      Path to the schema that defines the results that can
                            be eported
      -d, --data            Whether to display the data


## Usage demonstration

### Reporting

Naturally, the command line interface provides access to all the Python API functionalities of `pipestat`. So, for example, to report a result and back the object by a file use:


```bash
temp_file=`mktemp`
pipestat report -f $temp_file -n test -r sample1 -i number_of_things -v 100 -s ../tests/data/sample_output_schema.yaml --try-convert
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    Reported records for 'sample1' in 'test' namespace:
     - number_of_things: 100


The result has been reported and the database file has been updated:


```bash
cat $temp_file
```

    test:
      sample1:
        number_of_things: 100


Let's report another result:


```bash
pipestat report -f $temp_file -n test -r sample1 -i percentage_of_things -v 1.1 -s ../tests/data/sample_output_schema.yaml --try-convert
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    Reported records for 'sample1' in 'test' namespace:
     - percentage_of_things: 1.1



```bash
cat $temp_file
```

    test:
      sample1:
        number_of_things: 100
        percentage_of_things: 1.1


### Inspection

`pipestat inspect` command is a way to briefly look at the general `PipestatManager` state, like number of records, type of backend etc.


```bash
pipestat inspect -f $temp_file -n test
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    
    
    PipestatManager (test)
    Backend: file (/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d)
    Records count: 1


In order to display the contents of the results file or database table associated with the indicated namespace, add `--data` flag:


```bash
pipestat inspect --data -f $temp_file -n test
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    
    
    PipestatManager (test)
    Backend: file (/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d)
    Records count: 1
    
    Data:
    test:
      sample1:
        number_of_things: 100
        percentage_of_things: 1.1


### Retrieval

Naturally, the reported results can be retrieved. Just call `pipestat retrieve` to do so:


```bash
pipestat retrieve -f $temp_file -n test -r sample1 -i percentage_of_things
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    1.1


### Removal

In order to remove a result call `pipestat remove`:


```bash
pipestat remove -f $temp_file -n test -r sample1 -i percentage_of_things
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    Removed result 'percentage_of_things' for record 'sample1' from 'test' namespace


The results file and the state of the `PipestatManager` object reflect the removal:


```bash
cat $temp_file
```

    test:
      sample1:
        number_of_things: 100



```bash
pipestat inspect --data -f $temp_file -n test
```

    Reading data from '/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d'
    
    
    PipestatManager (test)
    Backend: file (/var/folders/3f/0wj7rs2144l9zsgxd3jn5nxc0000gn/T/tmp.G6Gtt93d)
    Records count: 1
    
    Data:
    test:
      sample1:
        number_of_things: 100

