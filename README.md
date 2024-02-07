![Run pytests](https://github.com/pepkit/pipestat/workflows/Run%20pytests/badge.svg)
[![docs-badge](https://readthedocs.org/projects/pipestat/badge/?version=latest)](https://pipestat.databio.org/en/latest/)
[![pypi-badge](https://img.shields.io/pypi/v/pipestat)](https://pypi.org/project/pipestat)
[![codecov](https://codecov.io/gh/pepkit/pipestat/branch/master/graph/badge.svg?token=O07MXSQZ32)](https://codecov.io/gh/pepkit/pipestat)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)


<img src="https://raw.githubusercontent.com/pepkit/pipestat/master/docs/img/pipestat_logo.svg?sanitize=true" alt="pipestat" height="70"/><br>

Pipestat standardizes reporting of pipeline results. It provides 1) a standard specification for how pipeline outputs should be stored; and 2) an implementation to easily write results to that format from within Python or from the command line. A pipeline author defines all the outputs produced by a pipeline by writing a JSON-schema. The pipeline then uses pipestat to report pipeline outputs as the pipeline runs, either via the Python API or command line interface. The user configures results to be stored either in a [YAML-formatted file](https://yaml.org/spec/1.2/spec.html) or a [PostgreSQL database](https://www.postgresql.org/).

See [Pipestat documentation](https://pep.databio.org/pipestat/) for complete details.


## Developer tests

###  Optional Dependencies

Note: to run the pytest suite locally, you will need to install the related requirements:

```bash
cd pipestat

pip install -r requirements/requirements-test.txt

```

### Database Backend Configuration for Tests

Many of the tests require a postgres database to be set up otherwise many of the tests will skip.

We recommend using docker:
```bash
docker run --rm -it --name pipestat_test_db \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=pipestat-password \
    -e POSTGRES_DB=pipestat-test \
    -p 5432:5432 \
    postgres
```

