"""Tests of the parsed schema class"""

from pathlib import Path
from typing import *
import pytest
import yaml
from pipestat.exceptions import SchemaError
from pipestat.helpers import ParsedSchema


TEMP_SCHEMA_FILENAME = "schema.tmp.yaml"


def write_mapping(file_data_pair: Tuple[Mapping, Path]):
    data, path = file_data_pair
    with open(path, "w") as fH:
        yaml.dump(data, fH)
    return path


@pytest.fixture(
    scope="function",
    params=[lambda _: {}, lambda p: write_mapping(({}, p / TEMP_SCHEMA_FILENAME))],
)
def empty_schema_source(request, tmp_path):
    return request.param(tmp_path)


def test_empty__fails_with_missing_pipeline_id(empty_schema_source):
    with pytest.raises(SchemaError):
        ParsedSchema(empty_schema_source)


@pytest.mark.skip("not implemented")
def test_only_status():
    pass


@pytest.mark.skip("not implemented")
def test_only_project_level(config_format):
    pass
