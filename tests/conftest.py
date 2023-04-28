"""Test fixtures and helpers to make widely available in the package"""

import os
import pytest
from pipestat.const import STATUS_SCHEMA
from pipestat.helpers import read_yaml_data


def get_data_file_path(filename: str) -> str:
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    return os.path.join(data_path, filename)


# Data corresponding to the non-default status schema info used in a few places in the test data files.
_, COMMON_CUSTOM_STATUS_DATA = read_yaml_data(
    path=get_data_file_path("custom_status_schema.yaml"), what="custom test schema data"
)

# Data corresponding to default status schema, at pipestat/schema/status_schema.yaml
_, DEFAULT_STATUS_DATA = read_yaml_data(path=STATUS_SCHEMA, what="default status schema")


@pytest.fixture
def results_file_path():
    return get_data_file_path("results_file.yaml")


@pytest.fixture
def schema_file_path():
    return get_data_file_path("sample_output_schema.yaml")


@pytest.fixture
def highlight_schema_file_path():
    return get_data_file_path("sample_output_schema_highlight.yaml")


@pytest.fixture
def recursive_schema_file_path():
    return get_data_file_path("sample_output_schema_recursive.yaml")


@pytest.fixture
def config_file_path():
    return get_data_file_path("config.yaml")


@pytest.fixture
def config_no_schema_file_path():
    return get_data_file_path("config_no_schema.yaml")
