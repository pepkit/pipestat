"""Test fixtures and helpers to make widely available in the package"""

import os
import pytest
from pipestat.const import STATUS_SCHEMA
from pipestat.helpers import read_yaml_data


BACKEND_KEY_DB = "db"
BACKEND_KEY_FILE = "file"
STANDARD_TEST_PIPE_ID = "default_pipeline_name"

DB_URL = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"


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
def backend_data(request, config_file_path, results_file_path):
    if request.param == BACKEND_KEY_DB:
        return {"config_file": config_file_path}
    elif request.param == BACKEND_KEY_FILE:
        return {"results_file_path": results_file_path}
    raise Exception(f"Unrecognized initial parametrization for backend data: {request.param}")


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


@pytest.fixture
def schema_with_project_with_samples_without_status():
    return get_data_file_path(
        "sample_output_schema__with_project_with_samples_without_status.yaml"
    )


@pytest.fixture
def custom_status_schema():
    return get_data_file_path("custom_status_schema.yaml")


@pytest.fixture
def custom_status_schema2():
    return get_data_file_path("custom_status_schema_2.yaml")

@pytest.fixture
def output_schema_html_report():
    return get_data_file_path("output_schema_html_report.yaml")
