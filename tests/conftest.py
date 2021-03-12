import os

import pytest


@pytest.fixture
def data_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture
def results_file_path(data_path):
    return os.path.join(data_path, "results_file.yaml")


@pytest.fixture
def schema_file_path(data_path):
    return os.path.join(data_path, "sample_output_schema.yaml")


@pytest.fixture
def highlight_schema_file_path(data_path):
    return os.path.join(data_path, "sample_output_schema_highlight.yaml")


@pytest.fixture
def custom_status_schema(data_path):
    return os.path.join(data_path, "custom_status_schema.yaml")


@pytest.fixture
def recursive_schema_file_path(data_path):
    return os.path.join(data_path, "sample_output_schema_recursive.yaml")


@pytest.fixture
def config_file_path(data_path):
    return os.path.join(data_path, "config.yaml")


@pytest.fixture
def config_no_schema_file_path(data_path):
    return os.path.join(data_path, "config_no_schema.yaml")
