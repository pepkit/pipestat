import os
import pytest


@pytest.fixture
def data_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture
def results_file_path(data_path):
    return os.path.join(data_path, "db.yaml")


@pytest.fixture
def schema_file_path(data_path):
    return os.path.join(data_path, "sample_output_schema.yaml")


@pytest.fixture
def config_file_path(data_path):
    return os.path.join(data_path, "config.yaml")
