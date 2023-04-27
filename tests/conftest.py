import os

import pytest


DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture
def results_file_path():
    return os.path.join(DATA_PATH, "results_file.yaml")


@pytest.fixture
def schema_file_path():
    return os.path.join(DATA_PATH, "sample_output_schema.yaml")


@pytest.fixture
def highlight_schema_file_path():
    return os.path.join(DATA_PATH, "sample_output_schema_highlight.yaml")


@pytest.fixture
def recursive_schema_file_path():
    return os.path.join(DATA_PATH, "sample_output_schema_recursive.yaml")


@pytest.fixture
def config_file_path():
    return os.path.join(DATA_PATH, "config.yaml")


@pytest.fixture
def config_no_schema_file_path():
    return os.path.join(DATA_PATH, "config_no_schema.yaml")
