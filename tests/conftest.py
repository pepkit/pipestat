import os
import pytest


@pytest.fixture
def data_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture
def db_file_path(data_path):
    return os.path.join(data_path, "db.yaml")
