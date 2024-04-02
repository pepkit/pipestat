"""Test fixtures and helpers to make widely available in the package"""

import os
import pytest
import subprocess

from pipestat.const import STATUS_SCHEMA
from yacman import load_yaml
from atexit import register

REC_ID = "constant_record_id"
BACKEND_KEY_DB = "db"
BACKEND_KEY_FILE = "file"
DB_URL = "postgresql+psycopg://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
DB_CMD = """
docker run --rm -it --name pipestat_test_db \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=pipestat-password \
    -e POSTGRES_DB=pipestat-test \
    -p 5432:5432 \
    postgres
"""
STANDARD_TEST_PIPE_ID = "default_pipeline_name"

try:
    subprocess.check_output(
        "docker inspect pipestat_test_db --format '{{.State.Status}}'", shell=True
    )
    SERVICE_UNAVAILABLE = False
except:
    register(print, f"Some tests require a test database. To initiate it, run:\n{DB_CMD}")
    SERVICE_UNAVAILABLE = True

try:
    result = subprocess.check_output(
        "pipestat report --c 'tests/data/config.yaml' -i 'name_of_something' -v 'test_value' -r 'dependency_value'",
        shell=True,
    )
    DB_DEPENDENCIES = True
except:
    register(
        print,
        f"Warning: you must install dependencies with pip install pipestat['dbbackend'] to run database tests.",
    )
    DB_DEPENDENCIES = False


def get_data_file_path(filename: str) -> str:
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    return os.path.join(data_path, filename)


# Data corresponding to the non-default status schema info used in a few places in the test data files.
COMMON_CUSTOM_STATUS_DATA = load_yaml(filepath=get_data_file_path("custom_status_schema.yaml"))

# Data corresponding to default status schema, at pipestat/schema/status_schema.yaml
DEFAULT_STATUS_DATA = load_yaml(filepath=STATUS_SCHEMA)


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


@pytest.fixture
def output_schema_as_JSON_schema():
    return get_data_file_path("output_schema_as_JSON_schema.yaml")


@pytest.fixture
def output_schema_with_index():
    return get_data_file_path("sample_output_schema_with_index.yaml")


@pytest.fixture
def output_schema_no_refs():
    return get_data_file_path("output_schema.yaml")


@pytest.fixture
def val_dict():
    val_dict = {
        "sample1": {"name_of_something": "test_name"},
        "sample2": {"number_of_things": 2},
    }
    return val_dict


@pytest.fixture
def values_project():
    values_project = [
        {"project_name_1": {"number_of_things": 2}},
        {"project_name_1": {"name_of_something": "name of something string"}},
    ]
    return values_project


@pytest.fixture
def values_sample():
    values_sample = [
        {"sample1": {"smooth_bw": "smooth_bw string"}},
        {"sample2": {"output_file": {"path": "path_string", "title": "title_string"}}},
    ]
    return values_sample


@pytest.fixture
def values_complex_linking():
    # paths to images and files
    path_file_1 = get_data_file_path("test_file_links/results/project_dir_example_1/ex1.txt")
    path_file_2 = get_data_file_path("test_file_links/results/project_dir_example_1/ex2.txt")
    path_image_1 = get_data_file_path("test_file_links/results/project_dir_example_1/ex3.png")
    path_image_2 = get_data_file_path("test_file_links/results/project_dir_example_1/ex4.png")

    values_complex_linking = [
        {"sample1": {"output_file": {"path": path_file_1, "title": "title_string"}}},
        {"sample2": {"output_file": {"path": path_file_2, "title": "title_string"}}},
        {
            "sample1": {
                "output_image": {
                    "path": path_image_1,
                    "thumbnail_path": "path_string",
                    "title": "title_string",
                }
            }
        },
        {
            "sample2": {
                "output_image": {
                    "path": path_image_2,
                    "thumbnail_path": "path_string",
                    "title": "title_string",
                }
            }
        },
        {
            "sample2": {
                "nested_object": {
                    "example_property_1": {
                        "path": path_file_1,
                        "thumbnail_path": "path_string",
                        "title": "title_string",
                    },
                    "example_property_2": {
                        "path": path_image_1,
                        "thumbnail_path": "path_string",
                        "title": "title_string",
                    },
                }
            }
        },
        {
            "sample2": {
                "output_file_nested_object": {
                    "example_property_1": {
                        "third_level_property_1": {
                            "path": path_file_1,
                            "thumbnail_path": "path_string",
                            "title": "title_string",
                        }
                    },
                    "example_property_2": {
                        "third_level_property_1": {
                            "path": path_file_1,
                            "thumbnail_path": "path_string",
                            "title": "title_string",
                        }
                    },
                }
            }
        },
    ]
    return values_complex_linking


@pytest.fixture
def range_values():
    range_values = []
    for i in range(12):
        r_id = "sample" + str(i)
        val = {
            "md5sum": "hash" + str(i),
            "number_of_things": i * 10,
            "percentage_of_things": i % 2,
            "switch_value": bool(i % 2),
            "output_image": {
                "path": "path_to_" + str(i),
                "thumbnail_path": "thumbnail_path" + str(i),
                "title": "title_string" + str(i),
            },
            "output_file_in_object_nested": {
                "prop1": {
                    "prop2": i,
                },
            },
        }
        range_values.append((r_id, val))
    return range_values
