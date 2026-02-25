"""Test fixtures and helpers to make widely available in the package"""

import os
import subprocess
from atexit import register

import pytest
from yacman import load_yaml


def pytest_addoption(parser):
    parser.addoption("--pephub", action="store_true", default=False, help="run PEPhub tests")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--pephub"):
        skip_pephub = pytest.mark.skip(reason="needs --pephub flag to run")
        for item in items:
            if "pephub" in item.keywords:
                item.add_marker(skip_pephub)

from pipestat.const import STATUS_SCHEMA

REC_ID = "constant_record_id"
BACKEND_KEY_DB = "db"
BACKEND_KEY_FILE = "file"
DB_PORT = os.environ.get("PIPESTAT_TEST_DB_PORT", "5432")
DB_URL = f"postgresql+psycopg://pipestatuser:shgfty^8922138$^!@127.0.0.1:{DB_PORT}/pipestat-test"
STANDARD_TEST_PIPE_ID = "default_pipeline_name"

PEPHUB_URL = "databio/pipestat_demo:default"


def _detect_postgres_container():
    """Check if a pipestat test PostgreSQL container is running."""
    container = os.environ.get("PIPESTAT_TEST_CONTAINER")
    if container:
        # Explicit container name from test-integration.sh
        try:
            subprocess.check_output(
                f"docker inspect {container} --format '{{{{.State.Status}}}}'",
                shell=True,
                stderr=subprocess.DEVNULL,
            )
            return True
        except subprocess.CalledProcessError:
            return False
    # Fallback: check for any pipestat test container
    for name in ["pipestat_test_db", "pipestat-db-test"]:
        try:
            result = (
                subprocess.check_output(
                    f"docker ps --filter 'name={name}' --format '{{{{.Names}}}}'",
                    shell=True,
                    stderr=subprocess.DEVNULL,
                )
                .decode()
                .strip()
            )
            if result:
                return True
        except subprocess.CalledProcessError:
            pass
    return False


SERVICE_UNAVAILABLE = not _detect_postgres_container()
if SERVICE_UNAVAILABLE:
    register(
        print,
        "Some tests require a test database. Run: ./tests/scripts/test-integration.sh\n"
        "Or start manually: ./tests/scripts/services.sh start",
    )

try:
    from pipestat.backends.db_backend.dbbackend import DBBackend  # noqa: F401

    DB_DEPENDENCIES = True
except ImportError:
    register(
        print,
        "Warning: install DB dependencies with: pip install 'pipestat[dbbackend]'",
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
def schema_file_path_sqlite():
    return get_data_file_path("sample_output_schema_sqlite.yaml")


@pytest.fixture
def highlight_schema_file_path():
    return get_data_file_path("sample_output_schema_highlight.yaml")


@pytest.fixture
def recursive_schema_file_path():
    return get_data_file_path("sample_output_schema_recursive.yaml")


@pytest.fixture
def config_file_path(tmp_path):
    """Generate config with dynamic DB port from PIPESTAT_TEST_DB_PORT env var."""
    schema_path = get_data_file_path("sample_output_schema_recursive.yaml")
    config_content = f"""\
project_name: test
record_identifier: sample1
schema_path: {schema_path}
database:
  dialect: postgresql
  driver: psycopg
  name: pipestat-test
  user: pipestatuser
  password: shgfty^8922138$^!
  host: 127.0.0.1
  port: {DB_PORT}
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def config_file_path_pephub():
    return get_data_file_path("config_pephub_url.yaml")


@pytest.fixture
def config_no_schema_file_path(tmp_path):
    """Generate no-schema config with dynamic DB port."""
    config_content = f"""\
project_name: test
sample_name: sample1
database:
  dialect: postgresql
  driver: psycopg
  name: pipestat-test
  user: pipestatuser
  password: shgfty^8922138$^!
  host: 127.0.0.1
  port: {DB_PORT}
"""
    config_file = tmp_path / "config_no_schema.yaml"
    config_file.write_text(config_content)
    return str(config_file)


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


@pytest.fixture
def schema_with_shared_keys(tmp_path):
    """Schema file with Time/Success defined at both sample and project levels."""
    schema_content = """
pipeline_name: test_pipeline
samples:
  Time:
    type: string
    description: "Sample runtime"
  Success:
    type: string
    description: "Sample completion"
  sample_only_result:
    type: integer
    description: "Sample-only result"
project:
  Time:
    type: string
    description: "Project runtime"
  Success:
    type: string
    description: "Project completion"
  project_only_result:
    type: number
    description: "Project-only result"
"""
    schema_file = tmp_path / "schema_shared_keys.yaml"
    schema_file.write_text(schema_content)
    return str(schema_file)
