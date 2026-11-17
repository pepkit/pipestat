"""Test fixtures and helpers to make widely available in the package.

Test layout:
- tests/            unit and file-backend tests; run with plain `pytest tests`.
- tests/integration integration tests that need a real PostgreSQL. They run only when
                    RUN_INTEGRATION_TESTS=true (tests/scripts/test-integration.sh sets it).

Tests that should run against both backends are written once, in tests/, and marked
`@pytest.mark.backends("file", "db")`. In tests/ they get `backend="file"`; the integration
package re-collects the same test classes, where they get `backend="db"`.
"""

import importlib.util
import os
from contextlib import nullcontext
from pathlib import Path

import pytest
from yacman import load_yaml

from pipestat.const import STATUS_SCHEMA

TESTS_DIR = Path(__file__).parent
INTEGRATION_DIR = TESTS_DIR / "integration"
RUN_INTEGRATION_TESTS = os.getenv("RUN_INTEGRATION_TESTS") == "true"

REC_ID = "constant_record_id"
BACKEND_KEY_DB = "db"
BACKEND_KEY_FILE = "file"
DB_HOST = os.environ.get("PIPESTAT_TEST_DB_HOST", "127.0.0.1")
DB_PORT = os.environ.get("PIPESTAT_TEST_DB_PORT", "5432")
DB_URL = f"postgresql+psycopg://pipestatuser:shgfty^8922138$^!@{DB_HOST}:{DB_PORT}/pipestat-test"
STANDARD_TEST_PIPE_ID = "default_pipeline_name"

# For unit tests that need the DB packages installed but no running database.
requires_db_deps = pytest.mark.skipif(
    importlib.util.find_spec("sqlmodel") is None,
    reason="needs DB dependencies: uv pip install -e '.[dbbackend]'",
)


def pytest_addoption(parser):
    parser.addoption(
        "--pephub-live",
        action="store_true",
        default=False,
        help="run PEPhub tests against the live pephub.databio.org service (needs `phc login`)",
    )


def _in_integration_dir(path) -> bool:
    return INTEGRATION_DIR in Path(path).parents


def _backend_for(path) -> str:
    """The backend that shared tests use, given the directory they are collected from."""
    return BACKEND_KEY_DB if _in_integration_dir(path) else BACKEND_KEY_FILE


def pytest_generate_tests(metafunc):
    """Parametrize `backend` for tests marked with @pytest.mark.backends(...)."""
    marker = metafunc.definition.get_closest_marker("backends")
    if marker is not None and "backend" in metafunc.fixturenames:
        metafunc.parametrize("backend", [_backend_for(metafunc.definition.path)])


def _is_reused_test(item) -> bool:
    """True if the test was defined outside the module that collected it (a re-collected class)."""
    func = getattr(item, "function", None)
    return func is not None and func.__module__ != item.module.__name__


def pytest_collection_modifyitems(config, items):
    """Route shared tests to the right directory and gate integration tests.

    - A test marked `backends(...)` runs only where its backend is available: as "file" in
      tests/, as "db" in tests/integration/. Other combinations are deselected.
    - Re-collected (imported) tests without a db variant are deselected in tests/integration/,
      so file-only tests are not run twice.
    - Tests in tests/integration/ are skipped unless RUN_INTEGRATION_TESTS=true.
    """
    skip_integration = pytest.mark.skip(
        reason="integration test: run tests/scripts/test-integration.sh "
        "(or set RUN_INTEGRATION_TESTS=true with a test database running)"
    )
    kept, deselected = [], []
    for item in items:
        in_integration = _in_integration_dir(item.path)
        marker = item.get_closest_marker("backends")
        if marker is not None:
            wanted = _backend_for(item.path) in marker.args
        else:
            wanted = not (in_integration and _is_reused_test(item))
        if not wanted:
            deselected.append(item)
            continue
        if in_integration and not RUN_INTEGRATION_TESTS:
            item.add_marker(skip_integration)
        kept.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept


class ContextManagerDBTesting:
    """
    Connect to the database at db_url and drop everything from it on exit, so each test
    starts with an empty database.
    """

    def __init__(self, db_url):
        self.db_url = db_url

    def __enter__(self):
        from sqlmodel import create_engine

        self.engine = create_engine(self.db_url, echo=True)
        self.connection = self.engine.connect()
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        from sqlmodel import SQLModel
        from sqlmodel.main import default_registry

        from pipestat.backends.db_backend.db_parsed_schema import clear_model_cache

        SQLModel.metadata.drop_all(self.engine)
        default_registry.dispose()
        clear_model_cache()  # Clear cached models when registry is disposed
        self.connection.close()


def db_context(backend):
    """Clean-database context for the db backend; a no-op for the file backend."""
    return ContextManagerDBTesting(DB_URL) if backend == BACKEND_KEY_DB else nullcontext()


def get_data_file_path(filename: str) -> str:
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    return os.path.join(data_path, filename)


# Data corresponding to the non-default status schema info used in a few places in the test data files.
COMMON_CUSTOM_STATUS_DATA = load_yaml(filepath=get_data_file_path("custom_status_schema.yaml"))

# Data corresponding to default status schema, at pipestat/schema/status_schema.yaml
DEFAULT_STATUS_DATA = load_yaml(filepath=STATUS_SCHEMA)


@pytest.fixture
def backend_data(backend, config_file_path, results_file_path):
    if backend == BACKEND_KEY_DB:
        return {"config_file": config_file_path}
    elif backend == BACKEND_KEY_FILE:
        return {"results_file_path": results_file_path}
    raise Exception(f"Unrecognized backend: {backend}")


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
  host: {DB_HOST}
  port: {DB_PORT}
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)
    return str(config_file)


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
  host: {DB_HOST}
  port: {DB_PORT}
"""
    config_file = tmp_path / "config_no_schema.yaml"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def schema_with_project_with_samples_without_status():
    return get_data_file_path("sample_output_schema__with_project_with_samples_without_status.yaml")


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
