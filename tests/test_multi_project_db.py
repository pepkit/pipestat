"""Tests for multi-project DB scenarios.

These tests verify that project_name correctly namespaces records in the
database backend, preventing collisions when multiple projects share the
same pipeline and database.

All tests require a running PostgreSQL database. They are skipped if the
DB dependencies are not installed or the test database is not available.
"""

import os

import pytest

from pipestat import PipestatManager
from pipestat.const import *

from .conftest import DB_DEPENDENCIES, DB_URL, SERVICE_UNAVAILABLE

if DB_DEPENDENCIES:
    try:
        from sqlmodel import SQLModel, create_engine
        from sqlmodel.main import default_registry

        from pipestat.backends.db_backend.db_parsed_schema import clear_model_cache
    except ImportError:
        pass


SCHEMA_CONTENT = """\
pipeline_name: test_multi_project
samples:
  number_of_things:
    type: integer
    description: "Number of things"
  name_of_something:
    type: string
    description: "Name of something"
project:
  number_of_things:
    type: integer
    description: "Number of things"
"""


class ContextManagerDBTesting:
    """Context manager to connect to DB and drop all tables on exit."""

    def __init__(self, db_url):
        self.db_url = db_url

    def __enter__(self):
        self.engine = create_engine(self.db_url, echo=True)
        self.connection = self.engine.connect()
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        SQLModel.metadata.drop_all(self.engine)
        default_registry.dispose()
        clear_model_cache()
        self.connection.close()


def _make_config(tmp_path, project_name, schema_path):
    """Create a pipestat config file for the given project."""
    port = os.environ.get("PIPESTAT_TEST_DB_PORT", "5432")
    config_content = f"""\
project_name: {project_name}
schema_path: {schema_path}
database:
  dialect: postgresql
  driver: psycopg
  name: pipestat-test
  user: pipestatuser
  password: shgfty^8922138$^!
  host: 127.0.0.1
  port: {port}
"""
    config_file = tmp_path / f"config_{project_name}.yaml"
    config_file.write_text(config_content)
    return str(config_file)


@pytest.fixture
def schema_path(tmp_path):
    schema_file = tmp_path / "multi_project_schema.yaml"
    schema_file.write_text(SCHEMA_CONTENT)
    return str(schema_file)


@pytest.fixture
def two_managers(tmp_path, schema_path):
    """Create two PipestatManagers for different projects sharing the same DB and pipeline."""
    config_a = _make_config(tmp_path, "cohort_A", schema_path)
    config_b = _make_config(tmp_path, "cohort_B", schema_path)

    with ContextManagerDBTesting(DB_URL):
        psm_a = PipestatManager(
            config_file=config_a,
            pipeline_type="sample",
        )
        psm_b = PipestatManager(
            config_file=config_b,
            pipeline_type="sample",
        )
        yield psm_a, psm_b


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires DB dependencies")
@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Requires running PostgreSQL")
class TestMultiProjectDB:
    def test_same_record_id_different_projects_no_collision(self, two_managers):
        """Records with same ID in different projects do not overwrite each other."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 10})
        psm_b.report(record_identifier="s1", values={"number_of_things": 20})

        result_a = psm_a.retrieve_one(record_identifier="s1")
        result_b = psm_b.retrieve_one(record_identifier="s1")

        assert result_a["number_of_things"] == 10
        assert result_b["number_of_things"] == 20

    def test_select_records_scoped_by_project(self, two_managers):
        """select_records returns only records for the current project."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 1})
        psm_a.report(record_identifier="s2", values={"number_of_things": 2})
        psm_b.report(record_identifier="s1", values={"number_of_things": 3})
        psm_b.report(record_identifier="s3", values={"number_of_things": 4})

        records_a = psm_a.select_records()["records"]
        records_b = psm_b.select_records()["records"]

        ids_a = sorted([r["record_identifier"] for r in records_a])
        ids_b = sorted([r["record_identifier"] for r in records_b])

        assert ids_a == ["s1", "s2"]
        assert ids_b == ["s1", "s3"]

    def test_count_records_scoped_by_project(self, two_managers):
        """count_records counts only records for the current project."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 1})
        psm_a.report(record_identifier="s2", values={"number_of_things": 2})
        psm_b.report(record_identifier="s1", values={"number_of_things": 3})
        psm_b.report(record_identifier="s3", values={"number_of_things": 4})

        assert psm_a.count_records() == 2
        assert psm_b.count_records() == 2

    def test_remove_scoped_by_project(self, two_managers):
        """Removing a record in one project does not affect the other."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 10})
        psm_b.report(record_identifier="s1", values={"number_of_things": 20})

        psm_a.remove(record_identifier="s1")

        assert psm_a.count_records() == 0
        assert psm_b.count_records() == 1

    def test_status_scoped_by_project(self, two_managers):
        """Status is scoped by project."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 1})
        psm_b.report(record_identifier="s1", values={"number_of_things": 2})

        psm_a.set_status(status_identifier="running", record_identifier="s1")
        psm_b.set_status(status_identifier="completed", record_identifier="s1")

        assert psm_a.get_status("s1") == "running"
        assert psm_b.get_status("s1") == "completed"

    def test_history_scoped_by_project(self, two_managers):
        """History records are scoped by project."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 1})
        psm_b.report(record_identifier="s1", values={"number_of_things": 2})

        # Update s1 in project A only
        psm_a.report(record_identifier="s1", values={"number_of_things": 100})

        history_a = psm_a.retrieve_history(record_identifier="s1")
        history_b = psm_b.retrieve_history(record_identifier="s1")

        # psm_a should have history (original value before update)
        assert history_a
        # psm_b should have no history (no updates)
        assert not history_b

    def test_list_projects(self, two_managers):
        """list_projects returns all project names in the database."""
        psm_a, psm_b = two_managers

        psm_a.report(record_identifier="s1", values={"number_of_things": 1})
        psm_b.report(record_identifier="s1", values={"number_of_things": 2})

        projects = psm_a.list_projects()
        assert projects == ["cohort_A", "cohort_B"]

    def test_project_level_results_scoped(self, tmp_path, schema_path):
        """Project-level results are scoped by project_name."""
        config_a = _make_config(tmp_path, "cohort_A", schema_path)
        config_b = _make_config(tmp_path, "cohort_B", schema_path)

        with ContextManagerDBTesting(DB_URL):
            psm_a = PipestatManager(
                config_file=config_a,
                pipeline_type="project",
            )
            psm_b = PipestatManager(
                config_file=config_b,
                pipeline_type="project",
            )

            psm_a.report(
                record_identifier="cohort_A",
                values={"number_of_things": 100},
            )
            psm_b.report(
                record_identifier="cohort_B",
                values={"number_of_things": 200},
            )

            result_a = psm_a.retrieve_one(record_identifier="cohort_A")
            result_b = psm_b.retrieve_one(record_identifier="cohort_B")

            assert result_a["number_of_things"] == 100
            assert result_b["number_of_things"] == 200


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires DB dependencies")
class TestProjectNameRequired:
    def test_missing_project_name_raises_for_db(self, tmp_path, schema_path):
        """Creating a DB-backed manager without project_name raises ValueError."""
        port = os.environ.get("PIPESTAT_TEST_DB_PORT", "5432")
        config_content = f"""\
schema_path: {schema_path}
database:
  dialect: postgresql
  driver: psycopg
  name: pipestat-test
  user: pipestatuser
  password: shgfty^8922138$^!
  host: 127.0.0.1
  port: {port}
"""
        config_file = tmp_path / "config_no_project.yaml"
        config_file.write_text(config_content)

        with pytest.raises(ValueError, match="project_name is required"):
            PipestatManager(
                config_file=str(config_file),
                pipeline_type="sample",
            )
