"""Tests for auto-default record_identifier at project level."""

from tempfile import NamedTemporaryFile

import pytest

from pipestat import PipestatManager
from pipestat.pipestat import ProjectPipestatManager, SamplePipestatManager

from .conftest import get_data_file_path


@pytest.fixture
def schema_with_project():
    return get_data_file_path("sample_output_schema__with_project_with_samples_without_status.yaml")


@pytest.fixture
def schema_file_path():
    return get_data_file_path("sample_output_schema.yaml")


class TestProjectRecordIdentifierDefaults:
    """Test that record_identifier auto-defaults to project_name at project level."""

    def test_project_level_with_project_name(self, schema_with_project):
        """Project-level with project_name: record_identifier == project_name."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                project_name="rrbs",
                pipeline_type="project",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            assert psm.record_identifier == "rrbs"

    def test_project_level_without_project_name(self, schema_with_project):
        """Project-level without project_name: record_identifier == 'project', warning emitted."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                pipeline_type="project",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            assert psm.record_identifier == "project"
            assert psm.project_name == "project"

    def test_report_at_project_level_without_record_identifier(self, schema_with_project):
        """report() at project level without record_identifier succeeds."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                project_name="rrbs",
                pipeline_type="project",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            result = psm.report(values={"number_of_things": 42})
            assert result is not False

    def test_retrieve_one_at_project_level(self, schema_with_project):
        """retrieve_one() at project level works without explicit record_identifier."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                project_name="rrbs",
                pipeline_type="project",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            psm.report(values={"number_of_things": 42})
            result = psm.retrieve_one(result_identifier="number_of_things")
            assert result == 42

    def test_remove_at_project_level(self, schema_with_project):
        """remove() at project level works without explicit record_identifier."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                project_name="rrbs",
                pipeline_type="project",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            psm.report(values={"number_of_things": 42})
            removed = psm.remove(result_identifier="number_of_things")
            assert removed is True

    def test_explicit_record_identifier_honored(self, schema_with_project):
        """Explicit record_identifier overrides project_name default."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                project_name="rrbs",
                pipeline_type="project",
                record_identifier="custom",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            assert psm.record_identifier == "custom"

    def test_project_pipestat_manager_auto_default(self, schema_with_project):
        """ProjectPipestatManager auto-defaults record_identifier to project_name."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = ProjectPipestatManager(
                project_name="rrbs",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            assert psm.record_identifier == "rrbs"

    def test_report_with_level_project_on_sample_manager(self, schema_with_project):
        """report() with level='project' on sample-level manager succeeds without explicit record_identifier."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                project_name="rrbs",
                pipeline_type="sample",
                schema_path=schema_with_project,
                results_file_path=f.name,
            )
            result = psm.report(
                values={"number_of_things": 42},
                level="project",
            )
            assert result is not False

    def test_sample_level_unchanged(self, schema_file_path):
        """Sample-level manager without record_identifier still raises on report."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                pipeline_type="sample",
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            with pytest.raises(NotImplementedError):
                psm.report(values={"name_of_something": "test"})
