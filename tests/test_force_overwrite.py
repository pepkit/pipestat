"""Tests for manager-level force_overwrite default behavior."""

from tempfile import NamedTemporaryFile

import pytest

from pipestat import PipestatManager

from .conftest import get_data_file_path


@pytest.fixture
def schema_file_path():
    return get_data_file_path("sample_output_schema.yaml")


class TestForceOverwriteManagerDefault:
    """Test that force_overwrite can be configured at the manager level."""

    def test_manager_default_true_allows_overwrite(self, schema_file_path):
        """Default force_overwrite=True: report overwrites existing results."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            psm.force_overwrite = True
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "first_value"},
            )
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "second_value"},
            )
            result = psm.retrieve_one(record_identifier="sample1")
            assert result["name_of_something"] == "second_value"

    def test_manager_default_false_blocks_overwrite(self, schema_file_path):
        """Manager with force_overwrite=False: report does not overwrite."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            psm.force_overwrite = False
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "first_value"},
            )
            result = psm.report(
                record_identifier="sample1",
                values={"name_of_something": "second_value"},
            )
            # When overwrite is blocked, report returns False
            assert result is False
            retrieved = psm.retrieve_one(record_identifier="sample1")
            assert retrieved["name_of_something"] == "first_value"

    def test_per_call_true_overrides_manager_false(self, schema_file_path):
        """Per-call force_overwrite=True overrides manager default of False."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            psm.force_overwrite = False
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "first_value"},
            )
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "second_value"},
                force_overwrite=True,
            )
            result = psm.retrieve_one(record_identifier="sample1")
            assert result["name_of_something"] == "second_value"

    def test_per_call_false_overrides_manager_true(self, schema_file_path):
        """Per-call force_overwrite=False overrides manager default of True."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            psm.force_overwrite = True
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "first_value"},
            )
            result = psm.report(
                record_identifier="sample1",
                values={"name_of_something": "second_value"},
                force_overwrite=False,
            )
            assert result is False
            retrieved = psm.retrieve_one(record_identifier="sample1")
            assert retrieved["name_of_something"] == "first_value"

    def test_default_manager_has_force_overwrite_true(self, schema_file_path):
        """When not specified, manager defaults to force_overwrite=True."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            assert psm.force_overwrite is True
