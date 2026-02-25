"""Tests for validation that empty string record_identifier is rejected."""

from tempfile import NamedTemporaryFile

import pytest

from pipestat import PipestatManager

from .conftest import get_data_file_path


@pytest.fixture
def schema_file_path():
    return get_data_file_path("sample_output_schema.yaml")


class TestEmptyRecordIdentifierRejected:
    """Empty string record_identifier must be rejected with ValueError."""

    def test_empty_record_identifier_in_init_raises(self, schema_file_path):
        """Passing record_identifier='' to PipestatManager() raises ValueError."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            with pytest.raises(ValueError):
                PipestatManager(
                    schema_path=schema_file_path,
                    results_file_path=f.name,
                    record_identifier="",
                )

    def test_empty_record_identifier_in_report_raises(self, schema_file_path):
        """Passing record_identifier='' to report() raises ValueError."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            with pytest.raises(ValueError):
                psm.report(
                    record_identifier="",
                    values={"name_of_something": "test"},
                )

    def test_none_record_identifier_is_ok_in_init(self, schema_file_path):
        """Passing record_identifier=None (default) to PipestatManager() is allowed."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
                record_identifier=None,
            )
            assert isinstance(psm, PipestatManager)

    def test_valid_record_identifier_works_in_report(self, schema_file_path):
        """A valid non-empty record_identifier works normally in report()."""
        with NamedTemporaryFile(suffix=".yaml") as f:
            psm = PipestatManager(
                schema_path=schema_file_path,
                results_file_path=f.name,
            )
            result = psm.report(
                record_identifier="sample1",
                values={"name_of_something": "test"},
            )
            assert result is not False
