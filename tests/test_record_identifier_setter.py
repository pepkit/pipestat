"""Tests for the record_identifier setter on PipestatManager."""

import pytest

from pipestat import PipestatManager


class TestRecordIdentifierSetter:
    """Tests for setting record_identifier after construction."""

    def test_set_record_identifier_after_construction(self, tmp_path):
        """Setting record_identifier updates the weak-bound default."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  count:
    type: integer
    description: A count
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # Initially None (no record_identifier set)
        assert psm.record_identifier is None

        # Set it
        psm.record_identifier = "sample1"
        assert psm.record_identifier == "sample1"

    def test_set_record_identifier_enables_default_reporting(self, tmp_path):
        """After setting record_identifier, report/retrieve use it as default."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  count:
    type: integer
    description: A count
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        psm.record_identifier = "sample1"

        # Report without passing record_identifier -- uses the weak bound
        psm.report(values={"count": 42})

        # Retrieve without passing record_identifier
        result = psm.retrieve_one()
        assert result["count"] == 42

    def test_set_record_identifier_to_none(self, tmp_path):
        """Setting record_identifier to None clears it."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            record_identifier="sample1",
            validate_results=False,
        )

        assert psm.record_identifier == "sample1"

        psm.record_identifier = None
        assert psm.record_identifier is None

    def test_set_record_identifier_empty_string_raises(self, tmp_path):
        """Setting record_identifier to empty string raises ValueError."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        with pytest.raises(ValueError, match="record_identifier cannot be empty"):
            psm.record_identifier = ""

    def test_set_record_identifier_changes_default(self, tmp_path):
        """Changing record_identifier switches which record is the default."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        psm.record_identifier = "sample1"
        psm.report(values={"metric": 10})

        psm.record_identifier = "sample2"
        psm.report(values={"metric": 20})

        # Retrieve each
        psm.record_identifier = "sample1"
        assert psm.retrieve_one()["metric"] == 10

        psm.record_identifier = "sample2"
        assert psm.retrieve_one()["metric"] == 20
