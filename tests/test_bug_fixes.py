import pytest

from pipestat import SamplePipestatManager
from pipestat.exceptions import RecordNotFoundError


class TestBugFixes:
    """Tests for bug fixes: retrieve_many raise, __len__, __iter__, functools.wraps, parital typo."""

    def test_retrieve_many_raises_on_missing_records(self, schema_file_path, tmp_path):
        """retrieve_many() must raise RecordNotFoundError for nonexistent records."""
        psm = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=str(tmp_path / "results.yaml"),
        )
        psm.report(record_identifier="existing_sample", values={"name_of_something": "val"})
        with pytest.raises(RecordNotFoundError):
            psm.retrieve_many(record_identifiers=["nonexistent1", "nonexistent2"])

    def test_len_returns_record_count(self, schema_file_path, tmp_path):
        """len(psm) must return the number of records, not the config key count."""
        psm = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=str(tmp_path / "results.yaml"),
        )
        assert len(psm) == 0
        psm.report(record_identifier="sample1", values={"name_of_something": "a"})
        assert len(psm) == 1
        psm.report(record_identifier="sample2", values={"name_of_something": "b"})
        assert len(psm) == 2

    def test_iter_returns_all_records(self, schema_file_path, tmp_path):
        """for record in psm: must iterate over all records."""
        psm = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=str(tmp_path / "results.yaml"),
        )
        psm.report(record_identifier="s1", values={"name_of_something": "a"})
        psm.report(record_identifier="s2", values={"name_of_something": "b"})
        records = list(psm)
        assert len(records) == 2

    def test_functools_wraps_preserves_metadata(self, schema_file_path, tmp_path):
        """Decorated methods must retain their original __name__ and __doc__."""
        psm = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=str(tmp_path / "results.yaml"),
        )
        assert psm.report.__name__ == "report"
        assert psm.count_records.__name__ == "count_records"
        assert psm.report.__doc__ is not None
        assert psm.count_records.__doc__ is not None

    def test_partial_flag_key_in_constants(self):
        """APPEARANCE_BY_FLAG must use 'partial', not 'parital'."""
        from pipestat.const import APPEARANCE_BY_FLAG

        assert "partial" in APPEARANCE_BY_FLAG
        assert "parital" not in APPEARANCE_BY_FLAG

    def test_retrieve_additional_property(self, schema_file_path, tmp_path):
        """Reporting and retrieving a result not in the schema works when additional_properties is True."""
        psm = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=str(tmp_path / "results.yaml"),
        )
        psm.report(record_identifier="s1", values={"extra_result": 1.1})
        result = psm.retrieve_one(record_identifier="s1", result_identifier="extra_result")
        assert result == 1.1
