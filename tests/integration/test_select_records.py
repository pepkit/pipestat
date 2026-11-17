"""PostgreSQL-backed tests for tests/test_select_records.py.

The shared test classes imported below are written once in tests/ and marked
`@pytest.mark.backends("file", "db")`; collected here, they run with backend="db".
"""

from tempfile import NamedTemporaryFile

import pytest

from pipestat import SamplePipestatManager

from ..conftest import db_context
from ..test_select_records import TestPipestatIter, TestSelectRecords  # noqa: F401


class TestSelectRecordsDB:
    @pytest.mark.parametrize("backend", ["db"])
    def test_select_records_column_doesnt_exist(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            if backend == "db":
                with pytest.raises(ValueError):
                    # Column doesn't exist raises error
                    psm.select_records(
                        filter_conditions=[
                            {
                                "key": "not_number_of_things",
                                "operator": "eq",
                                "value": 0,
                            },
                        ],
                        limit=50,
                        bool_operator="or",
                    )

    @pytest.mark.parametrize("backend", ["db"])
    def test_select_records_retrieve_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            # Gets one or many records
            result1 = psm.retrieve_one(record_identifier="sample1", result_identifier="md5sum")
            result2 = psm.retrieve_one(record_identifier="sample1")

            assert result1 == "hash1"
            # Verify all reported keys are present in the full record
            for key in range_values[1][1]:
                assert key in result2, f"Expected key '{key}' missing from retrieve_one result"
            assert "record_identifier" in result2


class TestSetIndexTrue:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["db"])
    def test_set_index(
        self,
        rec_id,
        val,
        config_file_path,
        output_schema_with_index,
        results_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=output_schema_with_index)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:10]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            mod = psm.backend.get_model(table_name=psm.backend.table_name)
            assert mod.md5sum.index is True
            assert mod.number_of_things.index is False
