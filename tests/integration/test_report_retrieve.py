"""PostgreSQL-backed tests for tests/test_report_retrieve.py.

The shared test classes imported below are written once in tests/ and marked
`@pytest.mark.backends("file", "db")`; collected here, they run with backend="db".
"""

from tempfile import NamedTemporaryFile

import pytest

from pipestat import SamplePipestatManager
from pipestat.exceptions import SchemaValidationErrorDuringReport

from ..conftest import REC_ID, STANDARD_TEST_PIPE_ID, db_context
from ..test_report_retrieve import (  # noqa: F401
    PROJECT_SAMPLE_LEVEL,
    TestNoRecordID,
    TestReporting,
    TestRetrieval,
    TestSplitClasses,
    assert_is_in_files,
    test_manager_has_correct_status_schema_and_status_schema_source,
)


class TestReportingDB:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
            ("sample1", {"number_of_things": 1}),
            ("sample2", {"number_of_things": 2}),
            ("sample2", {"percentage_of_things": 10.1}),
            ("sample2", {"name_of_something": "test_name"}),
            ("sample3", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["db"])
    def test_report_basic(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)
            if backend == "file":
                print(psm.backend._data[STANDARD_TEST_PIPE_ID])
                print(
                    "Test if",
                    rec_id,
                    " is in ",
                    psm.backend._data[STANDARD_TEST_PIPE_ID],
                )
                assert rec_id in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL]
                print("Test if", list(val.keys())[0], " is in ", rec_id)
                assert (
                    list(val.keys())[0]
                    in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL][rec_id]
                )
                if backend == "file":
                    assert_is_in_files(results_file_path, str(list(val.values())[0]))
            if backend == "db":
                # This is being captured in TestSplitClasses
                pass

    @pytest.mark.parametrize(
        "val",
        [
            {"output_file": {"path": "path_string", "title": "title_string"}},
            {
                "output_image": {
                    "path": "path_string",
                    "thumbnail_path": "thumbnail_path_string",
                    "title": "title_string",
                }
            },
        ],
    )
    @pytest.mark.parametrize("backend", ["db"])
    def test_complex_object_report_missing_fields(
        self, val, config_file_path, recursive_schema_file_path, results_file_path, backend
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
            del val[list(val.keys())[0]]["path"]
            with pytest.raises(SchemaValidationErrorDuringReport):
                psm.report(record_identifier=REC_ID, values=val)
