import os.path
import datetime
from collections.abc import Mapping

import pytest
from jsonschema import ValidationError

from pipestat import SamplePipestatManager, ProjectPipestatManager, PipestatBoss
from pipestat.const import *
from pipestat.exceptions import *
from pipestat.parsed_schema import ParsedSchema
from pipestat.helpers import default_formatter, markdown_formatter
from pipestat.cli import main
from .conftest import (
    get_data_file_path,
    BACKEND_KEY_DB,
    BACKEND_KEY_FILE,
    COMMON_CUSTOM_STATUS_DATA,
    DEFAULT_STATUS_DATA,
    STANDARD_TEST_PIPE_ID,
    DB_URL,
)
from tempfile import NamedTemporaryFile, TemporaryDirectory

from .test_db_only_mode import ContextManagerDBTesting

CONST_REC_ID = "constant_record_id"
PROJECT_SAMPLE_LEVEL = "sample"


def assert_is_in_files(fs, s):
    """
    Verify if string is in files content

    :param str | Iterable[str] fs: list of files
    :param str s: string to look for
    """
    for f in [fs] if isinstance(fs, str) else fs:
        with open(f, "r") as fh:
            assert s in fh.read()


class TestSplitClasses:
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
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            val_name = list(val.keys())[0]
            psm.set_status(status_identifier="running", record_identifier=rec_id)
            status = psm.get_status(record_identifier=rec_id)
            assert status == "running"
            assert val_name in psm.retrieve(record_identifier=rec_id)
            psm.remove(record_identifier=rec_id, result_identifier=val_name)
            if backend == "file":
                psm.clear_status(record_identifier=rec_id)
                status = psm.get_status(record_identifier=rec_id)
                assert status is None
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(record_identifier=rec_id)
            if backend == "db":
                assert getattr(psm.retrieve(record_identifier=rec_id), val_name, None) is None
                psm.remove(record_identifier=rec_id)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(record_identifier=rec_id)

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("project_name_1", {"name_of_something": "test_name"}),
            ("project_name_1", {"number_of_things": 1}),
            ("project_name_2", {"number_of_things": 2}),
            ("project_name_2", {"percentage_of_things": 10.1}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_project_level(
        self,
        rec_id,
        val,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = ProjectPipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            val_name = list(val.keys())[0]
            psm.set_status(status_identifier="running", record_identifier=rec_id)
            status = psm.get_status(record_identifier=rec_id)
            assert status == "running"
            assert val_name in psm.retrieve(record_identifier=rec_id)
            psm.remove(record_identifier=rec_id, result_identifier=val_name)
            if backend == "file":
                psm.clear_status(record_identifier=rec_id)
                status = psm.get_status(record_identifier=rec_id)
                assert status is None
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(record_identifier=rec_id)
            if backend == "db":
                assert (
                    getattr(
                        psm.retrieve(record_identifier=rec_id),
                        val_name,
                        None,
                    )
                    is None
                )
                psm.remove(record_identifier=rec_id)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(record_identifier=rec_id)


class TestReporting:
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
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_basic(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            if backend == "file":
                print(psm.backend._data[STANDARD_TEST_PIPE_ID])
                print("Test if", rec_id, " is in ", psm.backend._data[STANDARD_TEST_PIPE_ID])
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
        ["rec_id", "val"],
        [
            ("sample3", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_requires_schema(
        self,
        rec_id,
        val,
        config_no_schema_file_path,
        results_file_path,
        backend,
    ):
        """
        If schema is not provided at object instantiation stage, SchemaNotFondError
        is raised if report method is called with file as a backend.

        In case of the DB as a backend, the error is raised at object
        instantiation stage since there is no way to init relational DB table
        with no columns predefined
        """
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict()
            backend_data = (
                {"config_file": config_no_schema_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            if backend == "db":
                with pytest.raises(SchemaNotFoundError):
                    psm = SamplePipestatManager(**args)

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [("sample1", {"number_of_things": 2}), ("sample2", {"number_of_things": 1})],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_overwrite(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            if backend == "file":
                assert rec_id in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL]
                assert (
                    list(val.keys())[0]
                    in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL][rec_id]
                )
                if backend == "file":
                    assert_is_in_files(results_file_path, str(list(val.values())[0]))
            if backend == "db":
                assert list(val.keys())[0] in psm.retrieve(record_identifier=rec_id)

    @pytest.mark.parametrize(
        ["rec_id", "val", "success"],
        [
            ("sample1", {"number_of_things": "2"}, True),
            ("sample2", {"number_of_things": [1, 2, 3]}, False),
            ("sample2", {"output_file": {"path": 1, "title": "abc"}}, True),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_type_casting(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
        success,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            if success:
                psm.report(
                    record_identifier=rec_id,
                    values=val,
                    strict_type=False,
                    force_overwrite=True,
                )
            else:
                with pytest.raises((ValidationError, TypeError)):
                    psm.report(
                        record_identifier=rec_id,
                        values=val,
                        strict_type=False,
                        force_overwrite=True,
                    )

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
            ("sample2", {"number_of_things": 2}),
            ("sample3", {"dict_object": {"key": "value"}}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file"])
    @pytest.mark.parametrize("formatter", [default_formatter, markdown_formatter])
    def test_report_formatter(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
        formatter,
    ):
        """Simply test that we can pass the formatting functions and the returned result contains reported results"""
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            results = psm.report(
                record_identifier=rec_id,
                values=val,
                force_overwrite=True,
                result_formatter=formatter,
            )
            assert rec_id in results[0]
            value = list(val.keys())[0]
            assert value in results[0]


class TestRetrieval:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
            ("sample1", {"number_of_things": 2}),
            ("sample2", {"number_of_things": 1}),
            ("sample2", {"percentage_of_things": 10.1}),
            ("sample2", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_basic(
        self,
        rec_id,
        val,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            retrieved_val = psm.retrieve(
                record_identifier=rec_id, result_identifier=list(val.keys())[0]
            )
            # Test Retrieve Basic
            assert str(retrieved_val) == str(list(val.values())[0])
            # Test Retrieve Whole Record
            assert isinstance(psm.retrieve(record_identifier=rec_id), Mapping)

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_get_records(
        self,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            val_dict = {
                "sample1": {"name_of_something": "test_name"},
                "sample2": {"number_of_things": 2},
            }
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args = dict(schema_path=schema_file_path)
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            for k, v in val_dict.items():
                psm.report(record_identifier=k, values=v, force_overwrite=True)

            results = psm.get_records()
            assert results["records"][0] == list(val_dict.keys())[0]
            assert results["count"] == len(list(val_dict.keys()))
            results = psm.get_records(limit=1, offset=1)
            assert results["records"][0] == list(val_dict.keys())[1]
            assert results["count"] == 2

    @pytest.mark.parametrize(
        ["rec_id", "res_id"],
        [("nonexistent", "name_of_something"), ("sample1", "nonexistent")],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_nonexistent(
        self,
        rec_id,
        res_id,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            val_dict = {
                "sample1": {"name_of_something": "test_name"},
                "sample1": {"number_of_things": 2},
            }
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args = dict(schema_path=schema_file_path)
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            for k, v in val_dict.items():
                psm.report(record_identifier=k, values=v, force_overwrite=True)
            if backend == "db":
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(result_identifier=res_id, record_identifier=rec_id)
            else:
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(result_identifier=res_id, record_identifier=rec_id)


class TestRemoval:
    @pytest.mark.parametrize(["rec_id", "res_id", "val"], [("sample2", "number_of_things", 1)])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_basic(
        self,
        rec_id,
        res_id,
        val,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            vals = [
                {"number_of_things": 1},
                {"name_of_something": "test_name"},
            ]
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            for v in vals:
                psm.report(record_identifier=rec_id, values=v, force_overwrite=True)
            psm.remove(result_identifier=res_id, record_identifier=rec_id)
            if backend != "db":
                assert (
                    # res_id not in psm.data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL][rec_id]
                    res_id
                    not in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL][rec_id]
                )
            else:
                col_name = list(vals[0].keys())[0]
                value = list(vals[0].values())[0]
                result = psm.backend.select(filter_conditions=[(col_name, "eq", value)])
                assert len(result) == 0

    @pytest.mark.parametrize("rec_id", ["sample1", "sample2"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            vals = [
                {"number_of_things": 1},
                {"name_of_something": "test_name"},
            ]
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            for v in vals:
                psm.report(record_identifier=rec_id, values=v, force_overwrite=True)
            psm.remove(record_identifier=rec_id)
            if backend != "db":
                assert rec_id not in psm.backend._data[STANDARD_TEST_PIPE_ID]
            else:
                col_name = list(vals[0].keys())[0]
                value = list(vals[0].values())[0]
                result = psm.backend.select(filter_conditions=[(col_name, "eq", value)])
                assert len(result) == 0

    @pytest.mark.parametrize(
        ["rec_id", "res_id"], [("sample2", "nonexistent"), ("sample2", "bogus")]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_nonexistent_result(
        self,
        rec_id,
        res_id,
        schema_file_path,
        config_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            assert not psm.remove(record_identifier=rec_id, result_identifier=res_id)

    @pytest.mark.parametrize("rec_id", ["nonexistent", "bogus"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_nonexistent_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            assert not psm.remove(record_identifier=rec_id)

    @pytest.mark.parametrize(["rec_id", "res_id"], [("sample3", "name_of_something")])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_last_result_removal_removes_record(
        self,
        rec_id,
        res_id,
        schema_file_path,
        config_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(
                record_identifier=rec_id, values={res_id: "something"}, force_overwrite=True
            )
            assert psm.remove(record_identifier=rec_id, result_identifier=res_id)
            if backend == "file":
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve(record_identifier=rec_id)


class TestNoRecordID:
    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    @pytest.mark.parametrize("pipeline_type", ["sample"])
    def test_report(
        self,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
        pipeline_type,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(
                schema_path=schema_file_path,
                record_identifier=CONST_REC_ID,
                database_only=False,
            )
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(values=val)
            if backend == "file":
                assert_is_in_files(results_file_path, str(list(val.values())[0]))
                assert (
                    CONST_REC_ID in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL]
                )
                assert (
                    list(val.keys())[0]
                    in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL][CONST_REC_ID]
                )
            if backend == "db":
                val_name = list(val.keys())[0]
                assert psm.backend.select(filter_conditions=[(val_name, "eq", val[val_name])])

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve(self, val, config_file_path, schema_file_path, results_file_path, backend):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, record_identifier=CONST_REC_ID)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier="constant_record_id", values=val, force_overwrite=True)
            retrieved_val = psm.retrieve(result_identifier=list(val.keys())[0])
            assert str(retrieved_val) == str(list(val.values())[0])

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove(self, val, config_file_path, schema_file_path, results_file_path, backend):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, record_identifier=CONST_REC_ID)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier="constant_record_id", values=val, force_overwrite=True)
            assert psm.remove(result_identifier=list(val.keys())[0])


def test_highlighting_works(highlight_schema_file_path, results_file_path):
    """the highlighted results are sourced from the schema and only ones
    that are indicated with 'highlight: true` are respected"""
    with NamedTemporaryFile() as f:
        results_file_path = f.name
        s = ParsedSchema(highlight_schema_file_path)
        schema_highlighted_results = [
            k for k, v in s.sample_level_data.items() if v.get("highlight") is True
        ]
        psm = SamplePipestatManager(
            results_file_path=results_file_path,
            schema_path=highlight_schema_file_path,
        )
        assert psm.highlighted_results == schema_highlighted_results


class TestEnvVars:
    def test_no_config__psm_is_built_from_env_vars(
        self, monkeypatch, results_file_path, schema_file_path
    ):
        """
        test that the object can be created if the arguments
        are provided as env vars
        """

        monkeypatch.setenv(ENV_VARS["record_identifier"], "sample1")
        monkeypatch.setenv(ENV_VARS["results_file"], results_file_path)
        monkeypatch.setenv(ENV_VARS["schema"], schema_file_path)
        try:
            SamplePipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")

    # @pytest.mark.skip(reason="known failure for now with config file")
    def test_config__psm_is_built_from_config_file_env_var(self, monkeypatch, config_file_path):
        """PSM can be created from config parsed from env var value."""
        monkeypatch.setenv(ENV_VARS["config"], config_file_path)
        try:
            SamplePipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")


def test_no_constructor_args__raises_expected_exception():
    """See Issue #3 in the repository."""
    with pytest.raises(SchemaNotFoundError):
        SamplePipestatManager()


def absolutize_file(f: str) -> str:
    return f if os.path.isabs(f) else get_data_file_path(f)


@pytest.mark.parametrize(
    ["schema_file_path", "exp_status_schema", "exp_status_schema_path"],
    [
        (absolutize_file(fn1), exp_status_schema, absolutize_file(fn2))
        for fn1, exp_status_schema, fn2 in [
            ("sample_output_schema.yaml", DEFAULT_STATUS_DATA, STATUS_SCHEMA),
            (
                "sample_output_schema__with_project_with_samples_with_status.yaml",
                COMMON_CUSTOM_STATUS_DATA,
                "sample_output_schema__with_project_with_samples_with_status.yaml",
            ),
            (
                "sample_output_schema__with_project_with_samples_without_status.yaml",
                DEFAULT_STATUS_DATA,
                STATUS_SCHEMA,
            ),
            (
                "sample_output_schema__with_project_without_samples_with_status.yaml",
                COMMON_CUSTOM_STATUS_DATA,
                "sample_output_schema__with_project_without_samples_with_status.yaml",
            ),
            (
                "sample_output_schema__with_project_without_samples_without_status.yaml",
                DEFAULT_STATUS_DATA,
                STATUS_SCHEMA,
            ),
            (
                "sample_output_schema__without_project_with_samples_with_status.yaml",
                COMMON_CUSTOM_STATUS_DATA,
                "sample_output_schema__without_project_with_samples_with_status.yaml",
            ),
            (
                "sample_output_schema__without_project_with_samples_without_status.yaml",
                DEFAULT_STATUS_DATA,
                STATUS_SCHEMA,
            ),
        ]
    ],
)
@pytest.mark.parametrize("backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True)
def test_manager_has_correct_status_schema_and_status_schema_source(
    schema_file_path, exp_status_schema, exp_status_schema_path, backend_data
):
    psm = SamplePipestatManager(schema_path=schema_file_path, **backend_data)
    assert psm.status_schema == exp_status_schema
    assert psm.status_schema_source == exp_status_schema_path


class TestPipestatBoss:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basic_pipestatboss(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
    ):
        pipeline_list = ["sample", "project"]
        values_project = [
            {"project_name_1": {"number_of_things": 2}},
            {"project_name_1": {"name_of_something": "name of something string"}},
        ]

        values_sample = [
            {"sample4": {"smooth_bw": "smooth_bw string"}},
            {"sample5": {"output_file": {"path": "path_string", "title": "title_string"}}},
        ]

        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)

            psb = PipestatBoss(pipeline_list=pipeline_list, **args)

            for i in values_sample:
                for r, v in i.items():
                    psb.samplemanager.report(record_identifier=r, values=v, force_overwrite=True)
                    psb.samplemanager.set_status(record_identifier=r, status_identifier="running")
            for i in values_project:
                for r, v in i.items():
                    psb.projectmanager.report(record_identifier=r, values=v, force_overwrite=True)
                    psb.projectmanager.set_status(record_identifier=r, status_identifier="running")


class TestHTMLReport:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_samples(
        self,
        rec_id,
        val,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
    ):
        values_project = [
            {"project_name_1": {"number_of_things": 2}},
            {"project_name_1": {"name_of_something": "name of something string"}},
        ]
        values_sample = [
            {"sample4": {"smooth_bw": "smooth_bw string"}},
            {"sample5": {"output_file": {"path": "path_string", "title": "title_string"}}},
            {"sample4": {"aligned_bam": "aligned_bam string"}},
            {"sample6": {"output_file": {"path": "path_string", "title": "title_string"}}},
            {
                "sample7": {
                    "output_image": {
                        "path": "path_string",
                        "thumbnail_path": "path_string",
                        "title": "title_string",
                    }
                }
            },
        ]
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in values_sample:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v, force_overwrite=True)
                    psm.set_status(record_identifier=r, status_identifier="running")

            try:
                htmlreportpath = psm.summarize(amendment="")
                assert htmlreportpath is not None
            except:
                assert 0

            try:
                table_paths = psm.table()
                assert table_paths is not None
            except:
                assert 0

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_project(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
    ):
        values_project = [
            {"project_name_1": {"number_of_things": 2}},
            {"project_name_1": {"name_of_something": "name of something string"}},
        ]

        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            # project level
            psm = ProjectPipestatManager(**args)

            for i in values_project:
                for r, v in i.items():
                    psm.report(
                        record_identifier=r,
                        values=v,
                        force_overwrite=True,
                    )
                    psm.set_status(record_identifier=r, status_identifier="running")

            try:
                htmlreportpath = psm.summarize(amendment="")
                assert htmlreportpath is not None
            except:
                assert 0

            try:
                table_paths = psm.table()
                assert table_paths is not None
            except:
                assert 0


class TestPipestatCLI:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)

            # report
            if backend != "db":
                x = [
                    "report",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--value",
                    list(val.values())[0],
                    "--results-file",
                    results_file_path,
                    "--schema",
                    schema_file_path,
                ]
            else:
                x = [
                    "report",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--value",
                    list(val.values())[0],
                    "--config-file",
                    config_file_path,
                    "--schema",
                    schema_file_path,
                ]

            with pytest.raises(
                SystemExit
            ):  # pipestat cli normal behavior is to end with a "sys.exit(0)"
                main(test_args=x)

            # retrieve
            if backend != "db":
                x = [
                    "retrieve",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--value",
                    list(val.values())[0],
                    "--results-file",
                    results_file_path,
                    "--schema",
                    schema_file_path,
                ]
            else:
                x = [
                    "retrieve",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--value",
                    list(val.values())[0],
                    "--config-file",
                    config_file_path,
                    "--schema",
                    schema_file_path,
                ]

            with pytest.raises(
                SystemExit
            ):  # pipestat cli normal behavior is to end with a "sys.exit(0)"
                main(test_args=x)


class TestFileTypeLinking:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_linking(
        self,
        config_file_path,
        output_schema_as_JSON_schema,
        results_file_path,
        backend,
    ):
        # paths to images and files
        path_file_1 = get_data_file_path("test_file_links/results/project_dir_example_1/ex1.txt")
        path_file_2 = get_data_file_path("test_file_links/results/project_dir_example_1/ex2.txt")
        path_image_1 = get_data_file_path("test_file_links/results/project_dir_example_1/ex3.png")
        path_image_2 = get_data_file_path("test_file_links/results/project_dir_example_1/ex4.png")

        values_sample = [
            {"sample1": {"number_of_things": 100}},
            {"sample2": {"number_of_things": 200}},
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
                    "output_file_in_object": {
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

        with NamedTemporaryFile() as f, TemporaryDirectory() as d, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            temp_dir = d
            args = dict(schema_path=output_schema_as_JSON_schema, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            schema = ParsedSchema(output_schema_as_JSON_schema)
            print(schema)

            for i in values_sample:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v, force_overwrite=True)
                    psm.set_status(record_identifier=r, status_identifier="running")

            os.mkdir(temp_dir + "/test_file_links")
            output_dir = get_data_file_path(temp_dir + "/test_file_links")
            try:
                linkdir = psm.link(link_dir=output_dir)
            except Exception:
                assert False

            # Test simple
            for root, dirs, files in os.walk(os.path.join(linkdir, "output_file")):
                assert "sample1_output_file_ex1.txt" in files
            # Test complex types
            for root, dirs, files in os.walk(os.path.join(linkdir, "output_file_in_object")):
                assert "sample2_example_property_1_ex1.txt" in files

            for root, dirs, files in os.walk(os.path.join(linkdir, "output_file_nested_object")):
                # TODO This example will have collision if the file names and property names are the same
                print(files)


class TestTimeStamp:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["db"])
    def test_basic_time_stamp(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)

            # CHECK CREATION AND MODIFY TIME EXIST
            created = psm.retrieve(record_identifier=rec_id, result_identifier=CREATED_TIME)
            modified = psm.retrieve(record_identifier=rec_id, result_identifier=MODIFIED_TIME)
            assert created is not None
            assert modified is not None
            assert created == modified
            # Report new
            val = {"number_of_things": 1}
            psm.report(record_identifier="sample1", values=val, force_overwrite=True)
            # CHECK MODIFY TIME DIFFERS FROM CREATED TIME
            created = psm.retrieve(record_identifier=rec_id, result_identifier=CREATED_TIME)
            modified = psm.retrieve(record_identifier=rec_id, result_identifier=MODIFIED_TIME)
            assert created != modified

    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_filtering_by_time(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            # Report a few values
            val = {"number_of_things": 1}
            for i in range(10):
                rid = "sample" + str(i)
                psm.report(record_identifier=rid, values=val, force_overwrite=True)

            # Modify a couple of records
            val = {"number_of_things": 2}
            psm.report(record_identifier="sample3", values=val, force_overwrite=True)
            psm.report(record_identifier="sample4", values=val, force_overwrite=True)

            # Test default
            results = psm.list_recent_results()
            assert len(results["records"]) == 10

            # Test limit
            results = psm.list_recent_results(limit=2)
            assert len(results["records"]) == 2

            # Test garbled time raises error
            with pytest.raises(InvalidTimeFormatError):
                results = psm.list_recent_results(start="2100-01-01dsfds", end="1970-01-01")

            # Test large window
            results = psm.list_recent_results(start="2100-01-01 0:0:0", end="1970-01-01 0:0:0")
            assert len(results["records"]) == 10
