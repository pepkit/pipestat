import glob
import os.path
import time
from collections.abc import Mapping
from yacman import YAMLConfigManager

import pytest
from jsonschema import ValidationError

from pipestat import SamplePipestatManager, ProjectPipestatManager, PipestatBoss, PipestatManager
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
    SERVICE_UNAVAILABLE,
    DB_URL,
    REC_ID,
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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
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
            assert psm.retrieve_one(record_identifier=rec_id)[val_name] == val[val_name]
            psm.remove(record_identifier=rec_id, result_identifier=val_name)
            if backend == "file":
                psm.clear_status(record_identifier=rec_id)
                status = psm.get_status(record_identifier=rec_id)
                assert status is None
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)
            if backend == "db":
                assert psm.retrieve_one(record_identifier=rec_id).get(val_name, None) is None
                psm.remove(record_identifier=rec_id)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)

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
            assert psm.retrieve_one(record_identifier=rec_id)[val_name] == val[val_name]
            psm.remove(record_identifier=rec_id, result_identifier=val_name)
            if backend == "file":
                psm.clear_status(record_identifier=rec_id)
                status = psm.get_status(record_identifier=rec_id)
                assert status is None
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)
            if backend == "db":
                psm.remove(record_identifier=rec_id)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires postgres service to be available")
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
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    @pytest.mark.parametrize("pipeline_type", ["project", "sample"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_samples_and_project_with_pipestatmanager(
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
                schema_path=schema_file_path, pipeline_type=pipeline_type, database_only=False
            )
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)

            psm = PipestatManager(**args)
            val_name = list(val.keys())[0]
            if pipeline_type == "project":
                if val_name in psm.cfg[SCHEMA_KEY].project_level_data:
                    assert psm.report(
                        record_identifier="constant_record_id",
                        values=val,
                        force_overwrite=True,
                        strict_type=False,
                    )

            if pipeline_type == "sample":
                if val_name in psm.cfg[SCHEMA_KEY].sample_level_data:
                    assert psm.report(
                        record_identifier="constant_record_id",
                        values=val,
                        force_overwrite=True,
                        strict_type=False,
                    )

    @pytest.mark.parametrize(
        "val",
        [
            {
                "collection_of_images": [
                    {
                        "items": {
                            "properties": {
                                "prop1": {
                                    "properties": {
                                        "path": "pathstring",
                                        "title": "titlestring",
                                    }
                                }
                            }
                        }
                    }
                ]
            },
            {"output_file": {"path": "path_string", "title": "title_string"}},
            {
                "output_image": {
                    "path": "path_string",
                    "thumbnail_path": "thumbnail_path_string",
                    "title": "title_string",
                }
            },
            {
                "output_file_in_object": {
                    "properties": {
                        "prop1": {"properties": {"path": "pathstring", "title": "titlestring"}}
                    }
                }
            },
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_complex_object_report(
        self, val, config_file_path, recursive_schema_file_path, results_file_path, backend
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)

            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=REC_ID, values=val, force_overwrite=True)
            val_name = list(val.keys())[0]
            assert psm.select_records(
                filter_conditions=[
                    {
                        "key": val_name,
                        "operator": "eq",
                        "value": val[val_name],
                    }
                ]
            )

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_setitem(
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
            psm[rec_id] = val
            result = psm.retrieve_one(record_identifier=rec_id)
            assert list(val.keys())[0] in result

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
            # Report some other value to be overwritten
            psm.report(
                record_identifier=rec_id, values={list(val.keys())[0]: 1000}, force_overwrite=True
            )
            # Now overwrite
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            assert (
                psm.retrieve_one(record_identifier=rec_id)[list(val.keys())[0]]
                == val[list(val.keys())[0]]
            )

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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestRetrieval:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
            ("sample1", {"number_of_things": 2}),
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
            retrieved_val = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": rec_id,
                    },
                ],
                columns=[list(val.keys())[0]],
            )["records"][0]
            # Test Retrieve Basic
            assert str(list(val.keys())[0]) in list(retrieved_val.keys())
            # Test Retrieve Whole Record
            assert isinstance(psm.retrieve_one(record_identifier=rec_id), Mapping)

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
            ("sample1", {"number_of_things": 2}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_getitem(
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

            val_name = list(val.keys())[0]
            retrieved_val = psm[rec_id][val_name]
            value = list(val.values())[0]

            assert retrieved_val == value

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_no_filter(
        self, config_file_path, results_file_path, schema_file_path, backend, val_dict
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
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

            results = psm.select_records()

            assert results["records"][0]["record_identifier"] == list(val_dict.keys())[0]
            assert results["total_size"] == len(list(val_dict.keys()))
            results = psm.select_records(limit=1)
            assert len(results["records"]) == 1

    @pytest.mark.skip("This test needs to be re-done with the 0.6.0 api changes")
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

            if res_id == "nonexistent" and backend == "db":
                with pytest.raises(ColumnNotFoundError):
                    result = psm.select_records(
                        filter_conditions=[
                            {
                                "key": RECORD_IDENTIFIER,
                                "operator": "eq",
                                "value": rec_id,
                            }
                        ],
                        columns=[res_id],
                    )

            if res_id == "nonexistent" and backend == "file":
                with pytest.raises(ColumnNotFoundError):
                    result = psm.select_records(
                        filter_conditions=[
                            {
                                "key": RECORD_IDENTIFIER,
                                "operator": "eq",
                                "value": rec_id,
                            }
                        ],
                        columns=[res_id],
                    )
            #         assert len(result["records"]) == 0
            # if res_id == "nonexistent" and backend == "file":
            #     with pytest.raises(RecordNotFoundError):
            #         psm.retrieve_one(result_identifier=res_id, record_identifier=rec_id)

            # assert len(result['records'][0]) == 0


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires postgres service to be available")
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
        val_dict,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            vals = [val_dict]
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for val in vals:
                for key, value in val.items():
                    psm.report(record_identifier=key, values=value, force_overwrite=True)

            psm.remove(result_identifier=res_id, record_identifier=rec_id)

            col_name = res_id
            value = list(vals[0].values())[1][res_id]
            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": col_name,
                        "operator": "eq",
                        "value": value,
                    }
                ]
            )
            assert len(result["records"]) == 0

    @pytest.mark.parametrize("rec_id", ["sample1"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend, val_dict
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            vals = [val_dict]
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            for val in vals:
                for key, value in val.items():
                    psm.report(record_identifier=key, values=value, force_overwrite=True)
            psm.remove(record_identifier=rec_id)
            col_name = list(list(vals[0].values())[0].keys())[0]
            value = list(list(vals[0].values())[0].values())[0]
            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": col_name,
                        "operator": "eq",
                        "value": value,
                    }
                ]
            )
            assert len(result["records"]) == 0

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
    @pytest.mark.parametrize("backend", ["file"])
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
                record_identifier=rec_id,
                values={res_id: "something"},
                force_overwrite=True,
            )
            assert psm.remove(record_identifier=rec_id, result_identifier=res_id)

            with pytest.raises(RecordNotFoundError):
                result = psm.retrieve_one(record_identifier=rec_id)


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires postgres service to be available")
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
            val_name = list(val.keys())[0]
            assert psm.select_records(
                filter_conditions=[
                    {
                        "key": val_name,
                        "operator": "eq",
                        "value": val[val_name],
                    }
                ]
            )

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
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
            retrieved_val = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": "constant_record_id",
                    },
                ],
                columns=[list(val.keys())[0]],
            )["records"][0]
            assert str(list(val.keys())[0]) in list(retrieved_val.keys())

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
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
@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
@pytest.mark.parametrize("backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True)
def test_manager_has_correct_status_schema_and_status_schema_source(
    schema_file_path, exp_status_schema, exp_status_schema_path, backend_data
):
    with ContextManagerDBTesting(DB_URL):
        psm = SamplePipestatManager(schema_path=schema_file_path, **backend_data)
        assert psm.cfg[STATUS_SCHEMA_KEY] == exp_status_schema
        assert psm.cfg[STATUS_SCHEMA_SOURCE_KEY] == exp_status_schema_path


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestPipestatBoss:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basic_pipestatboss(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
        values_project,
    ):
        pipeline_list = ["sample", "project"]

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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestHTMLReport:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_samples_html_report(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
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
            psm = SamplePipestatManager(**args)

            for i in values_sample:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v, force_overwrite=True)
                    psm.set_status(record_identifier=r, status_identifier="running")

            htmlreportpath = psm.summarize(amendment="")
            assert htmlreportpath is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_project_html_report(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
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

            htmlreportpath = psm.summarize(amendment="")
            assert htmlreportpath is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_html_report_portable(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
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

            htmlreportpath = psm.summarize(amendment="", portable=True)
            assert htmlreportpath is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_zip_html_report_portable(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
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

            htmlreportpath = psm.summarize(amendment="", portable=True)

            directory = os.path.dirname(htmlreportpath)
            zip_files = glob.glob(directory + "*.zip")

            assert len(zip_files) > 0


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestTableCreation:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_table_for_samples(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
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
            psm = SamplePipestatManager(**args)

            for i in values_sample:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v, force_overwrite=True)
                    psm.set_status(record_identifier=r, status_identifier="running")

            table_paths = psm.table()
            assert table_paths is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_table_for_project(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
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

            for i in values_project:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v, force_overwrite=True)
                    psm.set_status(record_identifier=r, status_identifier="running")

            table_paths = psm.table()
            assert table_paths is not None


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestFileTypeLinking:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_linking(
        self,
        config_file_path,
        output_schema_as_JSON_schema,
        results_file_path,
        backend,
        values_complex_linking,
    ):
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

            for i in values_complex_linking:
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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestTimeStamp:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
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

            created = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": rec_id,
                    },
                ],
                columns=[CREATED_TIME],
            )["records"][0][CREATED_TIME]

            modified = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": rec_id,
                    },
                ],
                columns=[MODIFIED_TIME],
            )["records"][0][MODIFIED_TIME]

            assert created is not None
            assert modified is not None
            assert created == modified
            # Report new
            val = {"number_of_things": 1}
            time.sleep(
                1
            )  # The filebackend is so fast that the updated time will equal the created time
            psm.report(record_identifier="sample1", values=val, force_overwrite=True)
            # CHECK MODIFY TIME DIFFERS FROM CREATED TIME
            created = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": rec_id,
                    },
                ],
                columns=[CREATED_TIME],
            )["records"][0][CREATED_TIME]

            modified = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": rec_id,
                    },
                ],
                columns=[MODIFIED_TIME],
            )["records"][0][MODIFIED_TIME]

            assert created != modified

    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_list_recent_results(
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
                psm.list_recent_results(start="2100-01-01dsfds", end="1970-01-01")

            # Test large window
            results = psm.list_recent_results(start="2100-01-01 0:0:0", end="1970-01-01 0:0:0")
            assert len(results["records"]) == 10


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestSelectRecords:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_basic(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 80,
                    },
                ],
            )

            assert len(result1["records"]) == 4

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 80,
                    },
                    {
                        "key": "percentage_of_things",
                        "operator": "eq",
                        "value": 1,
                    },
                ],
            )

            assert len(result1["records"]) == 2

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 80,
                    },
                    {
                        "key": "percentage_of_things",
                        "operator": "eq",
                        "value": 1,
                    },
                ],
                bool_operator="OR",
            )
            assert len(result1["records"]) == 8

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_columns(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 0,
                    },
                ],
                columns=["number_of_things"],
            )

            print(result1)
            assert len(result1["records"][0]) == 2

    @pytest.mark.parametrize("backend", [BACKEND_KEY_FILE, BACKEND_KEY_DB])
    def test_select_records_columns_record_identifier(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result1 = psm.select_records(
                columns=["record_identifier"],
            )
            assert len(list(result1["records"][0].keys())) == 1

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_no_filter(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result1 = psm.select_records()

            print(result1)
            assert len(result1["records"]) == 2

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_no_filter_limit(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result1 = psm.select_records(limit=1)

            print(result1)
            assert len(result1["records"]) == 1

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_bad_operator_bad_key(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            with pytest.raises(ValueError):
                # Unknown operator raises error
                result20 = psm.select_records(
                    # columns=["md5sum"],
                    filter_conditions=[
                        {
                            "key": ["output_file_in_object_nested", "prop1", "prop2"],
                            "operator": "bad_operator",
                            "value": [7, 21],
                        },
                    ],
                    limit=50,
                    bool_operator="or",
                )

            with pytest.raises(ValueError):
                # bad key raises error
                psm.select_records(
                    filter_conditions=[
                        {
                            "_garbled_key": "number_of_things",
                            "operator": "eq",
                            "value": 0,
                        }
                    ],
                    limit=50,
                    bool_operator="or",
                )

    @pytest.mark.parametrize("backend", ["db"])
    def test_select_records_column_doesnt_exist(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

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

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_complex_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": ["output_image", "path"],
                        "operator": "eq",
                        "value": "path_to_1",
                    },
                ],
            )

            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": ["output_file_in_object_nested", "prop1", "prop2"],
                        "operator": "eq",
                        "value": 1,
                    },
                ],
            )

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_retrieve_one(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:6]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            # Gets one or many records
            result1 = psm.retrieve_one(record_identifier="sample1")

            assert result1["record_identifier"] == "sample1"

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_retrieve_many(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:6]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            result = psm.retrieve_many(["sample1", "sample3", "sample5"])
            assert len(result["records"]) == 3

            assert result["records"][2]["record_identifier"] == "sample5"

    @pytest.mark.parametrize("backend", ["db"])
    def test_select_records_retrieve_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            # Gets one or many records
            result1 = psm.retrieve_one(record_identifier="sample1", result_identifier="md5sum")
            result2 = psm.retrieve_one(record_identifier="sample1")

            assert result1 == "hash1"
            assert len(result2.keys()) == 16

    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_select_records_retrieve_multi_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            # Gets one or many records
            result1 = psm.retrieve_one(
                record_identifier="sample1", result_identifier=["md5sum", "number_of_things"]
            )
            print(result1)

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_records_retrieve_many_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            # Gets one or many records
            result1 = psm.retrieve_many(
                record_identifiers=["sample0", "sample1"], result_identifier="md5sum"
            )

            assert len(result1["records"]) == 2

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_distinct(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            for i in range(0, 10, 2):
                r_id = "sample" + str(i)
                val = {
                    "md5sum": "hash0",
                    "number_of_things": 500,
                }
                # Overwrite a couple of results such that they are not all unique
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            val = {
                "number_of_things": 900,
            }
            psm.report(record_identifier="sample2", values=val, force_overwrite=True)
            # Gets one or many records
            result1 = psm.select_distinct(
                columns=["md5sum", "number_of_things", "record_identifier"]
            )
            assert len(result1) == 10
            result2 = psm.select_distinct(columns=["md5sum", "number_of_things"])
            assert len(result2) == 7
            result3 = psm.select_distinct(columns=["md5sum"])
            assert len(result3) == 6

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_select_distinct_string(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            for i in range(0, 10, 2):
                r_id = "sample" + str(i)
                val = {
                    "md5sum": "hash0",
                    "number_of_things": 500,
                }
                # Overwrite a couple of results such that they are not all unique
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            val = {
                "number_of_things": 900,
            }
            psm.report(record_identifier="sample2", values=val, force_overwrite=True)
            # Gets one or many records
            result3 = psm.select_distinct(columns="md5sum")
            assert len(result3) == 6


class TestPipestatIter:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_pipestat_iter(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:12]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            cursor = 10
            limit = 6
            count = 0
            for j in psm.__iter__(cursor=cursor, limit=limit):
                count += 1

            if backend == "db":
                assert count == 2
            if backend == "file":
                assert count == 6


class TestMultiResultFiles:
    @pytest.mark.parametrize("backend", ["file"])
    def test_multi_results_not_implemented(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, TemporaryDirectory() as d, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            temp_dir = d
            single_results_file_path = "{record_identifier}_results.yaml"
            results_file_path = os.path.join(temp_dir, single_results_file_path)
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            backend_data = {"results_file_path": results_file_path}
            args.update(backend_data)

            with pytest.raises(NotImplementedError):
                psm = SamplePipestatManager(**args)

    @pytest.mark.parametrize("backend", ["file"])
    def test_multi_results_basic(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with TemporaryDirectory() as d, ContextManagerDBTesting(DB_URL):
            temp_dir = d
            single_results_file_path = "{record_identifier}_results.yaml"
            results_file_path = os.path.join(temp_dir, single_results_file_path)
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            n = 3

            for i in range_values[:n]:
                r_id = i[0]
                val = i[1]
                backend_data = {"record_identifier": r_id, "results_file_path": results_file_path}
                args.update(backend_data)
                psm = SamplePipestatManager(**args)
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            files = glob.glob(os.path.dirname(psm.file) + "**/*.yaml")
            assert len(files) == n

    @pytest.mark.parametrize("backend", ["file"])
    def test_multi_results_summarize(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with TemporaryDirectory() as d, ContextManagerDBTesting(DB_URL):
            temp_dir = d
            single_results_file_path = "{record_identifier}/results.yaml"
            results_file_path = os.path.join(temp_dir, single_results_file_path)
            args = dict(schema_path=recursive_schema_file_path, database_only=False)
            n = 3

            for i in range_values[:n]:
                r_id = i[0]
                val = i[1]
                backend_data = {"record_identifier": r_id, "results_file_path": results_file_path}
                args.update(backend_data)
                psm = SamplePipestatManager(**args)
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            psm.summarize()
            data = YAMLConfigManager(filepath=os.path.join(temp_dir, "aggregate_results.yaml"))
            assert r_id in data[psm.pipeline_name][psm.pipeline_type].keys()


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
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
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_with_index, database_only=False)
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
                psm.report(record_identifier=r_id, values=val, force_overwrite=True)

            mod = psm.backend.get_model(table_name=psm.backend.table_name)
            assert mod.md5sum.index is True
            assert mod.number_of_things.index is False
