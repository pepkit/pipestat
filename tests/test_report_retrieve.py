import os.path
from collections.abc import Mapping
from tempfile import NamedTemporaryFile

import pytest
from jsonschema import ValidationError

from pipestat import (
    PipestatManager,
    ProjectPipestatManager,
    SamplePipestatManager,
)
from pipestat.const import *
from pipestat.exceptions import *
from pipestat.helpers import default_formatter, markdown_formatter
from pipestat.parsed_schema import ParsedSchema

from .conftest import (
    BACKEND_KEY_DB,
    BACKEND_KEY_FILE,
    COMMON_CUSTOM_STATUS_DATA,
    DB_DEPENDENCIES,
    DB_URL,
    DEFAULT_STATUS_DATA,
    REC_ID,
    SERVICE_UNAVAILABLE,
    STANDARD_TEST_PIPE_ID,
    get_data_file_path,
)
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


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
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
    def test_basics_all(
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
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)
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
                psm.remove_record(record_identifier=rec_id, rm_record=True)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)
            if backend == "db":
                assert psm.retrieve_one(record_identifier=rec_id).get(val_name, None) is None
                psm.remove(record_identifier=rec_id)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)

    @pytest.mark.parametrize("backend", ["file"])
    def test_similar_record_ids(
        self,
        config_file_path,
        schema_file_path,
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

            #####
            rec_id1 = "sample1"
            rec_id2 = "sample"

            val = {"name_of_something": "ABCDEFG"}
            psm.report(record_identifier=rec_id1, values=val)

            val = {"name_of_something": "HIJKLMOP"}
            psm.report(record_identifier=rec_id2, values=val)

            result1 = psm.retrieve_one(
                record_identifier=rec_id1, result_identifier="name_of_something"
            )
            result2 = psm.retrieve_one(
                record_identifier=rec_id2, result_identifier="name_of_something"
            )

            assert result1 == "ABCDEFG"
            assert result2 == "HIJKLMOP"

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
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = ProjectPipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)
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
                # with pytest.raises(RecordNotFoundError):
                #     psm.retrieve_one(record_identifier=rec_id)
            if backend == "db":
                psm.remove(record_identifier=rec_id)
                with pytest.raises(RecordNotFoundError):
                    psm.retrieve_one(record_identifier=rec_id)


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
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
            args = dict(schema_path=schema_file_path, pipeline_type=pipeline_type)
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
                        strict_type=False,
                    )

            if pipeline_type == "sample":
                if val_name in psm.cfg[SCHEMA_KEY].sample_level_data:
                    assert psm.report(
                        record_identifier="constant_record_id",
                        values=val,
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
    @pytest.mark.parametrize("backend", ["file"])
    def test_complex_object_report(
        self, val, config_file_path, recursive_schema_file_path, results_file_path, backend
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)

            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=REC_ID, values=val)
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
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
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
            args = dict(schema_path=schema_file_path)
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
                    SamplePipestatManager(**args)

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
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            # Report some other value to be overwritten
            psm.report(record_identifier=rec_id, values={list(val.keys())[0]: 1000})
            # Now overwrite
            psm.report(record_identifier=rec_id, values=val)
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
                )
            else:
                with pytest.raises((ValidationError, TypeError)):
                    psm.report(
                        record_identifier=rec_id,
                        values=val,
                        strict_type=False,
                    )

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
            ("sample2", {"number_of_things": 2}),
            ("sample3", {"switch_value": True}),
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
            args = dict(schema_path=schema_file_path)
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
                result_formatter=formatter,
            )
            assert rec_id in results[0]
            value = list(val.keys())[0]
            assert value in results[0]


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
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
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)
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
    def test_retrieve_basic_no_record_identifier(
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
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            args.update(record_identifier=rec_id)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)
            assert (
                psm.retrieve_one(result_identifier=list(val.keys())[0]) == list(val.items())[0][1]
            )

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"number_of_things": 2}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_one_single_result_as_list(
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
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)
            assert psm.retrieve_one(
                record_identifier=rec_id, result_identifier="number_of_things"
            ) == psm.retrieve_one(record_identifier=rec_id, result_identifier=["number_of_things"])

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
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val)

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
                psm.report(record_identifier=k, values=v)

            results = psm.select_records()

            assert results["records"][0]["record_identifier"] == list(val_dict.keys())[0]
            assert results["total_size"] == len(list(val_dict.keys()))
            results = psm.select_records(limit=1)
            assert len(results["records"]) == 1

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
                psm.report(record_identifier=k, values=v)

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
                assert len(result["records"]) == 0 or res_id not in result["records"][0]


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
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
            psm.report(record_identifier="constant_record_id", values=val)
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
            psm.report(record_identifier="constant_record_id", values=val)
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
@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
@pytest.mark.parametrize("backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True)
def test_manager_has_correct_status_schema_and_status_schema_source(
    schema_file_path, exp_status_schema, exp_status_schema_path, backend_data
):
    with ContextManagerDBTesting(DB_URL):
        psm = SamplePipestatManager(schema_path=schema_file_path, **backend_data)
        assert psm.cfg[STATUS_SCHEMA_KEY] == exp_status_schema
        assert psm.cfg[STATUS_SCHEMA_SOURCE_KEY] == exp_status_schema_path
