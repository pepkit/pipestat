import os.path
from collections.abc import Mapping

import pytest
from jsonschema import ValidationError

from pipestat import PipestatManager
from pipestat.const import *
from pipestat.exceptions import *
from pipestat.parsed_schema import ParsedSchema
from pipestat.helpers import default_formatter, markdown_formatter
from .conftest import (
    get_data_file_path,
    BACKEND_KEY_DB,
    BACKEND_KEY_FILE,
    COMMON_CUSTOM_STATUS_DATA,
    DEFAULT_STATUS_DATA,
    STANDARD_TEST_PIPE_ID,
    DB_URL,
)
from tempfile import NamedTemporaryFile

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
            psm = PipestatManager(**args)
            psm.report(sample_name=rec_id, values=val, force_overwrite=True)
            val_name = list(val.keys())[0]
            psm.set_status(status_identifier="running", sample_name=rec_id)
            status = psm.get_status(sample_name=rec_id)
            assert status == "running"
            assert val_name in psm.retrieve(sample_name=rec_id)
            psm.remove(sample_name=rec_id, result_identifier=val_name)
            if backend == "file":
                psm.clear_status(sample_name=rec_id)
                status = psm.get_status(sample_name=rec_id)
                assert status is None
                with pytest.raises(PipestatDataError):
                    psm.retrieve(sample_name=rec_id)
            if backend == "db":
                assert getattr(psm.retrieve(sample_name=rec_id), val_name, None) is None
                psm.remove(sample_name=rec_id)
                with pytest.raises(PipestatDatabaseError):
                    psm.retrieve(sample_name=rec_id)


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
            psm = PipestatManager(**args)
            psm.report(sample_name=rec_id, values=val, force_overwrite=True)
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
                    psm = PipestatManager(**args)

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
            psm = PipestatManager(**args)
            psm.report(sample_name=rec_id, values=val, force_overwrite=True)
            if backend == "file":
                assert rec_id in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL]
                assert (
                    list(val.keys())[0]
                    in psm.backend._data[STANDARD_TEST_PIPE_ID][PROJECT_SAMPLE_LEVEL][rec_id]
                )
                if backend == "file":
                    assert_is_in_files(results_file_path, str(list(val.values())[0]))
            if backend == "db":
                assert list(val.keys())[0] in psm.retrieve(sample_name=rec_id)

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
            psm = PipestatManager(**args)
            if success:
                psm.report(
                    sample_name=rec_id,
                    values=val,
                    strict_type=False,
                    force_overwrite=True,
                )
            else:
                with pytest.raises((ValidationError, TypeError)):
                    psm.report(
                        sample_name=rec_id,
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
            psm = PipestatManager(**args)
            results = psm.report(
                sample_name=rec_id, values=val, force_overwrite=True, result_formatter=formatter
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
            psm = PipestatManager(**args)
            psm.report(sample_name=rec_id, values=val, force_overwrite=True)
            retrieved_val = psm.retrieve(sample_name=rec_id, result_identifier=list(val.keys())[0])
            # Test Retrieve Basic
            assert str(retrieved_val) == str(list(val.values())[0])
            # Test Retrieve Whole Record
            assert isinstance(psm.retrieve(sample_name=rec_id), Mapping)

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
            psm = PipestatManager(**args)
            for k, v in val_dict.items():
                psm.report(sample_name=k, values=v, force_overwrite=True)
            if backend == "db":
                with pytest.raises(PipestatDatabaseError):
                    psm.retrieve(result_identifier=res_id, sample_name=rec_id)
            else:
                with pytest.raises(PipestatDataError):
                    psm.retrieve(result_identifier=res_id, sample_name=rec_id)


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
            psm = PipestatManager(**args)
            for v in vals:
                psm.report(sample_name=rec_id, values=v, force_overwrite=True)
            psm.remove(result_identifier=res_id, sample_name=rec_id)
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
            psm = PipestatManager(**args)
            for v in vals:
                psm.report(sample_name=rec_id, values=v, force_overwrite=True)
            psm.remove(sample_name=rec_id)
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
            psm = PipestatManager(**args)
            assert not psm.remove(sample_name=rec_id, result_identifier=res_id)

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
            psm = PipestatManager(**args)
            assert not psm.remove(sample_name=rec_id)

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
            psm = PipestatManager(**args)
            psm.report(sample_name=rec_id, values={res_id: "something"}, force_overwrite=True)
            assert psm.remove(sample_name=rec_id, result_identifier=res_id)
            if backend == "file":
                with pytest.raises(PipestatDataError):
                    psm.retrieve(sample_name=rec_id)


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
                sample_name=CONST_REC_ID,
                database_only=False,
            )
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = PipestatManager(**args)
            psm.report(values=val, pipeline_type=pipeline_type)
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
            args = dict(schema_path=schema_file_path, sample_name=CONST_REC_ID)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = PipestatManager(**args)
            psm.report(sample_name="constant_record_id", values=val, force_overwrite=True)
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
            args = dict(schema_path=schema_file_path, sample_name=CONST_REC_ID)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = PipestatManager(**args)
            psm.report(sample_name="constant_record_id", values=val, force_overwrite=True)
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
        psm = PipestatManager(
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
        monkeypatch.setenv(ENV_VARS["project_name"], STANDARD_TEST_PIPE_ID)
        monkeypatch.setenv(ENV_VARS["sample_name"], "sample1")
        monkeypatch.setenv(ENV_VARS["results_file"], results_file_path)
        monkeypatch.setenv(ENV_VARS["schema"], schema_file_path)
        try:
            PipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")

    # @pytest.mark.skip(reason="known failure for now with config file")
    def test_config__psm_is_built_from_config_file_env_var(self, monkeypatch, config_file_path):
        """PSM can be created from config parsed from env var value."""
        monkeypatch.setenv(ENV_VARS["config"], config_file_path)
        try:
            PipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")


def test_no_constructor_args__raises_expected_exception():
    """See Issue #3 in the repository."""
    with pytest.raises(SchemaNotFoundError):
        PipestatManager()


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
    psm = PipestatManager(schema_path=schema_file_path, **backend_data)
    assert psm.status_schema == exp_status_schema
    assert psm.status_schema_source == exp_status_schema_path


class TestHTMLReport:
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
        output_schema_html_report,
        results_file_path,
        backend,
    ):
        values_project = [
            {"sample2": {"number_of_things": 2}},
            {"sample3": {"name_of_something": "name of something string"}},
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
            psm = PipestatManager(**args)
            psm.report(
                sample_name=rec_id, values=val, force_overwrite=True, pipeline_type="project"
            )
            psm.set_status(
                sample_name=rec_id, status_identifier="completed", pipeline_type="project"
            )
            for i in values_project:
                for r, v in i.items():
                    psm.report(
                        sample_name=r, values=v, force_overwrite=True, pipeline_type="project"
                    )
                    psm.set_status(
                        sample_name=r, status_identifier="running", pipeline_type="project"
                    )
            for i in values_sample:
                for r, v in i.items():
                    psm.report(
                        sample_name=r, values=v, force_overwrite=True, pipeline_type="sample"
                    )
                    psm.set_status(sample_name=r, status_identifier="running")

            htmlreportpath = psm.summarize(amendment="")
