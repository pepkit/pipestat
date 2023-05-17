import os.path
from collections.abc import Mapping

import pytest
from jsonschema import ValidationError

from pipestat import PipestatManager
from pipestat.const import *
from pipestat.exceptions import *
from pipestat.parsed_schema import ParsedSchema
from .conftest import (
    get_data_file_path,
    BACKEND_KEY_DB,
    BACKEND_KEY_FILE,
    COMMON_CUSTOM_STATUS_DATA,
    DEFAULT_STATUS_DATA,
    STANDARD_TEST_PIPE_ID,
)

CONST_REC_ID = "constant_record_id"


def assert_is_in_files(fs, s):
    """
    Verify if string is in files content

    :param str | Iterable[str] fs: list of files
    :param str s: string to look for
    """
    for f in [fs] if isinstance(fs, str) else fs:
        with open(f, "r") as fh:
            assert s in fh.read()


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
        args = dict(schema_path=schema_file_path, database_only=False)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
        print(psm.data[STANDARD_TEST_PIPE_ID])
        print("Test if", rec_id, " is in ", psm.data[STANDARD_TEST_PIPE_ID])

        print(backend)
        assert rec_id in psm.data[STANDARD_TEST_PIPE_ID]
        print("Test if", list(val.keys())[0], " is in ", rec_id)
        assert list(val.keys())[0] in psm.data[STANDARD_TEST_PIPE_ID][rec_id]
        if backend == "file":
            assert_is_in_files(results_file_path, str(list(val.values())[0]))

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
        args = dict()
        backend_data = (
            {"config": config_no_schema_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
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
        args = dict(schema_path=schema_file_path, database_only=False)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
        assert rec_id in psm.data[STANDARD_TEST_PIPE_ID]
        assert list(val.keys())[0] in psm.data[STANDARD_TEST_PIPE_ID][rec_id]
        if backend == "file":
            assert_is_in_files(results_file_path, str(list(val.values())[0]))

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
        args = dict(schema_path=schema_file_path)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
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
        args = dict(schema_path=schema_file_path)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        retrieved_val = psm.retrieve(
            record_identifier=rec_id, result_identifier=list(val.keys())[0]
        )
        assert str(retrieved_val) == str(list(val.values())[0])

    @pytest.mark.parametrize("rec_id", ["sample1", "sample2"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_whole_record(
        self, rec_id, config_file_path, results_file_path, schema_file_path, backend
    ):
        args = dict(schema_path=schema_file_path)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert isinstance(psm.retrieve(record_identifier=rec_id), Mapping)

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
        args = dict(schema_path=schema_file_path)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        with pytest.raises(PipestatDatabaseError):
            psm.retrieve(result_identifier=res_id, record_identifier=rec_id)


class TestRemoval:
    @pytest.mark.parametrize(
        ["rec_id", "res_id", "val"], [("sample2", "number_of_things", 1)]
    )
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
        args = dict(schema_path=schema_file_path, database_only=False)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(result_identifier=res_id, record_identifier=rec_id)
        if backend != "db":
            assert res_id not in psm.data[STANDARD_TEST_PIPE_ID][rec_id]
        else:
            # TODO write something here to query db
            pass

    @pytest.mark.parametrize("rec_id", ["sample1", "sample2"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend
    ):
        args = dict(schema_path=schema_file_path, database_only=False)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(record_identifier=rec_id)
        if backend != "db":
            assert rec_id not in psm.data[STANDARD_TEST_PIPE_ID]
        else:
            # TODO write something here to query db
            pass


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
        args = dict(schema_path=schema_file_path)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert not psm.remove(record_identifier=rec_id, result_identifier=res_id)

    @pytest.mark.parametrize("rec_id", ["nonexistent", "bogus"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_nonexistent_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend
    ):
        args = dict(schema_path=schema_file_path)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
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
        args = dict(schema_path=schema_file_path, database_only=False)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert psm.remove(record_identifier=rec_id, result_identifier=res_id)
        assert rec_id not in psm.data


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
    @pytest.mark.parametrize("project_level", [False, True])
    def test_report(
        self,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
        project_level,
    ):
        args = dict(
            schema_path=schema_file_path,
            record_identifier=CONST_REC_ID,
            database_only=False,
        )
        # TODO: combine this with the fixture in conftest.py
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(values=val, project_level=project_level)
        assert CONST_REC_ID in psm.data[STANDARD_TEST_PIPE_ID]
        assert list(val.keys())[0] in psm.data[STANDARD_TEST_PIPE_ID][CONST_REC_ID]
        if backend == "file":
            assert_is_in_files(results_file_path, str(list(val.values())[0]))

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve(
        self, val, config_file_path, schema_file_path, results_file_path, backend
    ):
        args = dict(schema_path=schema_file_path, record_identifier=CONST_REC_ID)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
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
    def test_remove(
        self, val, config_file_path, schema_file_path, results_file_path, backend
    ):
        args = dict(schema_path=schema_file_path, record_identifier=CONST_REC_ID)
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert psm.remove(result_identifier=list(val.keys())[0])


@pytest.mark.xfail(reason="need to re-implement logic for highlighting status")
def test_highlighting_works(highlight_schema_file_path, results_file_path):
    """the highlighted results are sourced from the schema and only ones
    that are indicated with 'highlight: true` are respected"""
    s = ParsedSchema(highlight_schema_file_path)
    schema_highlighted_results = [
        k for k, v in s.project_level_data.items() if v.get("highlight", False)
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
        monkeypatch.setenv(ENV_VARS["namespace"], STANDARD_TEST_PIPE_ID)
        monkeypatch.setenv(ENV_VARS["record_identifier"], "sample1")
        monkeypatch.setenv(ENV_VARS["results_file"], results_file_path)
        monkeypatch.setenv(ENV_VARS["schema"], schema_file_path)
        try:
            PipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")

    def test_config__psm_is_built_from_config_file_env_var(
        self, monkeypatch, config_file_path
    ):
        """PSM can be created from config parsed from env var value."""
        monkeypatch.setenv(ENV_VARS["config"], config_file_path)
        try:
            PipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")


def test_no_constructor_args__raises_expected_exception():
    """See Issue #3 in the repository."""
    with pytest.raises(NoBackendSpecifiedError):
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
@pytest.mark.parametrize(
    "backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True
)
def test_manager_has_correct_status_schema_and_status_schema_source(
    schema_file_path, exp_status_schema, exp_status_schema_path, backend_data
):
    psm = PipestatManager(schema_path=schema_file_path, **backend_data)
    assert psm.status_schema == exp_status_schema
    assert psm.status_schema_source == exp_status_schema_path
