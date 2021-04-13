import os
from collections import Mapping
from tempfile import mkdtemp

import pytest
from _pytest.monkeypatch import monkeypatch
from jsonschema import ValidationError
from psycopg2 import Error as psycopg2Error
from yaml import dump

from pipestat import PipestatManager
from pipestat.const import *
from pipestat.exceptions import *
from pipestat.helpers import read_yaml_data


def is_in_file(fs, s, reverse=False):
    """
    Verify if string is in files content
    :param str | Iterable[str] fs: list of files
    :param str s: string to look for
    :param bool reverse: whether the reverse should be checked
    """
    if isinstance(fs, str):
        fs = [fs]
    for f in fs:
        with open(f, "r") as fh:
            if reverse:
                assert s not in fh.read()
            else:
                assert s in fh.read()


class TestConnection:
    def test_connection_checker(self, config_file_path, schema_file_path):
        pm = PipestatManager(
            config=config_file_path,
            database_only=True,
            schema_path=schema_file_path,
            namespace="test",
        )
        assert pm.is_db_connected()

    def test_connection_overwrite_error(self, config_file_path, schema_file_path):
        pm = PipestatManager(
            config=config_file_path,
            database_only=True,
            schema_path=schema_file_path,
            namespace="test",
        )
        with pytest.raises(PipestatDatabaseError):
            pm.establish_db_connection()


class TestPipestatManagerInstantiation:
    def test_obj_creation_file(self, schema_file_path, results_file_path):
        """ Object constructor works with file as backend"""
        assert isinstance(
            PipestatManager(
                namespace="test",
                results_file_path=results_file_path,
                schema_path=schema_file_path,
            ),
            PipestatManager,
        )

    def test_obj_creation_db(self, config_file_path):
        """ Object constructor works with database as backend"""
        assert isinstance(PipestatManager(config=config_file_path), PipestatManager)

    @pytest.mark.xfail(reason="schema is no longer required to init the object")
    def test_schema_req(self, results_file_path):
        """
        Object constructor raises exception if schema is not provided
        """
        with pytest.raises(PipestatError):
            PipestatManager(namespace="test", results_file_path=results_file_path)

    def test_schema_recursive_custom_type_conversion(
        self, recursive_schema_file_path, results_file_path
    ):
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=recursive_schema_file_path,
        )
        assert (
            "path"
            in psm.result_schemas["output_file_in_object"]["properties"]["prop1"][
                "properties"
            ]
        )
        assert (
            "thumbnail_path"
            in psm.result_schemas["output_file_in_object"]["properties"]["prop2"][
                "properties"
            ]
        )

    def test_missing_cfg_data(self, schema_file_path):
        """ Object constructor raises exception if cfg is missing data """
        tmp_pth = os.path.join(mkdtemp(), "res.yml")
        with open(tmp_pth, "w") as file:
            dump({"database": {"host": "localhost"}}, file)
        with pytest.raises(MissingConfigDataError):
            PipestatManager(
                namespace="test", config=tmp_pth, schema_path=schema_file_path
            )

    def test_unknown_backend(self, schema_file_path):
        """ Either db config or results file path needs to be provided """
        with pytest.raises(MissingConfigDataError):
            PipestatManager(namespace="test", schema_path=schema_file_path)

    def test_create_results_file(self, schema_file_path):
        """ Results file is created if a nonexistent path provided """
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        PipestatManager(
            namespace="test",
            results_file_path=tmp_res_file,
            schema_path=schema_file_path,
        )
        assert os.path.exists(tmp_res_file)

    def test_use_other_namespace_file(self, schema_file_path):
        """ Results file can be used with just one namespace """
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        PipestatManager(
            namespace="test",
            results_file_path=tmp_res_file,
            schema_path=schema_file_path,
        )
        assert os.path.exists(tmp_res_file)
        with pytest.raises(PipestatDatabaseError):
            PipestatManager(
                namespace="new_test",
                results_file_path=tmp_res_file,
                schema_path=schema_file_path,
            )

    @pytest.mark.parametrize("pth", [["/$HOME/path.yaml"], 1])
    def test_wrong_class_results_file(self, schema_file_path, pth):
        """ Input string that is not a file path raises an informative error """
        with pytest.raises((TypeError, AssertionError)):
            PipestatManager(
                namespace="test", results_file_path=pth, schema_path=schema_file_path
            )

    def test_results_file_contents_loaded(self, results_file_path, schema_file_path):
        """ Contents of the results file are present after loading """
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=schema_file_path,
        )
        assert "test" in psm.data

    def test_str_representation(self, results_file_path, schema_file_path):
        """ Test string representation identifies number of records """
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=schema_file_path,
        )
        assert f"Records count: {len(psm.data[psm.namespace])}" in str(psm)


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
        args = dict(schema_path=schema_file_path, namespace="test")
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(record_identifier=rec_id, values=val)
        assert rec_id in psm.data["test"]
        assert list(val.keys())[0] in psm.data["test"][rec_id]
        if backend == "file":
            is_in_file(results_file_path, str(list(val.values())[0]))

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
        args = dict(namespace="test")
        backend_data = (
            {"config": config_no_schema_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        if backend == "db":
            with pytest.raises(SchemaNotFoundError):
                psm = PipestatManager(**args)
        else:
            psm = PipestatManager(**args)
        if backend == "file":
            with pytest.raises(SchemaNotFoundError):
                psm.report(record_identifier=rec_id, values=val)

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
        args = dict(schema_path=schema_file_path, namespace="test")
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
        assert rec_id in psm.data["test"]
        assert list(val.keys())[0] in psm.data["test"][rec_id]
        if backend == "file":
            is_in_file(results_file_path, str(list(val.values())[0]))

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
        args = dict(schema_path=schema_file_path, namespace="test")
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
        args = dict(schema_path=schema_file_path, namespace="test")
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
        args = dict(schema_path=schema_file_path, namespace="test")
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
        args = dict(schema_path=schema_file_path, namespace="test")
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
        args = dict(schema_path=schema_file_path, namespace="test")
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(result_identifier=res_id, record_identifier=rec_id)
        assert res_id not in psm.data["test"][rec_id]

    @pytest.mark.parametrize("rec_id", ["sample1", "sample2"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend
    ):
        args = dict(schema_path=schema_file_path, namespace="test")
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(record_identifier=rec_id)
        assert rec_id not in psm.data["test"]

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
        args = dict(schema_path=schema_file_path, namespace="test")
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
        args = dict(schema_path=schema_file_path, namespace="test")
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
        args = dict(schema_path=schema_file_path, namespace="test")
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
    def test_report(
        self, val, config_file_path, schema_file_path, results_file_path, backend
    ):
        REC_ID = "constant_record_id"
        args = dict(
            schema_path=schema_file_path, namespace="test", record_identifier=REC_ID
        )
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(values=val)
        assert REC_ID in psm.data["test"]
        assert list(val.keys())[0] in psm.data["test"][REC_ID]
        if backend == "file":
            is_in_file(results_file_path, str(list(val.values())[0]))

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
        REC_ID = "constant_record_id"
        args = dict(
            schema_path=schema_file_path, namespace="test", record_identifier=REC_ID
        )
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
        REC_ID = "constant_record_id"
        args = dict(
            schema_path=schema_file_path, namespace="test", record_identifier=REC_ID
        )
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert psm.remove(result_identifier=list(val.keys())[0])


class TestDatabaseOnly:
    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    def test_report(self, val, config_file_path, schema_file_path, results_file_path):
        REC_ID = "constant_record_id"
        psm = PipestatManager(
            schema_path=schema_file_path,
            namespace="test",
            record_identifier=REC_ID,
            database_only=True,
            config=config_file_path,
        )
        psm.report(values=val)
        assert len(psm.data) == 0
        val_name = list(val.keys())[0]
        assert psm.select(
            condition=val_name + "=%s", condition_val=[str(val[val_name])]
        )

    @pytest.mark.parametrize(["rec_id", "res_id"], [("sample2", "number_of_things")])
    @pytest.mark.parametrize("backend", ["db"])
    @pytest.mark.parametrize("limit", [1, 2, 3, 15555])
    def test_select_limit(
        self,
        rec_id,
        res_id,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
        limit,
    ):
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        result = psm.select(
            condition=f"{RECORD_ID}=%s",
            condition_val=[rec_id],
            columns=[res_id],
            limit=limit,
        )
        assert len(result) <= limit

    @pytest.mark.parametrize("backend", ["db"])
    @pytest.mark.parametrize("offset", [0, 1, 2, 3, 15555])
    def test_select_offset(
        self, config_file_path, results_file_path, schema_file_path, backend, offset
    ):
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        result = psm.select(offset=offset)
        print(result)
        assert len(result) == max((psm.record_count - offset), 0)

    @pytest.mark.parametrize("backend", ["db"])
    @pytest.mark.parametrize(
        ["offset", "limit"], [(0, 0), (0, 1), (0, 2), (0, 11111), (1, 1), (1, 0)]
    )
    def test_select_pagination(
        self,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
        offset,
        limit,
    ):
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        result = psm.select(offset=offset, limit=limit)
        print(result)
        assert len(result) == min(max((psm.record_count - offset), 0), limit)


class TestHighlighting:
    def test_highlighting_works(self, highlight_schema_file_path, results_file_path):
        """the highlighted results are sourced from the schema and only ones
        that are indicated with 'highlight: true` are respected"""
        _, s = read_yaml_data(highlight_schema_file_path, "schema")
        schema_highlighted_results = [
            k for k, v in s.items() if ("highlight" in v and v["highlight"] == True)
        ]
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=highlight_schema_file_path,
        )
        assert psm.highlighted_results == schema_highlighted_results


class TestStatus:
    def test_status_file_defult_location(self, schema_file_path, results_file_path):
        """status file location is set to the results file dir
        if not specified"""
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=schema_file_path,
        )
        assert psm[STATUS_FILE_DIR] == os.path.dirname(psm.file)

    @pytest.mark.parametrize("backend", ["file", "db"])
    @pytest.mark.parametrize("status_id", ["running", "failed", "completed"])
    def test_status_not_configured(
        self, schema_file_path, config_file_path, results_file_path, backend, status_id
    ):
        """ status management works even in case it has not been configured"""
        args = dict(schema_path=schema_file_path, namespace="test")
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.set_status(record_identifier="sample1", status_identifier=status_id)
        assert psm.get_status(record_identifier="sample1") == status_id

    @pytest.mark.parametrize("backend", ["file", "db"])
    @pytest.mark.parametrize(
        "status_id", ["running_custom", "failed_custom", "completed_custom"]
    )
    def test_custom_status_schema(
        self,
        schema_file_path,
        config_file_path,
        results_file_path,
        backend,
        status_id,
        custom_status_schema,
    ):
        """ status management works even in case it has not been configured"""
        args = dict(
            schema_path=schema_file_path,
            namespace="test",
            status_schema_path=custom_status_schema,
        )
        backend_data = (
            {"config": config_file_path}
            if backend == "db"
            else {"results_file_path": results_file_path}
        )
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.set_status(record_identifier="sample1", status_identifier=status_id)
        assert psm.get_status(record_identifier="sample1") == status_id


class TestEnvVars:
    def test_no_config(self, monkeypatch, results_file_path, schema_file_path):
        """
        test that the object can be created if the arguments
        are provided as env vars
        """
        monkeypatch.setenv(ENV_VARS["namespace"], "test")
        monkeypatch.setenv(ENV_VARS["record_identifier"], "sample1")
        monkeypatch.setenv(ENV_VARS["results_file"], results_file_path)
        monkeypatch.setenv(ENV_VARS["schema"], schema_file_path)
        PipestatManager()

    def test_config(self, monkeypatch, config_file_path):
        """
        test that the object can be created if the arguments are
        provided in a config that is provided as env vars
        """
        monkeypatch.setenv(ENV_VARS["config"], config_file_path)
        PipestatManager()
