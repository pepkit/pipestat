import pytest
import os
from tempfile import mkdtemp
from yaml import dump
from collections import Mapping
from jsonschema import ValidationError

from pipestat.exceptions import *
from pipestat import PipestatManager


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
        with open(f, 'r') as fh:
            if reverse:
                assert s not in fh.read()
            else:
                assert s in fh.read()


class TestPipestatManagerInstantiation:
    def test_obj_creation_file(self, schema_file_path, results_file_path):
        """ Object constructor works with file as backend"""
        assert isinstance(
            PipestatManager(
                name="test",
                results_file=results_file_path,
                schema_path=schema_file_path
            ), PipestatManager)

    def test_obj_creation_db(self, schema_file_path, config_file_path):
        """ Object constructor works with database as backend"""
        assert isinstance(
            PipestatManager(
                name="test",
                database_config=config_file_path,
                schema_path=schema_file_path
            ), PipestatManager)

    def test_schema_req_for_db(self, config_file_path):
        """
        Object constructor raises exception if schema is not provided
        with database configuration file
        """
        with pytest.raises(SchemaNotFoundError):
            PipestatManager(
                name="test",
                database_config=config_file_path
            )

    def test_missing_cfg_data(self, schema_file_path):
        """ Object constructor raises exception if cfg is missing data """
        tmp_pth = os.path.join(mkdtemp(), "res.yml")
        with open(tmp_pth, 'w') as file:
            dump({"database": {"host": "localhost"}}, file)
        with pytest.raises(MissingConfigDataError):
            PipestatManager(
                name="test",
                database_config=tmp_pth,
                schema_path=schema_file_path
            )

    def test_unknown_backend(self, schema_file_path):
        """ Either db config or results file path needs to be provided """
        with pytest.raises(MissingConfigDataError):
            PipestatManager(name="test", schema_path=schema_file_path)

    def test_create_results_file(self, schema_file_path):
        """ Results file is created if a nonexistent path provided """
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        PipestatManager(
            name="test",
            results_file=tmp_res_file,
            schema_path=schema_file_path
        )
        assert os.path.exists(tmp_res_file)

    def test_use_other_namespace_file(self, schema_file_path):
        """ Results file can be used with just one namespace """
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        PipestatManager(
            name="test",
            results_file=tmp_res_file,
            schema_path=schema_file_path
        )
        assert os.path.exists(tmp_res_file)
        with pytest.raises(PipestatDatabaseError):
            PipestatManager(
                name="new_test",
                results_file=tmp_res_file,
                schema_path=schema_file_path
            )

    @pytest.mark.parametrize("pth", [["/$HOME/path.yaml"], 1])
    def test_wrong_class_results_file(self, schema_file_path, pth):
        """ Input string that is not a file path raises an informative error """
        with pytest.raises(TypeError):
            PipestatManager(
                name="test",
                results_file=pth,
                schema_path=schema_file_path
            )

    def test_results_file_contents_loaded(
            self, results_file_path, schema_file_path):
        """ Contents of the results file are present after loading """
        psm = PipestatManager(
            name="test",
            results_file=results_file_path,
            schema_path=schema_file_path
        )
        assert "test" in psm.data

    def test_str_representation(
            self, results_file_path, schema_file_path):
        """ Test string representation identifies number of records """
        psm = PipestatManager(
            name="test",
            results_file=results_file_path,
            schema_path=schema_file_path
        )
        assert f"Records count: {len(psm.data[psm.name])}" in str(psm)


class TestReporting:
    def test_report_requires_schema(self, results_file_path):
        """ Report fails if schema not provided, even for a file backend """
        psm = PipestatManager(name="test", results_file=results_file_path)
        with pytest.raises(SchemaNotFoundError):
            psm.report(
                record_identifier="sample1",
                values={"name_of_something": "test_name"}
            )

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [("sample1", {"name_of_something": "test_name"}),
         ("sample1", {"number_of_things": 1}),
         ("sample2", {"number_of_things": 2}),
         ("sample2", {"percentage_of_things": 10.1}),
         ("sample2", {"name_of_something": "test_name"}),
         ("sample3", {"name_of_something": "test_name"})]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_basic(self, rec_id, val, config_file_path,
                          schema_file_path, results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(
            record_identifier=rec_id,
            values=val
        )
        assert rec_id in psm.data["test"]
        assert list(val.keys())[0] in psm.data["test"][rec_id]
        if backend == "file":
            is_in_file(results_file_path, str(list(val.values())[0]))

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [("sample1", {"number_of_things": 2}),
         ("sample2", {"number_of_things": 1})]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_overwrite(self, rec_id, val, config_file_path,
                          schema_file_path, results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(
            record_identifier=rec_id,
            values=val,
            force_overwrite=True
        )
        assert rec_id in psm.data["test"]
        assert list(val.keys())[0] in psm.data["test"][rec_id]
        if backend == "file":
            is_in_file(results_file_path, str(list(val.values())[0]))

    @pytest.mark.parametrize(
        ["rec_id", "val", "success"],
        [("sample1", {"number_of_things": "2"}, True),
         ("sample2", {"number_of_things": [1, 2, 3]}, False),
         ("sample2", {"output_file": {"path": 1, "title": "abc"}}, True)]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_type_casting(
            self, rec_id, val, config_file_path, schema_file_path,
            results_file_path, backend, success):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        if success:
            psm.report(
                record_identifier=rec_id,
                values=val,
                strict_type=False,
                force_overwrite=True
            )
        else:
            with pytest.raises((ValidationError, TypeError)):
                psm.report(
                    record_identifier=rec_id,
                    values=val,
                    strict_type=False,
                    force_overwrite=True
                )


class TestRetrieval:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [("sample1", {"name_of_something": "test_name"}),
         ("sample1", {"number_of_things": 2}),
         ("sample2", {"number_of_things": 1}),
         ("sample2", {"percentage_of_things": 10.1}),
         ("sample2", {"name_of_something": "test_name"})]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_basic(self, rec_id, val, config_file_path,
                            results_file_path, schema_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        retrieved_val = psm.retrieve(
            record_identifier=rec_id,
            result_identifier=list(val.keys())[0]
        )
        assert str(retrieved_val) == str(list(val.values())[0])

    @pytest.mark.parametrize("rec_id", ["sample1", "sample2"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_whole_record(
            self, rec_id, config_file_path, results_file_path,
            schema_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert isinstance(psm.retrieve(record_identifier=rec_id), Mapping)

    @pytest.mark.parametrize(
        ["rec_id", "res_id"],
        [("nonexistent", "name_of_something"), ("sample1", "nonexistent")]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve_nonexistent(
            self, rec_id, res_id, config_file_path, results_file_path,
            schema_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        with pytest.raises(PipestatDatabaseError):
            psm.retrieve(
                result_identifier=res_id,
                record_identifier=rec_id
            )


class TestRemoval:
    @pytest.mark.parametrize(
        ["rec_id", "res_id", "val"],
        [("sample2", "number_of_things", 1)]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_basic(self, rec_id, res_id, val, config_file_path,
                             results_file_path, schema_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(
            result_identifier=res_id,
            record_identifier=rec_id
        )
        assert res_id not in psm.data["test"][rec_id]

    @pytest.mark.parametrize("rec_id", ["sample1", "sample2"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_record(
            self, rec_id, schema_file_path, config_file_path,
            results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(record_identifier=rec_id)
        assert rec_id not in psm.data["test"]

    @pytest.mark.parametrize(
        ["rec_id", "res_id"],
        [("sample2", "nonexistent"),
         ("sample2", "bogus")]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_nonexistent_result(
            self, rec_id, res_id, schema_file_path, config_file_path,
            results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert not psm.remove(
            record_identifier=rec_id,
            result_identifier=res_id
        )

    @pytest.mark.parametrize("rec_id", ["nonexistent", "bogus"])
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_nonexistent_record(
            self, rec_id, schema_file_path, config_file_path,
            results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert not psm.remove(record_identifier=rec_id)

    @pytest.mark.parametrize(
        ["rec_id", "res_id"],
        [("sample3", "name_of_something")]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_last_result_removal_removes_record(
            self, rec_id, res_id, schema_file_path, config_file_path,
            results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert psm.remove(
            record_identifier=rec_id,
            result_identifier=res_id
        )
        assert rec_id not in psm.data


class TestNoRecordID:
    @pytest.mark.parametrize("val",
        [{"name_of_something": "test_name"},
         {"number_of_things": 1},
         {"percentage_of_things": 10.1}]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report(self, val, config_file_path, schema_file_path,
                          results_file_path, backend):
        REC_ID = "constant_record_id"
        args = dict(schema_path=schema_file_path, name="test",
                    record_identifier=REC_ID)
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(values=val)
        assert REC_ID in psm.data["test"]
        assert list(val.keys())[0] in psm.data["test"][REC_ID]
        if backend == "file":
            is_in_file(results_file_path, str(list(val.values())[0]))

    @pytest.mark.parametrize("val",
        [{"name_of_something": "test_name"},
         {"number_of_things": 1},
         {"percentage_of_things": 10.1}]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_retrieve(self, val, config_file_path, schema_file_path,
                      results_file_path, backend):
        REC_ID = "constant_record_id"
        args = dict(schema_path=schema_file_path, name="test",
                    record_identifier=REC_ID)
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        retrieved_val = psm.retrieve(result_identifier=list(val.keys())[0])
        assert str(retrieved_val) == str(list(val.values())[0])

    @pytest.mark.parametrize("val",
        [{"name_of_something": "test_name"},
         {"number_of_things": 1},
         {"percentage_of_things": 10.1}]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove(self, val, config_file_path, schema_file_path,
                    results_file_path, backend):
        REC_ID = "constant_record_id"
        args = dict(schema_path=schema_file_path, name="test",
                    record_identifier=REC_ID)
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        assert psm.remove(result_identifier=list(val.keys())[0])


class TestDatabaseOnly:
    @pytest.mark.parametrize("val",
        [{"name_of_something": "test_name"},
         {"number_of_things": 1},
         {"percentage_of_things": 10.1}]
    )
    def test_report(self, val, config_file_path, schema_file_path,
                          results_file_path):
        REC_ID = "constant_record_id"
        psm = PipestatManager(schema_path=schema_file_path, name="test",
                    record_identifier=REC_ID, database_only=True,
                    database_config=config_file_path)
        psm.report(values=val)
        assert len(psm.data) == 0
        val_name = list(val.keys())[0]
        assert psm.select(
            condition=val_name + "=%s", condition_val=[str(val[val_name])])
