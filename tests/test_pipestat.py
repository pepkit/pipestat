import pytest
import os
from tempfile import mkdtemp
from shutil import copyfile

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

    def test_unknown_backend(self, schema_file_path):
        """ either db config or results file path needs to be provided """
        with pytest.raises(MissingConfigDataError):
            PipestatManager(name="test", schema_path=schema_file_path)

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


class TestReporting:
    @pytest.mark.parametrize(
        ["rec_id", "res_id", "val"],
        [("sample1", "name_of_something", "test_name"),
         ("sample1", "number_of_things", 2),
         ("sample2", "number_of_things", 1),
         ("sample2", "percentage_of_things", 10.1),
         ("sample2", "name_of_something", "test_name")]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_basic(self, rec_id, res_id, val, config_file_path,
                          schema_file_path, results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.report(
            result_identifier=res_id,
            record_identifier=rec_id,
            value=val
        )
        assert rec_id in psm.data["test"]
        assert res_id in psm.data["test"][rec_id]
        if backend == "file":
            is_in_file(results_file_path, str(val))


class TestRemoval:
    @pytest.mark.parametrize(
        ["rec_id", "res_id", "val"],
        [("sample2", "number_of_things", 1)]
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_remove_basic_db(self, rec_id, res_id, val, config_file_path,
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
    def test_remove_record(self, rec_id, schema_file_path, config_file_path, results_file_path, backend):
        args = dict(schema_path=schema_file_path, name="test")
        backend_data = {"database_config": config_file_path} if backend == "db"\
            else {"results_file": results_file_path}
        args.update(backend_data)
        psm = PipestatManager(**args)
        psm.remove(record_identifier=rec_id)
        assert rec_id not in psm.data["test"]
