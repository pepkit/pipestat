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
         ("sample2", "number_of_things", 1),
         ("sample2", "name_of_something", "test_name")]
    )
    def test_report_basic_file(
            self, rec_id, res_id, val, results_file_path, schema_file_path):
        temp_file = os.path.join(mkdtemp(), os.path.basename(results_file_path))
        copyfile(results_file_path, temp_file)
        psm = PipestatManager(
            schema_path=schema_file_path,
            results_file=temp_file,
            name="test"
        )
        psm.report(
            result_identifier=res_id,
            record_identifier=rec_id,
            value=val
        )
        assert rec_id in psm.data["test"]
        assert res_id in psm.data["test"][rec_id]
        is_in_file(temp_file, str(val))

    # @pytest.mark.parametrize(["id", "type", "value"],
    #                          [("id1", "string", "test"),
    #                           ("id2", "integer", 1),
    #                           ("id4", "array", [1, 2, 3]),
    #                           ("id3", "object", {"test": "val"})])
    # def test_report_basic_file(self, id, type, value, results_file_path):
    #     temp_db = os.path.join(mkdtemp(), os.path.basename(results_file_path))
    #     copyfile(results_file_path, temp_db)
    #     psm = PipestatManager(temp_db, "test")
    #     psm.report(id, type, value)
    #     assert psm.database["test"][id]["value"] == value
    #     if not (isinstance(value, list) or isinstance(value, dict)):
    #         # arrays and objects are represented differently in yamls
    #         is_in_file(temp_db, str(value))
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id1", ["string"], "test"),
#                               ("id2", "test", 1),
#                               ("id4", "type", [1, 2, 3]),
#                               ("id3", "aaa", {"test": "val"})])
#     def test_invalid_type_error(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         with pytest.raises(InvalidTypeError):
#             psm.report(id, type, value)
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id1", "string", 1),
#                               ("id2", "integer", "1"),
#                               ("id4", "float", "1")])
#     def test_val_class_conversion(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.report(id, type, value, strict_type=True)
#         assert isinstance(psm.database["test"][id]["value"],
#                           CLASSES_BY_TYPE[type])
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id2", "integer", 1),
#                               ("id4", "float", 2.0)])
#     def test_val_no_overwrite(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.report(id, type, value)
#         value = value + 1
#         psm.report(id, type, value)
#         assert value != psm.database["test"][id]["value"]
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id2", "integer", 1),
#                               ("id4", "float", 2.0)])
#     def test_val_overwrite(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.report(id, type, value)
#         value = value + 1
#         psm.report(id, type, value, force_overwrite=True)
#         assert value == psm.database["test"][id]["value"]
#
#
# class TestRemoval:
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id1", "string", "test"),
#                               ("id2", "integer", 1),
#                               ("id4", "array", [1, 2, 3]),
#                               ("id3", "object", {"test": "val"})])
#     def test_ramoval(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.report(id, type, value)
#         psm.remove(id)
#         assert id not in psm.database["test"]
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id1", "string", "test"),
#                               ("id2", "integer", 1),
#                               ("id4", "array", [1, 2, 3]),
#                               ("id3", "object", {"test": "val"})])
#     def test_ramoval_cache(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.report(id, type, value, cache=True)
#         assert id in psm.cache["test"]
#         psm.remove(id)
#         assert "test" not in psm.cache
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id1", "string", "test"),
#                               ("id2", "integer", 1),
#                               ("id4", "array", [1, 2, 3]),
#                               ("id3", "object", {"test": "val"})])
#     def test_ramoval_nonexistent_namespace(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.remove(id)
#
#     @pytest.mark.parametrize(["id", "type", "value"],
#                              [("id1", "string", "test"),
#                               ("id2", "integer", 1),
#                               ("id4", "array", [1, 2, 3]),
#                               ("id3", "object", {"test": "val"})])
#     def test_ramoval_nonexistent_entry(self, id, type, value):
#         psm = PipestatManager({}, "test")
#         psm.report(id + "_test", type, value)
#         psm.remove(id)
