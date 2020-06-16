import pytest
import os
from pipestat import PipeStatManager
from pipestat.exceptions import IncompatibleClassError, InvalidTypeError
from pipestat import CLASSES_BY_TYPE
from tempfile import mkdtemp
from shutil import copyfile


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


class TestPipeStatManagerInstantiation:
    def test_name_required(self):
        """ Require a namespace for the results """
        with pytest.raises(TypeError):
            PipeStatManager({})

    def test_basic_obj_creation(self):
        """ Object constructor works """
        assert isinstance(PipeStatManager({}, "a"), PipeStatManager)

    def test_unknown_db_type(self):
        """ Only string (filepath) and Mappings are supported now """
        with pytest.raises(NotImplementedError):
            PipeStatManager([], "a")

    def test_nonexistent_file_db(self):
        """ Input string that is not a file path raises an informative error """
        with pytest.raises(FileNotFoundError):
            PipeStatManager("slndns", "a")

    def test_file_db_contents_are_loaded(self, db_file_path):
        """ Contents of the DB file are present in the obj DB after loading """
        psm = PipeStatManager(db_file_path, "test")
        assert "test" in psm.database


class TestReporting:
    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", "test"),
                              ("id2", "integer", 1),
                              ("id2", "array", [1, 2, 3])])
    def test_report_basic_dict(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id, type, value)
        assert id in psm.database["test"]

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", "test"),
                              ("id2", "integer", 1),
                              ("id4", "array", [1, 2, 3]),
                              ("id3", "object", {"test": "val"})])
    def test_report_basic_file(self, id, type, value, db_file_path):
        temp_db = os.path.join(mkdtemp(), os.path.basename(db_file_path))
        copyfile(db_file_path, temp_db)
        psm = PipeStatManager(temp_db, "test")
        psm.report(id, type, value)
        assert psm.database["test"][id]["value"] == value
        if not (isinstance(value, list) or isinstance(value, dict)):
            # arrays and objects are represented differently in yamls
            is_in_file(temp_db, str(value))

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", ["string"], "test"),
                              ("id2", "test", 1),
                              ("id4", "type", [1, 2, 3]),
                              ("id3", "aaa", {"test": "val"})])
    def test_invalid_type_error(self, id, type, value):
        psm = PipeStatManager({}, "test")
        with pytest.raises(InvalidTypeError):
            psm.report(id, type, value)

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", 1),
                              ("id2", "integer", "1"),
                              ("id4", "float", "1")])
    def test_val_class_conversion(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id, type, value, strict_type=True)
        assert isinstance(psm.database["test"][id]["value"],
                          CLASSES_BY_TYPE[type])

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id2", "integer", 1),
                              ("id4", "float", 2.0)])
    def test_val_no_overwrite(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id, type, value)
        value = value + 1
        psm.report(id, type, value)
        assert value != psm.database["test"][id]["value"]

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id2", "integer", 1),
                              ("id4", "float", 2.0)])
    def test_val_overwrite(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id, type, value)
        value = value + 1
        psm.report(id, type, value, overwrite=True)
        assert value == psm.database["test"][id]["value"]


class TestRemoval:
    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", "test"),
                              ("id2", "integer", 1),
                              ("id4", "array", [1, 2, 3]),
                              ("id3", "object", {"test": "val"})])
    def test_ramoval(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id, type, value)
        psm.remove(id)
        assert id not in psm.database["test"]

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", "test"),
                              ("id2", "integer", 1),
                              ("id4", "array", [1, 2, 3]),
                              ("id3", "object", {"test": "val"})])
    def test_ramoval_cache(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id, type, value, cache=True)
        assert id in psm.cache["test"]
        psm.remove(id)
        assert "test" not in psm.cache

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", "test"),
                              ("id2", "integer", 1),
                              ("id4", "array", [1, 2, 3]),
                              ("id3", "object", {"test": "val"})])
    def test_ramoval_nonexistent_namespace(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.remove(id)

    @pytest.mark.parametrize(["id", "type", "value"],
                             [("id1", "string", "test"),
                              ("id2", "integer", 1),
                              ("id4", "array", [1, 2, 3]),
                              ("id3", "object", {"test": "val"})])
    def test_ramoval_nonexistent_entry(self, id, type, value):
        psm = PipeStatManager({}, "test")
        psm.report(id + "_test", type, value)
        psm.remove(id)
