import pytest
from pipestat import PipeStatManager


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
