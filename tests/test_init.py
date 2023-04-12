from tempfile import mkdtemp

import pytest
from yaml import dump

from pipestat import PipestatManager
from pipestat.const import *
from pipestat.exceptions import *


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
        """Object constructor works with file as backend"""
        assert isinstance(
            PipestatManager(
                namespace="test",
                results_file_path=results_file_path,
                schema_path=schema_file_path,
            ),
            PipestatManager,
        )

    def test_obj_creation_db(self, config_file_path):
        """Object constructor works with database as backend"""
        assert isinstance(PipestatManager(config=config_file_path), PipestatManager)

    def test_schema_is_required_to_create_manager(self, results_file_path):
        """
        Object constructor raises exception if schema is not provided.
        """
        with pytest.raises(PipestatError) as err_ctx:
            PipestatManager(results_file_path=results_file_path)
        obs_err_msg = str(err_ctx.value)
        exp_err_msg = "No schema path could be found."
        assert obs_err_msg == exp_err_msg

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
        """Object constructor raises exception if cfg is missing data"""
        tmp_pth = os.path.join(mkdtemp(), "res.yml")
        with open(tmp_pth, "w") as file:
            dump({"database": {"host": "localhost"}}, file)
        with pytest.raises(MissingConfigDataError):
            PipestatManager(
                namespace="test", config=tmp_pth, schema_path=schema_file_path
            )

    def test_unknown_backend(self, schema_file_path):
        """Either db config or results file path needs to be provided"""
        with pytest.raises(NoBackendSpecifiedError):
            PipestatManager(namespace="test", schema_path=schema_file_path)

    def test_create_results_file(self, schema_file_path):
        """Results file is created if a nonexistent path provided"""
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        PipestatManager(
            namespace="test",
            results_file_path=tmp_res_file,
            schema_path=schema_file_path,
        )
        assert os.path.exists(tmp_res_file)

    # @pytest.mark.skip()
    def test_use_other_namespace_file(self, schema_file_path):
        """Results file can be used with just one namespace"""
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        psm = PipestatManager(
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
        """Input string that is not a file path raises an informative error"""
        with pytest.raises((TypeError, AssertionError)):
            PipestatManager(
                namespace="test", results_file_path=pth, schema_path=schema_file_path
            )

    def test_results_file_contents_loaded(self, results_file_path, schema_file_path):
        """Contents of the results file are present after loading"""
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=schema_file_path,
        )
        assert "test" in psm.data

    def test_str_representation(self, results_file_path, schema_file_path):
        """Test string representation identifies number of records"""
        psm = PipestatManager(
            namespace="test",
            results_file_path=results_file_path,
            schema_path=schema_file_path,
        )
        assert f"Records count: {len(psm.data[psm.namespace])}" in str(psm)
