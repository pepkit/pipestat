from tempfile import mkdtemp

import oyaml
import pytest
import os
from yaml import dump

from pipestat import PipestatManager, SamplePipestatManager, ProjectPipestatManager
from pipestat.exceptions import *
from pipestat.parsed_schema import SCHEMA_PIPELINE_NAME_KEY
from tempfile import NamedTemporaryFile, TemporaryDirectory
from .conftest import STANDARD_TEST_PIPE_ID
from .conftest import SERVICE_UNAVAILABLE
from pipestat.helpers import init_generic_config
from pipestat.const import PIPESTAT_GENERIC_CONFIG, SCHEMA_KEY


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires postgres service to be available")
class TestPipestatManagerInstantiation:
    def test_obj_creation_file(self, schema_file_path, results_file_path):
        """Object constructor works with file as backend"""
        assert isinstance(
            SamplePipestatManager(
                results_file_path=results_file_path,
                schema_path=schema_file_path,
            ),
            SamplePipestatManager,
        )

    def test_obj_creation_db(self, config_file_path):
        """Object constructor works with database as backend"""
        assert isinstance(
            SamplePipestatManager(config_file=config_file_path), SamplePipestatManager
        )

    def test_schema_is_required_to_create_manager(self, config_no_schema_file_path):
        """
        Object constructor raises exception if schema is not provided.
        """
        # ToDo this test may be redundant with the modified test_report_requires_schema
        with pytest.raises(SchemaNotFoundError):
            # psm = PipestatManager(results_file_path=results_file_path)
            psm = SamplePipestatManager(config_file=config_no_schema_file_path)

    def test_schema_recursive_custom_type_conversion(
        self, recursive_schema_file_path, results_file_path
    ):
        psm = SamplePipestatManager(
            results_file_path=results_file_path,
            schema_path=recursive_schema_file_path,
        )
        assert (
            "path"
            in psm.result_schemas["output_file_in_object"]["properties"]["prop1"]["properties"]
        )
        assert (
            "thumbnail_path"
            in psm.result_schemas["output_file_in_object"]["properties"]["prop2"]["properties"]
        )

    def test_missing_cfg_data(self, schema_file_path):
        """Object constructor raises exception if cfg is missing data"""
        tmp_pth = os.path.join(mkdtemp(), "res.yml")
        with open(tmp_pth, "w") as file:
            dump({"database": {"host": "localhost"}}, file)
        with pytest.raises(MissingConfigDataError):
            SamplePipestatManager(config_file=tmp_pth, schema_path=schema_file_path)

    def test_unknown_backend(self, schema_file_path):
        """Either db config or results file path needs to be provided"""
        with pytest.raises(NoBackendSpecifiedError):
            SamplePipestatManager(schema_path=schema_file_path)

    def test_create_results_file(self, schema_file_path):
        """Results file is created if a nonexistent path provided"""
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        SamplePipestatManager(
            results_file_path=tmp_res_file,
            schema_path=schema_file_path,
        )
        assert os.path.exists(tmp_res_file)

    # @pytest.mark.skip()
    def test_use_other_project_name_file(self, schema_file_path, tmp_path):
        """Results file can be used with just one project name"""
        tmp_res_file = os.path.join(mkdtemp(), "res.yml")
        print(f"Temporary results file: {tmp_res_file}")
        assert not os.path.exists(tmp_res_file)
        psm1 = SamplePipestatManager(
            results_file_path=tmp_res_file,
            schema_path=schema_file_path,
        )
        assert os.path.exists(tmp_res_file)
        with open(schema_file_path, "r") as init_schema_file:
            init_schema = oyaml.safe_load(init_schema_file)

        assert (
            psm1.cfg[SCHEMA_KEY].pipeline_name
            == init_schema["properties"][SCHEMA_PIPELINE_NAME_KEY]
        )

        ns2 = "namespace2"
        temp_schema_path = str(tmp_path / "schema.yaml")
        init_schema["properties"][SCHEMA_PIPELINE_NAME_KEY] = ns2
        with open(temp_schema_path, "w") as temp_schema_file:
            dump(init_schema, temp_schema_file)
        with pytest.raises(PipestatError) as exc_ctx:
            SamplePipestatManager(
                results_file_path=tmp_res_file,
                schema_path=temp_schema_path,
            )
        # exp_msg = f"'{tmp_res_file}' is already used to report results for a different (not {ns2}) namespace: {psm1.schema.pipeline_name}"
        exp_msg = f"'{tmp_res_file}' is already in use for 1 namespaces: {psm1.cfg[SCHEMA_KEY].pipeline_name} and multi_pipelines = False."
        obs_msg = str(exc_ctx.value)
        assert obs_msg == exp_msg

    @pytest.mark.parametrize("pth", [["/$HOME/path.yaml"], 1])
    def test_wrong_class_results_file(self, schema_file_path, pth):
        """Input string that is not a file path raises an informative error"""
        with pytest.raises((TypeError, AssertionError)):
            SamplePipestatManager(results_file_path=pth, schema_path=schema_file_path)

    def test_results_file_contents_loaded(self, results_file_path, schema_file_path):
        """Contents of the results file are present after loading"""
        with NamedTemporaryFile() as f:
            results_file_path = f.name
            psm = SamplePipestatManager(
                results_file_path=results_file_path,
                schema_path=schema_file_path,
            )
            val_dict = {
                "sample1": {"name_of_something": "test_name"},
                "sample1": {"number_of_things": 2},
            }
            for k, v in val_dict.items():
                psm.report(record_identifier=k, values=v, force_overwrite=True)
            # Check that a new pipestatmanager object can correctly read the results_file.
            psm2 = SamplePipestatManager(
                results_file_path=results_file_path,
                schema_path=schema_file_path,
            )
            assert STANDARD_TEST_PIPE_ID in psm2.backend._data

    def test_str_representation(self, results_file_path, schema_file_path):
        """Test string representation identifies number of records"""
        with NamedTemporaryFile() as f:
            results_file_path = f.name
            psm = SamplePipestatManager(
                results_file_path=results_file_path,
                schema_path=schema_file_path,
            )
            val_dict = {
                "sample1": {"name_of_something": "test_name"},
                "sample1": {"number_of_things": 2},
            }
            for k, v in val_dict.items():
                psm.report(record_identifier=k, values=v, force_overwrite=True)
            assert f"Records count: {len(psm.backend._data[STANDARD_TEST_PIPE_ID])}" in str(psm)

    def test_init_config(self, capfd):
        """Tests initializing generic configuration file"""
        with TemporaryDirectory() as dir:
            os.chdir(dir)

            expectedpath = os.path.join(os.getcwd(), "config", PIPESTAT_GENERIC_CONFIG)
            result = init_generic_config()
            assert result is True
            assert os.path.exists(expectedpath)
            result = init_generic_config()
            out, err = capfd.readouterr()
            assert "already exists" in out
