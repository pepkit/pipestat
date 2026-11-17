import os
from tempfile import NamedTemporaryFile, TemporaryDirectory, mkdtemp

import pytest
from yaml import dump, safe_load

from pipestat import SamplePipestatManager
from pipestat.const import PIPESTAT_GENERIC_CONFIG
from pipestat.exceptions import *
from pipestat.helpers import init_generic_config
from pipestat.parsed_schema import SCHEMA_PIPELINE_NAME_KEY

from .conftest import STANDARD_TEST_PIPE_ID, requires_db_deps


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

    @requires_db_deps
    def test_missing_cfg_data(self, schema_file_path):
        """Object constructor raises exception if cfg is missing data"""
        tmp_pth = os.path.join(mkdtemp(), "res.yml")
        with open(tmp_pth, "w") as file:
            dump({"database": {"host": "localhost"}, "project_name": "test"}, file)
        with pytest.raises(MissingConfigDataError):
            SamplePipestatManager(config_file=tmp_pth, schema_path=schema_file_path)

    @requires_db_deps
    def test_missing_project_name_raises(self, schema_file_path):
        """DB config without project_name raises ValueError"""
        tmp_pth = os.path.join(mkdtemp(), "res.yml")
        with open(tmp_pth, "w") as file:
            dump({"database": {"host": "localhost"}}, file)
        with pytest.raises(ValueError, match="project_name is required"):
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

    def test_use_other_project_name_file(self, schema_file_path, tmp_path):
        """A results file holds one pipeline unless multi_pipelines is True."""
        tmp_res_file = str(tmp_path / "res.yml")
        psm1 = SamplePipestatManager(
            results_file_path=tmp_res_file,
            schema_path=schema_file_path,
        )
        assert os.path.exists(tmp_res_file)
        with open(schema_file_path, "r") as init_schema_file:
            init_schema = safe_load(init_schema_file)
        assert psm1.pipeline_name == init_schema["properties"][SCHEMA_PIPELINE_NAME_KEY]

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
        exp_msg = (
            f"Trying to report result for namespace '{ns2}' at '{tmp_res_file}', but 1 other "
            f"namespaces are already in the file: [{psm1.pipeline_name}]. Pipestat will not "
            "report multiple namespaces to one file unless `multi_pipelines` is True."
        )
        assert str(exc_ctx.value) == exp_msg

        # With multi_pipelines=True the second pipeline may share the file.
        psm2 = SamplePipestatManager(
            results_file_path=tmp_res_file,
            schema_path=temp_schema_path,
            multi_pipelines=True,
        )
        assert psm2.pipeline_name == ns2

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
                "sample2": {"number_of_things": 2},
            }
            for k, v in val_dict.items():
                psm.report(record_identifier=k, values=v)
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
                "sample2": {"number_of_things": 2},
            }
            for k, v in val_dict.items():
                psm.report(record_identifier=k, values=v)
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
