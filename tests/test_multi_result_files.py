import glob
import os
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from yacman import YAMLConfigManager

from pipestat import SamplePipestatManager


class TestMultiResultFiles:
    @pytest.mark.parametrize("backend", ["file"])
    def test_multi_results_not_implemented(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, TemporaryDirectory() as temp_dir:
            results_file_path = f.name
            single_results_file_path = "{record_identifier}_results.yaml"
            results_file_path = os.path.join(temp_dir, single_results_file_path)
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = {"results_file_path": results_file_path}
            args.update(backend_data)

            # with pytest.raises(NotImplementedError):
            SamplePipestatManager(**args)

    @pytest.mark.parametrize("backend", ["file"])
    def test_multi_results_basic(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with TemporaryDirectory() as temp_dir:
            single_results_file_path = "{record_identifier}_results.yaml"
            results_file_path = os.path.join(temp_dir, single_results_file_path)
            args = dict(schema_path=recursive_schema_file_path)
            n = 3

            for i in range_values[:n]:
                r_id = i[0]
                val = i[1]
                backend_data = {"record_identifier": r_id, "results_file_path": results_file_path}
                args.update(backend_data)
                psm = SamplePipestatManager(**args)
                psm.report(record_identifier=r_id, values=val)

            files = glob.glob(os.path.dirname(psm.file) + "**/*.yaml")
            assert len(files) == n

    @pytest.mark.parametrize("backend", ["file"])
    def test_multi_results_summarize(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with TemporaryDirectory() as temp_dir:
            single_results_file_path = "{record_identifier}/results.yaml"
            results_file_path = os.path.join(temp_dir, single_results_file_path)
            args = dict(schema_path=recursive_schema_file_path)
            n = 3

            for i in range_values[:n]:
                r_id = i[0]
                val = i[1]
                backend_data = {"record_identifier": r_id, "results_file_path": results_file_path}
                args.update(backend_data)
                psm = SamplePipestatManager(**args)
                psm.report(record_identifier=r_id, values=val)

            psm.summarize()
            data = YAMLConfigManager.from_yaml_file(
                os.path.join(temp_dir, "aggregate_results.yaml")
            )
            assert r_id in data[psm.pipeline_name][psm.pipeline_type].keys()

    @pytest.mark.parametrize("backend", ["file"])
    def test_template_path_without_record_identifier(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        """PSM created with template path and no record_identifier can check status and list results."""
        with TemporaryDirectory() as temp_dir:
            results_file_path = os.path.join(temp_dir, "{record_identifier}/results.yaml")
            psm = SamplePipestatManager(
                results_file_path=results_file_path,
                schema_path=recursive_schema_file_path,
            )
            # These are what looper calls on a PSM without record_identifier
            assert psm.count_records() == 0
            r_id = range_values[0][0]
            assert psm.backend.check_record_exists(record_identifier=r_id) is False
            assert psm.backend.list_results(record_identifier=r_id) == []
