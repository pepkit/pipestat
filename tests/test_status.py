import pytest

from pipestat import PipestatManager
from pipestat.const import *


class TestStatus:
    def test_status_file_default_location(self, schema_file_path, results_file_path):
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
        """status management works even in case it has not been configured"""
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
        """status management works even in case it has not been configured"""
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
