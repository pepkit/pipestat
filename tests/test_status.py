"""Tests for pipestat's status checking/management functionality"""

import os
import pytest

from pipestat import SamplePipestatManager

from pipestat.const import STATUS_FILE_DIR, FILE_KEY
from .conftest import BACKEND_KEY_DB, BACKEND_KEY_FILE, DB_URL, SERVICE_UNAVAILABLE

from .test_db_only_mode import ContextManagerDBTesting
from pipestat.exceptions import UnrecognizedStatusError
from tempfile import NamedTemporaryFile


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires postgres service to be available")
class TestStatus:
    def test_status_file_default_location(self, schema_file_path, results_file_path):
        """status file location is set to the results file dir
        if not specified"""
        psm = SamplePipestatManager(
            results_file_path=results_file_path,
            schema_path=schema_file_path,
        )
        assert psm.cfg[STATUS_FILE_DIR] == os.path.dirname(psm.cfg[FILE_KEY])

    @pytest.mark.parametrize("backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True)
    @pytest.mark.parametrize("status_id", ["running", "failed", "completed"])
    def test_status_not_configured(
        self, schema_file_path, config_file_path, backend_data, status_id
    ):
        """Status management works even in case it has not been configured."""
        with ContextManagerDBTesting(DB_URL):
            args = dict(
                schema_path=schema_file_path,
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.set_status(record_identifier="sample1", status_identifier=status_id)
            assert psm.get_status(record_identifier="sample1") == status_id

    @pytest.mark.parametrize("backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True)
    @pytest.mark.parametrize("status_id", ["running_custom", "failed_custom", "completed_custom"])
    def test_custom_status_schema(
        self,
        backend_data,
        status_id,
        custom_status_schema2,
    ):
        """Status management works even in case it has not been configured."""
        with ContextManagerDBTesting(DB_URL) as connection:
            args = dict(
                schema_path=custom_status_schema2,
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            psm.set_status(record_identifier="sample1", status_identifier=status_id)
            assert psm.get_status(record_identifier="sample1") == status_id

    @pytest.mark.parametrize("backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True)
    @pytest.mark.parametrize("status_id", ["NOTINSCHEMA"])
    def test_status_not_in_schema__raises_expected_error(
        self, schema_file_path, config_file_path, backend_data, status_id
    ):
        """A status to set must be a value declared in the active schema, whether default or custom."""
        with ContextManagerDBTesting(DB_URL) as connection:
            args = dict(
                schema_path=schema_file_path,
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            with pytest.raises(UnrecognizedStatusError):
                psm.set_status(record_identifier="sample1", status_identifier=status_id)

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", [BACKEND_KEY_FILE])
    def test_clear_status_for_filebackend(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
    ):
        """Test clearing flag files"""
        with NamedTemporaryFile() as f:
            results_file_path = f.name
            args = dict(schema_path=schema_file_path, database_only=False)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            psm.report(record_identifier=rec_id, values=val, force_overwrite=True)
            psm.set_status(status_identifier="running", record_identifier=rec_id)
            status = psm.get_status(record_identifier=rec_id)
            assert status == "running"
            psm.clear_status(record_identifier=rec_id)
            status = psm.get_status(record_identifier=rec_id)
            assert status is None
