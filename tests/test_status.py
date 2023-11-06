"""Tests for pipestat's status checking/management functionality"""

import os
import pytest
from pipestat import SamplePipestatManager
from pipestat.const import STATUS_FILE_DIR, FILE_KEY
from .conftest import BACKEND_KEY_DB, BACKEND_KEY_FILE, DB_URL

from .test_db_only_mode import ContextManagerDBTesting
from pipestat.exceptions import UnrecognizedStatusError


def test_status_file_default_location(schema_file_path, results_file_path):
    """status file location is set to the results file dir
    if not specified"""
    psm = SamplePipestatManager(
        results_file_path=results_file_path,
        schema_path=schema_file_path,
    )
    assert psm.cfg[STATUS_FILE_DIR] == os.path.dirname(psm.cfg[FILE_KEY])


@pytest.mark.parametrize("backend_data", ["file", "db"], indirect=True)
@pytest.mark.parametrize("status_id", ["running", "failed", "completed"])
def test_status_not_configured(schema_file_path, config_file_path, backend_data, status_id):
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
    backend_data,
    status_id,
    custom_status_schema2,
):
    """Status management works even in case it has not been configured."""
    with ContextManagerDBTesting(DB_URL):
        args = dict(
            schema_path=custom_status_schema2,
        )
        args.update(backend_data)
        psm = SamplePipestatManager(**args)
        psm.set_status(record_identifier="sample1", status_identifier=status_id)
        assert psm.get_status(record_identifier="sample1") == status_id


@pytest.mark.parametrize("backend_data", ["file", "db"], indirect=True)
@pytest.mark.parametrize("status_id", ["NOTINSCHEMA"])
def test_status_not_in_schema__raises_expected_error(
    schema_file_path, config_file_path, backend_data, status_id
):
    """A status to set must be a value declared in the active schema, whether default or custom."""
    with ContextManagerDBTesting(DB_URL):
        args = dict(
            schema_path=schema_file_path,
        )
        args.update(backend_data)
        psm = SamplePipestatManager(**args)
        with pytest.raises(UnrecognizedStatusError):
            psm.set_status(record_identifier="sample1", status_identifier=status_id)


@pytest.mark.skip(reason="not implemented")
def test_clear_status():
    # TODO write pytest for clearing statuses
    """Test clearing flag files"""
    pass
