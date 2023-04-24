"""Tests for pipestat's status checking/management functionality"""

import pytest

from pipestat import PipestatManager
from pipestat.const import *

BACKEND_KEY_DB = "db"
BACKEND_KEY_FILE = "file"


@pytest.fixture
def backend_data(request, config_file_path, results_file_path):
    if request.param == BACKEND_KEY_DB:
        return {"config": config_file_path}
    elif request.param == BACKEND_KEY_FILE:
        return {"results_file_path": results_file_path}
    raise Exception(f"Unrecognized initial parametrization for backend data: {request.param}")


def test_status_file_default_location(schema_file_path, results_file_path):
    """status file location is set to the results file dir
    if not specified"""
    psm = PipestatManager(
        results_file_path=results_file_path,
        schema_path=schema_file_path,
    )
    assert psm[STATUS_FILE_DIR] == os.path.dirname(psm.file)


@pytest.mark.parametrize("backend_data", ["file", "db"], indirect=True)
@pytest.mark.parametrize("status_id", ["running", "failed", "completed"])
def test_status_not_configured(
    schema_file_path, config_file_path, backend_data, status_id
):
    """Status management works even in case it has not been configured."""
    args = dict(
        schema_path=schema_file_path,
    )
    args.update(backend_data)
    psm = PipestatManager(**args)
    psm.set_status(record_identifier="sample1", status_identifier=status_id)
    assert psm.get_status(record_identifier="sample1") == status_id


@pytest.mark.parametrize(
    "backend_data", [BACKEND_KEY_FILE, BACKEND_KEY_DB], indirect=True
)
@pytest.mark.parametrize(
    "status_id", ["running_custom", "failed_custom", "completed_custom"]
)
def test_custom_status_schema(
    backend_data,
    status_id,
    custom_status_schema,
):
    """Status management works even in case it has not been configured."""
    args = dict(
        schema_path=custom_status_schema,
    )
    args.update(backend_data)
    psm = PipestatManager(**args)
    psm.set_status(record_identifier="sample1", status_identifier=status_id)
    assert psm.get_status(record_identifier="sample1") == status_id
