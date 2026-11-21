"""PostgreSQL-backed tests for tests/test_status_job.py."""

import pytest

from pipestat import SamplePipestatManager
from pipestat.const import STATUS

from ..conftest import DB_URL, ContextManagerDBTesting


class TestDBBackend:
    @pytest.fixture
    def dbpsm(self, config_file_path, schema_file_path):
        with ContextManagerDBTesting(DB_URL):
            yield SamplePipestatManager(config_file=config_file_path, schema_path=schema_file_path)

    def test_job_id_stored_and_cleared(self, dbpsm):
        dbpsm.set_status("running", "s1", job_id="99")
        assert dbpsm.get_status("s1") == "running"
        assert dbpsm.get_status_job_id("s1") == "99"
        dbpsm.set_status("completed", "s1", job_id="100")
        assert dbpsm.get_status_job_id("s1") == "100"
        dbpsm.set_status("running", "s1")
        assert dbpsm.get_status_job_id("s1") is None

    def test_job_id_ignored_when_status_changed_by_unaware_writer(self, dbpsm):
        dbpsm.set_status("running", "s1", job_id="99")
        dbpsm.backend.report(values={STATUS: "completed"}, record_identifier="s1")
        assert dbpsm.get_status_job_id("s1") is None

    def test_resolve_status_stale(self, dbpsm):
        dbpsm.set_status("running", "s1", job_id="99")
        res = dbpsm.resolve_status("s1", job_state_fn=lambda j: "out_of_memory")
        assert (res.job_state, res.stale) == ("out_of_memory", True)
        assert dbpsm.get_status("s1") == "running"

    def test_remove_record_removes_job_id_row(self, dbpsm):
        dbpsm.set_status("running", "s1", job_id="99")
        dbpsm.backend.remove_record("s1", rm_record=True)
        dbpsm.set_status("running", "s1")
        assert dbpsm.get_status_job_id("s1") is None
