"""Tests for job ids stored with statuses and stale 'running' detection."""

import json
import os

import pytest

from pipestat import SamplePipestatManager
from pipestat.cli import main
from pipestat.const import STATUS
from pipestat.job_state import slurm_job_state

from .conftest import DB_DEPENDENCIES, DB_URL, SERVICE_UNAVAILABLE
from .test_db_only_mode import ContextManagerDBTesting


@pytest.fixture(autouse=True)
def no_slurm_env(monkeypatch):
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)


@pytest.fixture
def results_path(tmp_path):
    return str(tmp_path / "results.yaml")


@pytest.fixture
def psm(results_path, schema_file_path):
    return SamplePipestatManager(results_file_path=results_path, schema_path=schema_file_path)


@pytest.fixture
def fake_slurm(tmp_path, monkeypatch):
    """Fake sacct/squeue on PATH; each prints $FAKE_<NAME>_OUT and exits $FAKE_<NAME>_RC."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name in ("sacct", "squeue"):
        script = bindir / name
        script.write_text(
            f'#!/bin/sh\nprintf "%s" "$FAKE_{name.upper()}_OUT"\n'
            f'[ -n "$FAKE_{name.upper()}_ERR" ] && echo "$FAKE_{name.upper()}_ERR" >&2\n'
            f'exit "${{FAKE_{name.upper()}_RC:-0}}"\n'
        )
        script.chmod(0o755)
    monkeypatch.setenv("PATH", str(bindir))

    def configure(**env):
        for name in ("sacct", "squeue"):
            for suffix in ("OUT", "ERR", "RC"):
                monkeypatch.setenv(
                    f"FAKE_{name.upper()}_{suffix}", str(env.get(f"{name}_{suffix.lower()}", ""))
                )

    return configure


class TestFileBackend:
    def test_job_id_stored_next_to_flag_without_changing_it(self, psm):
        psm.set_status("running", "s1", job_id="99")
        flag = psm.backend.get_status_flag_path("running", "s1")
        assert open(flag).read() == "running"
        assert open(flag + ".job").read() == "99"
        assert psm.get_status_job_id("s1") == "99"
        assert psm.backend.get_flag_file("s1") == flag  # the .job file is not a flag

    def test_job_id_defaults_to_slurm_env(self, psm, monkeypatch):
        psm.set_status("running", "s1")
        assert psm.get_status_job_id("s1") is None
        assert not os.path.exists(psm.backend.get_status_flag_path("running", "s1") + ".job")
        monkeypatch.setenv("SLURM_JOB_ID", "555")
        psm.set_status("running", "s1")
        assert psm.get_status_job_id("s1") == "555"

    def test_job_id_follows_status_and_is_cleared_when_omitted(self, psm):
        psm.set_status("running", "s1", job_id="1")
        psm.set_status("completed", "s1", job_id="1")
        assert not os.path.exists(psm.backend.get_status_flag_path("running", "s1") + ".job")
        assert psm.get_status_job_id("s1") == "1"
        psm.set_status("running", "s1")
        assert psm.get_status_job_id("s1") is None
        psm.set_status("running", "s1", job_id="2")
        assert psm.clear_status("s1") == ["running"]
        assert not os.path.exists(psm.backend.get_status_flag_path("running", "s1") + ".job")

    def test_job_id_older_than_flag_is_ignored(self, psm):
        psm.set_status("running", "s1", job_id="1")
        flag = psm.backend.get_status_flag_path("running", "s1")
        old = os.path.getmtime(flag) - 100
        os.utime(flag + ".job", (old, old))
        assert psm.get_status_job_id("s1") is None


@pytest.mark.parametrize(
    ["status", "job_id", "job_state", "stale"],
    [
        ("running", "1", "timeout", True),
        ("running", "1", "absent", True),
        ("running", "1", "running", False),
        ("running", "1", "unknown", False),
        ("completed", "1", None, False),
        ("running", None, None, False),
    ],
)
def test_resolve_status(psm, status, job_id, job_state, stale):
    psm.set_status(status, "s1", job_id=job_id)
    calls = []

    def fn(jid):
        calls.append(jid)
        return job_state

    res = psm.resolve_status("s1", job_state_fn=fn)
    assert (res.status, res.job_id, res.job_state, res.stale) == (status, job_id, job_state, stale)
    assert calls == ([job_id] if job_state else [])
    assert psm.get_status("s1") == status  # never rewritten


def test_resolve_status_without_probe_does_not_probe(psm):
    psm.set_status("running", "s1", job_id="1")
    res = psm.resolve_status("s1")
    assert (res.job_id, res.job_state, res.stale) == ("1", None, False)


@pytest.mark.parametrize(
    ["env", "expected"],
    [
        (dict(sacct_out="COMPLETED\n"), "completed"),
        (dict(sacct_out="CANCELLED by 5\n"), "cancelled"),
        (dict(sacct_out="RUNNING+\n"), "running"),
        (dict(sacct_out="WEIRD\n"), "unknown"),
        (dict(sacct_out="", squeue_out="PENDING\n"), "pending"),  # too new for accounting
        (dict(sacct_out="", squeue_out="", squeue_rc=1, squeue_err="Invalid job id"), "absent"),
        (dict(sacct_rc=1, squeue_out="RUNNING\n"), "running"),  # no accounting
        (dict(sacct_rc=1, squeue_rc=1, squeue_err="connection refused"), "unknown"),
    ],
)
def test_slurm_job_state(fake_slurm, env, expected):
    fake_slurm(**env)
    assert slurm_job_state("123") == expected


def test_slurm_job_state_without_slurm_is_unknown(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", str(tmp_path))
    assert slurm_job_state("123") == "unknown"


class TestCLI:
    def test_set_job_id_and_check_job(self, results_path, schema_file_path, fake_slurm, capsys):
        common = ["-f", results_path, "-s", schema_file_path, "-r", "s1"]
        with pytest.raises(SystemExit):
            main(test_args=["status", "set", "running", "--job-id", "123"] + common)
        with pytest.raises(SystemExit):
            main(test_args=["status", "get"] + common)
        assert capsys.readouterr().out.strip().splitlines()[-1] == "running"
        fake_slurm(sacct_out="FAILED\n")
        with pytest.raises(SystemExit):
            main(test_args=["status", "get", "--check-job"] + common)
        assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == {
            "record_identifier": "s1",
            "status": "running",
            "job_id": "123",
            "job_state": "failed",
            "stale": True,
        }


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires postgres service to be available")
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
