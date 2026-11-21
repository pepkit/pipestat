"""Job ids and scheduler job state, used to tell whether a 'running' status is stale.

When a scheduler kills a job (timeout, out of memory, scancel, node failure),
the pipeline never gets to set its status to 'failed', so the status stays
'running'. pipestat stores the id of the job that set a status; this module
asks the scheduler about that job and reports whether the status is stale.
pipestat never rewrites the status itself.
"""

import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from logging import getLogger

from .const import PKG_NAME

_LOGGER = getLogger(PKG_NAME)

# Normalized job states. A 'running' status is stale when its job is in a
# state other than these three.
ALIVE_JOB_STATES = ("pending", "running", "unknown")

_SLURM_STATE_MAP = {
    **dict.fromkeys(
        ["PENDING", "REQUEUED", "REQUEUE_HOLD", "REQUEUE_FED", "RESIZING", "SPECIAL_EXIT"],
        "pending",
    ),
    **dict.fromkeys(
        ["RUNNING", "COMPLETING", "CONFIGURING", "STAGE_OUT", "SIGNALING", "SUSPENDED", "STOPPED"],
        "running",
    ),
    "COMPLETED": "completed",
    **dict.fromkeys(["FAILED", "BOOT_FAIL", "DEADLINE", "REVOKED"], "failed"),
    **dict.fromkeys(["CANCELLED", "PREEMPTED"], "cancelled"),
    "TIMEOUT": "timeout",
    "OUT_OF_MEMORY": "out_of_memory",
    "NODE_FAIL": "node_fail",
}


@dataclass(frozen=True)
class StatusResolution:
    """A record's status, plus what the scheduler says about the job that set it.

    Attributes:
        record_identifier: The record the status belongs to.
        status: The stored status, e.g. 'running', or None if none is set.
        job_id: The job id stored with the status, or None.
        job_state: Normalized scheduler state of the job, or None if not probed.
        stale: True when the status is 'running' but the job is finished or gone.
    """

    record_identifier: str | None
    status: str | None
    job_id: str | None
    job_state: str | None
    stale: bool


def current_job_id() -> str | None:
    """Get the id of the SLURM job this process runs in, or None."""
    return os.environ.get("SLURM_JOB_ID") or None


def resolve_status(
    record_identifier: str | None,
    status: str | None,
    job_id: str | None,
    job_state_fn: Callable[[str], str] | None,
) -> StatusResolution:
    """Combine a status and its job id into a StatusResolution.

    The job is probed only when a probe is given, the status is 'running' and
    a job id is known.

    Args:
        record_identifier: The record the status belongs to.
        status: The stored status.
        job_id: The job id stored with the status.
        job_state_fn: Function mapping a job id to a job state, or None.

    Returns:
        StatusResolution: The combined result.
    """
    if job_state_fn is None or status != "running" or job_id is None:
        return StatusResolution(record_identifier, status, job_id, None, False)
    job_state = job_state_fn(job_id)
    return StatusResolution(
        record_identifier, status, job_id, job_state, job_state not in ALIVE_JOB_STATES
    )


def slurm_job_state(job_id: str, timeout: float = 30) -> str:
    """Get the state of a SLURM job from sacct, or squeue if sacct cannot answer.

    Args:
        job_id: SLURM job id.
        timeout: Seconds to wait for each command.

    Returns:
        str: One of pending, running, completed, failed, cancelled, timeout,
            out_of_memory, node_fail, 'absent' (the scheduler does not know
            the job) or 'unknown' (the scheduler could not be asked).
    """
    job_id = str(job_id)
    out = _run(["sacct", "-j", job_id, "--format=State", "--noheader", "-P", "-X"], timeout)
    if not out:
        # sacct is unavailable, or does not know the job (maybe too new for accounting)
        squeue_out = _run(["squeue", "-h", "-j", job_id, "-o", "%T"], timeout)
        if out is None and squeue_out is None:
            return "unknown"
        out = squeue_out
    if not out:
        return "absent"
    state = out.split()[0].rstrip("+").upper()
    return _SLURM_STATE_MAP.get(state, "unknown")


def _run(cmd: list[str], timeout: float) -> str | None:
    """Run a scheduler command.

    Returns:
        str | None: Its stdout; '' when the scheduler reports an invalid job
            id; None when the command is missing, failed or timed out.
    """
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as e:
        _LOGGER.debug(f"Could not run {cmd[0]}: {e}")
        return None
    if res.returncode != 0:
        if "invalid job id" in res.stderr.lower():
            return ""
        _LOGGER.debug(f"{cmd[0]} failed: {res.stderr.strip()}")
        return None
    return res.stdout
