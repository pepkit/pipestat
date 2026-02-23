"""Pipestat: a pipeline results manager.

Classes:
    PipestatManager - The core class. Use this directly for most cases.
        Pass pipeline_type="sample" or pipeline_type="project" to control
        what level of results you are managing.

    SamplePipestatManager - Convenience wrapper that sets pipeline_type="sample".
        Equivalent to PipestatManager(pipeline_type="sample", ...).

    ProjectPipestatManager - Convenience wrapper that sets pipeline_type="project".
        Equivalent to PipestatManager(pipeline_type="project", ...).

    PipestatDualManager - Holds both a SamplePipestatManager and a ProjectPipestatManager.
        Use when you need to manage results at both levels simultaneously.
        Access the sub-managers via .sample and .project attributes.
"""

# Project configuration, particularly for logging.

import logmuse

from .const import PKG_NAME
from .exceptions import PipestatError
from .pipestat import (
    PipestatDualManager,
    PipestatManager,
    ProjectPipestatManager,
    SamplePipestatManager,
)

__all__ = [
    "PipestatError",
    "SamplePipestatManager",
    "ProjectPipestatManager",
    "PipestatDualManager",
    "PipestatManager",
]

logmuse.init_logger(PKG_NAME)
