"""Pipestat: a pipeline results manager.

Classes:
    PipestatManager - The core class. Backend-specific entry points:
        PipestatManager.from_file_backend("results.yaml", schema_path="schema.yaml")
        PipestatManager.from_db_backend("pipestat_config.yaml")
        PipestatManager.from_pephub_backend("databio/project:default")
        PipestatManager.from_config("pipestat_config.yaml")  # generic, config decides backend

    SamplePipestatManager - Convenience wrapper that sets pipeline_type="sample".
        Equivalent to PipestatManager(pipeline_type="sample", ...).

    ProjectPipestatManager - Convenience wrapper that sets pipeline_type="project".
        Equivalent to PipestatManager(pipeline_type="project", ...).

    PipestatDualManager - Holds both a SamplePipestatManager and a ProjectPipestatManager.
        Use when you need to manage results at both levels simultaneously.
        Access the sub-managers via .sample and .project attributes.
"""

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
