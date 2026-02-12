# Project configuration, particularly for logging.

import logmuse

from .const import PKG_NAME
from .exceptions import PipestatError
from .pipestat import PipestatBoss, PipestatManager, ProjectPipestatManager, SamplePipestatManager

__all__ = [
    "PipestatError",
    "SamplePipestatManager",
    "ProjectPipestatManager",
    "PipestatBoss",
    "PipestatManager",
]

logmuse.init_logger(PKG_NAME)
