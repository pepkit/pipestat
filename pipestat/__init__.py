# Project configuration, particularly for logging.

import logmuse

from ._version import __version__
from .exceptions import PipestatError
from .const import PKG_NAME
from .pipestat import (
    PipestatManager,
    SamplePipestatManager,
    ProjectPipestatManager,
    PipestatBoss,
)


__all__ = [
    "PipestatError",
    "SamplePipestatManager",
    "ProjectPipestatManager",
    "PipestatBoss",
    "PipestatManager",
    "__version__",
]

logmuse.init_logger(PKG_NAME)
