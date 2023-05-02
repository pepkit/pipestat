# Project configuration, particularly for logging.

import logmuse

from ._version import __version__
from .helpers import *
from .exceptions import PipestatError
from .pipestat import *

__classes__ = ["PipestatError", "PipestatManager"]
__all__ = __classes__

logmuse.init_logger(PKG_NAME)
