# Project configuration, particularly for logging.

import logmuse

from ._version import __version__
from .helpers import *
from .pipestat import *

__classes__ = ["PipestatManager"]
__all__ = __classes__

logmuse.init_logger("pipestat")
