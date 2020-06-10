# Project configuration, particularly for logging.

import logmuse
from ._version import __version__
from .pipestat import *
from .helpers import *

__classes__ = ["PipeStatManager"]
__all__ = __classes__ + ["connect_mongo"]

logmuse.init_logger("pipestat")
