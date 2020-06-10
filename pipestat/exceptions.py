""" Package exception types """

import abc
from .const import *

__all__ = ["InvalidTypeError"]


class PipestatError(Exception):
    """ Base exception type for this package """
    __metaclass__ = abc.ABCMeta


class InvalidTypeError(PipestatError):
    """ Type of the reported value is not supported """
    def __init__(self, type):
        super(InvalidTypeError, self).__init__(
            "'{}' is an invalid type. Only the following types are "
            "supported: {}".format(type, TYPES))