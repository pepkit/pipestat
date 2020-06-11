""" Package exception types """

import abc
from .const import *

__all__ = ["InvalidTypeError", "IncompatibleClassError", "PipestatError"]


class PipestatError(Exception):
    """ Base exception type for this package """
    __metaclass__ = abc.ABCMeta


class InvalidTypeError(PipestatError):
    """ Type of the reported value is not supported """
    def __init__(self, type):
        super(InvalidTypeError, self).__init__(
            "'{}' is an invalid type. Only the following types are "
            "supported: {}".format(type, list(CLASSES_BY_TYPE.keys())))


class IncompatibleClassError(PipestatError):
    """ Class  of the reported value is not supported """
    def __init__(self, cls, req_cls, type):
        super(IncompatibleClassError, self).__init__(
            "Incompatible value class for the declared result type ({}). "
            "Required: {}; got: {}".format(type, req_cls, cls))
