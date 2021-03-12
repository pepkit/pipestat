""" Package exception types """

import abc

from .const import *

__all__ = [
    "InvalidTypeError",
    "IncompatibleClassError",
    "PipestatError",
    "PipestatDatabaseError",
    "MissingConfigDataError",
    "SchemaError",
    "SchemaNotFoundError",
]


class PipestatError(Exception):
    """ Base exception type for this package """

    __metaclass__ = abc.ABCMeta


class SchemaError(PipestatError):
    """ Schema error """

    def __init__(self, msg):
        super(SchemaError, self).__init__(msg)


class SchemaNotFoundError(SchemaError):
    """ Schema not found error """

    def __init__(self, msg):
        txt = (
            f"Results schema not found. The schema is required to {msg}. "
            f"It needs to be supplied to the object constructor."
        )
        super(SchemaNotFoundError, self).__init__(txt)


class MissingConfigDataError(PipestatError):
    """ Exception for invalid config file. """

    def __init__(self, msg):
        spacing = " " if msg[-1] in ["?", ".", "\n"] else "; "
        suggest = "For config format documentation please see: " + DOC_URL
        super(MissingConfigDataError, self).__init__(msg + spacing + suggest)


class PipestatDatabaseError(PipestatError):
    """ Database error """

    def __init__(self, msg):
        super(PipestatDatabaseError, self).__init__(msg)


class InvalidTypeError(PipestatError):
    """ Type of the reported value is not supported """

    def __init__(self, type):
        super(InvalidTypeError, self).__init__(
            "'{}' is an invalid type. Only the following types are "
            "supported: {}".format(type, list(CLASSES_BY_TYPE.keys()))
        )


class IncompatibleClassError(PipestatError):
    """ Class  of the reported value is not supported """

    def __init__(self, cls, req_cls, type):
        super(IncompatibleClassError, self).__init__(
            "Incompatible value class for the declared result type ({}). "
            "Required: {}; got: {}".format(type, req_cls, cls)
        )
