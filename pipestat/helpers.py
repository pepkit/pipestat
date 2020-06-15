import logging
from .const import *
from ._version import __version__
from .exceptions import IncompatibleClassError
from ubiquerg import VersionInHelpParser

_LOGGER = logging.getLogger(__name__)


def build_argparser():
    """
    Builds argument parser.

    :return argparse.ArgumentParser
    """
    banner = "%(prog)s - pipeline results reported"
    additional_description = "\n..."
    parser = VersionInHelpParser(version=__version__, description=banner,
                                 epilog=additional_description)
    subparsers = parser.add_subparsers(dest="command")
    sps = {}
    # common arguments
    for cmd, desc in SUBPARSER_MSGS.items():
        sps[cmd] = subparsers.add_parser(cmd, description=desc, help=desc)
        sps[cmd].add_argument("-d", "--database", required=True, type=str,
            metavar="DB", help="database to store results in")
        sps[cmd].add_argument(
                "-n", "--name", required=True, type=str, metavar="N",
                help="name of the pipeline to report result for")

    # remove and report
    for subparser in [REMOVE_CMD, REPORT_CMD]:
        sps[subparser].add_argument(
                "-i", "--id", required=True, type=str, metavar="ID",
                help="id of the result to report")

    # report
    sps[REPORT_CMD].add_argument(
            "-v", "--value", required=True, type=str, metavar="V",
            help="value of the result to report")

    sps[REPORT_CMD].add_argument(
            "-t", "--type", required=True, type=str, metavar="T",
            help="type of the result to report")

    sps[REPORT_CMD].add_argument(
            "-o", "--overwrite", action="store_true",
            help="whether the result should override existing ones in "
                 "case of name clashes")

    sps[REPORT_CMD].add_argument(
            "-s", "--strict-type", action="store_true",
            help="whether the result should be casted to the class required by "
                 "the declared type and the effect of this operation verified")

    return parser


def connect_mongo(host='0.0.0.0', port=27017, database='pipestat_dict',
                  collection='store'):
    """
    Connect to MongoDB and return the MongoDB-backed dict object

    Firstly, the required libraries are imported.

    :param str host: DB address
    :param int port: port DB is listening on
    :param str database: DB name
    :param str collection: collection key
    :return mongodict.MongoDict: a dict backed by MongoDB, ready to use as a
        Henge backend
    """
    from importlib import import_module
    from inspect import stack
    for lib in LIBS_BY_BACKEND["mongo"]:
        try:
            globals()[lib] = import_module(lib)
        except ImportError:
            raise ImportError(
                "Requirements not met. Package '{}' is required to setup "
                "MongoDB connection. Install the package and call '{}' again.".
                    format(lib, stack()[0][3]))
    pymongo.Connection = lambda host, port, **kwargs: \
        pymongo.MongoClient(host=host, port=port)
    return mongodict.MongoDict(host=host, port=port, database=database,
                               collection=collection)


def validate_value_class(type, value):
    """
    Try to convert provided result value to the required class for the declared
     type and check if the conversion was successful.
     Raise an informative exception if not.

    :param str type: name of the type
    :param value: value to check
    :raise IncompatibleClassError: if type cannot be converted to the
        required one
    """
    try:
        value = CLASSES_BY_TYPE[type](value)
    except Exception as e:
        _LOGGER.debug("Impossible type conversion: {}".
                      format(getattr(e, 'message', repr(e))))
    if not isinstance(value, CLASSES_BY_TYPE[type]):
        raise IncompatibleClassError(value.__class__.__name__,
                                     CLASSES_BY_TYPE[type].__name__, type)
    return value