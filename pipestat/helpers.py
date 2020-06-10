from .const import *
from ._version import __version__
from ubiquerg import VersionInHelpParser


def build_argparser():
    """
    Builds argument parser.

    :return argparse.ArgumentParser
    """
    banner = "%(prog)s - pipeline results reported"
    additional_description = "\n..."
    parser = VersionInHelpParser(version=__version__, description=banner,
                                 epilog=additional_description)

    parser.add_argument(
            "-n", "--name", required=True, type=str, metavar="N",
            help="name of the pipeline to report result for")

    parser.add_argument(
            "-i", "--id", required=True, type=str, metavar="ID",
            help="id of the result to report")

    parser.add_argument(
            "-v", "--value", required=True, type=str, metavar="V",
            help="value of the result to report")

    parser.add_argument(
            "-t", "--type", required=True, type=str, metavar="T",
            help="type of the result to report")

    parser.add_argument(
            "-d", "--database", required=False, type=str, metavar="DB",
            help="database to store results in")

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
