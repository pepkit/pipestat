import logging
from .const import *
from ._version import __version__
from .exceptions import *
from ubiquerg import VersionInHelpParser
from collections import Mapping

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
        sps[cmd].add_argument(
                "-n", "--name", required=True, type=str, metavar="N",
                help="name of the pipeline to report result for")

    # remove, report and inspect
    for cmd in [REMOVE_CMD, REPORT_CMD, INSPECT_CMD]:
        sps[cmd].add_argument("-d", "--database", required=True, type=str,
            metavar="DB", help="database to store results in")

    # table
    sps[TABLE_CMD].add_argument("-d", "--databases", required=True, nargs="+",
                                metavar="DB", help="collection of file paths "
                                                   "the results are stored in")

    sps[TABLE_CMD].add_argument("-p", "--path", required=True, type=str,
                                metavar="P",
                                help="Path to the file where to store the result.")

    # remove and report
    for cmd in [REMOVE_CMD, REPORT_CMD]:
        sps[cmd].add_argument(
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


def mk_list_of_str(x):
    """
    Make sure the input is a list of strings
    :param str | list[str] | falsy x: input to covert
    :return list[str]: converted input
    :raise TypeError: if the argument cannot be converted
    """
    if not x or isinstance(x, list):
        return x
    if isinstance(x, str):
        return [x]
    raise TypeError(f"String or list of strings required as input. Got: "
                    f"{x.__class__.__name__}")


def validate_schema(schema):
    """
    Check schema for any possible issues

    :param schema:
    :raises SchemaError: if any schema format issue is detected
    """
    _LOGGER.debug(f"Validating schema: {schema}")
    assert SCHEMA_PROP_KEY in schema, \
        SchemaError(f"Schema is missing '{SCHEMA_PROP_KEY}' section")
    assert isinstance(schema[SCHEMA_PROP_KEY], Mapping), \
        SchemaError(f"'{SCHEMA_PROP_KEY}' section in the schama has to be a "
                    f"{Mapping.__class__.__name__}")
    for k, v in schema[SCHEMA_PROP_KEY].items():
        assert SCHEMA_TYPE_KEY in v, \
            SchemaError(f"Result '{k}' is missing '{SCHEMA_TYPE_KEY}' key")


def schema_to_columns(schema):
    """
    Get a list of database table columns from a schema

    :param dict schema: schema to parse
    :return list[str]: columns to inialize database table with
    """
    columns = []
    for colname, col_dict in schema[SCHEMA_PROP_KEY].items():
        if col_dict[SCHEMA_TYPE_KEY] not in TABLE_COLS_BY_TYPE:
            _LOGGER.warning(f"'{col_dict[SCHEMA_TYPE_KEY]}' result type defined"
                            f" in schema is not supported")
            continue
        columns.append(TABLE_COLS_BY_TYPE[col_dict[SCHEMA_TYPE_KEY]].format(colname))
    _LOGGER.info(f"Table columns created based on schema: {columns}")
    return columns












