import logging
import os
import jsonschema
from .const import *
from ._version import __version__
from .exceptions import *
from ubiquerg import VersionInHelpParser, expandpath
from collections import Mapping
from oyaml import safe_load

_LOGGER = logging.getLogger(__name__)


def build_argparser(desc):
    """
    Builds argument parser.

    :param str desc: additional description to print in help
    :return argparse.ArgumentParser
    """
    banner = "%(prog)s - report pipeline results"
    additional_description = desc
    parser = VersionInHelpParser(version=__version__, description=banner,
                                 epilog=additional_description)
    subparsers = parser.add_subparsers(dest="command")
    sps = {}
    # common arguments
    for cmd, desc in SUBPARSER_MSGS.items():
        sps[cmd] = subparsers.add_parser(cmd, description=desc, help=desc)
        sps[cmd].add_argument(
                "-n", "--namespace", required=True, type=str, metavar="N",
                help="Name of the pipeline to report result for")

    # remove, report and inspect
    for cmd in [REMOVE_CMD, REPORT_CMD, INSPECT_CMD, RETRIEVE_CMD]:
        storage_group = sps[cmd].add_mutually_exclusive_group(required=True)
        storage_group.add_argument(
            "-f", "--results-file", type=str, metavar="F",
            help=f"Path to the YAML file where the results will be stored. "
                 f"This file will be used as {PKG_NAME} backend and to restore"
                 f" the reported results across sesssions")
        storage_group.add_argument(
            "-c", "--database-config", type=str, metavar="C",
            help=f"Path to the YAML file with PostgreSQL database "
                 f"configuration. Please refer to the documentation for the "
                 f"file format requirements.")
        sps[cmd].add_argument(
            "-s", "--schema", required=True if cmd == REPORT_CMD else False,
            type=str, metavar="S", help="Path to the schema that defines the "
                                        "results that can be eported")

    # remove and report
    for cmd in [REMOVE_CMD, REPORT_CMD, RETRIEVE_CMD]:
        sps[cmd].add_argument(
            "-i", "--result-identifier", required=True, type=str, metavar="I",
            help="ID of the result to report; needs to be defined in the schema")
        sps[cmd].add_argument(
            "-r", "--record-identifier", required=True, type=str, metavar="R",
            help="ID of the record to report the result for")

    # report
    sps[REPORT_CMD].add_argument(
            "-v", "--value", required=True, metavar="V",
            help="Value of the result to report")

    sps[REPORT_CMD].add_argument(
            "-o", "--overwrite", action="store_true",
            help="Whether the result should override existing ones in "
                 "case of name clashes")

    sps[REPORT_CMD].add_argument(
            "-t", "--try-convert", action="store_true",
            help="Whether to try to convert the reported value into reqiuired "
                 "class in case it does not meet the schema requirements")

    # inspect
    sps[INSPECT_CMD].add_argument(
            "-d", "--data", action="store_true",
            help="Whether to display the data")

    return parser


def schema_to_columns(schema):
    """
    Get a list of database table columns from a schema

    :param dict schema: schema to parse
    :return list[str]: columns to inial ize database table with
    """
    columns = []
    for colname, col_dict in schema.items():
        if col_dict[SCHEMA_TYPE_KEY] not in TABLE_COLS_BY_TYPE:
            _LOGGER.warning(f"'{col_dict[SCHEMA_TYPE_KEY]}' result type defined"
                            f" in schema is not supported")
            continue
        columns.append(TABLE_COLS_BY_TYPE[col_dict[SCHEMA_TYPE_KEY]].format(colname))
    _LOGGER.info(f"Table columns created based on schema: {columns}")
    return columns


def validate_type(value, schema, strict_type=False):
    """
    Validate reported result against a partial schema, in case of failure try
    to cast the value into the required class.

    Does not support objects of objects.

    :param any value: reported value
    :param dict schema: partial jsonschema schema to validate
        against, e.g. {"type": "integer"}
    :param bool strict_type: whether the value should validate as is
    """
    try:
        jsonschema.validate(value, schema)
    except jsonschema.exceptions.ValidationError as e:
        if strict_type:
            raise
        _LOGGER.debug(f"{str(e)}")
        if schema[SCHEMA_TYPE_KEY] != "object":
            value = CLASSES_BY_TYPE[schema[SCHEMA_TYPE_KEY]](value)
        else:
            for prop, prop_dict in schema[SCHEMA_PROP_KEY].items():
                try:
                    cls_fun = CLASSES_BY_TYPE[prop_dict[SCHEMA_TYPE_KEY]]
                    value[prop] = cls_fun(value[prop])
                except Exception as e:
                    _LOGGER.error(f"Could not cast the result into "
                                  f"required type: {str(e)}")
                else:
                    _LOGGER.debug(f"Casted the reported result into required "
                                  f"type: {str(cls_fun)}")
        jsonschema.validate(value, schema)
    else:
        _LOGGER.debug(f"Value '{value}' validated successfully against a schema")


def read_yaml_data(path, what):
    """
    Safely read YAML file and log message

    :param str path: YAML file to read
    :param str what: context
    :return (str, dict): absolute path to the read file and the read data
    """
    assert isinstance(path, str), TypeError(f"Path is not a string: {path}")
    path = expandpath(path)
    assert os.path.exists(path), FileNotFoundError(f"File not found: {path}")
    _LOGGER.debug(f"Reading {what} from '{path}'")
    with open(path, "r") as f:
        return path, safe_load(f)
