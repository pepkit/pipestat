"""Assorted project utilities"""

import logging
import os
import yaml
import jsonschema
from json import dumps, loads
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import sqlalchemy.orm
import sqlmodel.sql.expression
from sqlmodel.main import SQLModel
from oyaml import safe_load
from sqlmodel.sql.expression import SelectOfScalar
from ubiquerg import expandpath
from urllib.parse import quote_plus

from .const import *
from .exceptions import *

_LOGGER = logging.getLogger(__name__)


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
                    _LOGGER.error(f"Could not cast the result into " f"required type: {str(e)}")
                else:
                    _LOGGER.debug(
                        f"Casted the reported result into required " f"type: {str(cls_fun)}"
                    )
        jsonschema.validate(value, schema)
    else:
        _LOGGER.debug(f"Value '{value}' validated successfully against a schema")


def read_yaml_data(path: Union[str, Path], what: str) -> Tuple[str, Dict[str, Any]]:
    """
    Safely read YAML file and log message

    :param str path: YAML file to read
    :param str what: context
    :return (str, dict): absolute path to the read file and the read data
    """
    if isinstance(path, Path):
        test = lambda p: p.is_file()
    elif isinstance(path, str):
        path = expandpath(path)
        test = os.path.isfile
    else:
        raise TypeError(f"Alleged path to YAML file to read is neither path nor string: {path}")
    assert test(path), FileNotFoundError(f"File not found: {path}")
    _LOGGER.debug(f"Reading {what} from '{path}'")
    with open(path, "r") as f:
        return path, safe_load(f)


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
    raise TypeError(
        f"String or list of strings required as input. Got: " f"{x.__class__.__name__}"
    )


def mk_abs_via_cfg(
    path: Optional[str],
    cfg_path: Optional[str],
) -> Optional[str]:
    """
    Helper function to ensure a path is absolute.

    Assumes a relative path is relative to cfg_path, or to current working directory if cfg_path is None.

    : param str path: The path to make absolute.
    : param str cfg_path: Relative paths will be relative the containing folder of this pat
    """
    if path is None:
        return path
    assert isinstance(path, str), TypeError("Path is expected to be a str")
    if os.path.isabs(path):
        return path
    if cfg_path is None:
        rel_to_cwd = os.path.join(os.getcwd(), path)
        if os.path.exists(rel_to_cwd) or os.access(os.path.dirname(rel_to_cwd), os.W_OK):
            return rel_to_cwd
        else:
            raise OSError(f"File not found: {path}")
    joined = os.path.join(os.path.dirname(cfg_path), path)
    if os.path.isabs(joined):
        return joined
    raise OSError(f"Could not make this path absolute: {path}")


def construct_db_url(dbconf):
    """Builds database URL from config settings"""
    try:
        creds = dict(
            name=dbconf["name"],
            user=dbconf["user"],
            passwd=dbconf["password"],
            host=dbconf["host"],
            port=dbconf["port"],
            dialect=dbconf["dialect"],
            driver=dbconf["driver"],
        )  # driver = sqlite, mysql, postgresql, oracle, or mssql
    except KeyError as e:
        raise MissingConfigDataError(f"Could not determine database URL. Caught error: {str(e)}")
    parsed_creds = {k: quote_plus(str(v)) for k, v in creds.items()}
    return "{dialect}+{driver}://{user}:{passwd}@{host}:{port}/{name}".format(**parsed_creds)


def dynamic_filter(
    ORM: SQLModel,
    statement: SelectOfScalar,
    filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
    json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
) -> sqlmodel.sql.expression.SelectOfScalar:
    """
    Return filtered query based on condition.

    :param sqlalchemy.orm.DeclarativeMeta ORM:
    :param sqlalchemy.orm.Query query: takes query
    :param [(key,operator,value)] filter_conditions: e.g. [("id", "eq", 1)] operator list
        - eq for ==
        - lt for <
        - ge for >=
        - in for in_
        - like for like
    :param [(col,key,value)] json_filter_conditions: conditions for JSONB column to query.
        Only '==' is supported e.g. [("other", "genome", "hg38")]
    :return: query
    """

    def _unpack_tripartite(x):
        if not (isinstance(x, List) or isinstance(x, Tuple)):
            raise TypeError("Wrong filter class; a List or Tuple is required")
        if len(x) != 3:
            raise ValueError(
                f"Invalid filter value: {x}. The filter must be a tripartite iterable"
            )
        return tuple(x)

    if filter_conditions is not None:
        for filter_condition in filter_conditions:
            key, op, value = _unpack_tripartite(filter_condition)
            column = getattr(ORM, key, None)
            if column is None:
                raise ValueError(f"Selected filter column does not exist: {key}")
            if op == "in":
                filt = column.in_(value if isinstance(value, list) else value.split(","))
            else:
                attr = next(
                    filter(lambda a: hasattr(column, a), [op, op + "_", f"__{op}__"]),
                    None,
                )
                if attr is None:
                    raise ValueError(f"Invalid filter operator: {op}")
                if value == "null":
                    value = None
                filt = getattr(column, attr)(value)
            statement = statement.where(filt)

    if json_filter_conditions is not None:
        for json_filter_condition in json_filter_conditions:
            col, key, value = _unpack_tripartite(json_filter_condition)
            statement = statement.where(getattr(ORM, col) == value)

    return statement


def init_generic_config():
    """
    Create generic config file for DB Backend
    """
    try:
        os.makedirs("config")
    except FileExistsError:
        pass

    # Destination one level down from CWD in config folder
    dest_file = os.path.join(os.getcwd(), "config", PIPESTAT_GENERIC_CONFIG)

    # Determine Generic Configuration File
    generic_config_dict = {
        "project_name": "generic_test_name",
        "sample_name": "sample1",
        "schema_path": "sample_output_schema.yaml",
        "database": {
            "dialect": "postgresql",
            "driver": "psycopg2",
            "name": "pipestat-test",
            "user": "postgres",
            "password": "pipestat-password",
            "host": "127.0.0.1",
            "port": 5432,
        },
    }
    # Write file
    if not os.path.exists(dest_file):
        with open(dest_file, "w") as file:
            yaml.dump(generic_config_dict, file)
        print(f"Generic configuration file successfully created at: {dest_file}")
    else:
        print(f"Generic configuration file already exists `{dest_file}`. Skipping creation..")

    return True


def markdown_formatter(pipeline_name, sample_name, res_id, value) -> str:
    """
    Returns Markdown formatted value as string
    """
    if type(value) is not dict:
        nl = "\n"
        rep_strs = [f"`{res_id}`: ```{value}```"]
        formatted_result = (
            f"\n > Reported records for `'{sample_name}'` in `'{pipeline_name}'` {nl} "
            + f"{nl} {(nl).join(rep_strs)}"
        )
    else:
        nl = "\n"
        rep_strs = [f"`{res_id}`:\n ```\n{dumps(value, indent=2)}\n```"]
        formatted_result = (
            f"\n > Reported records for `'{sample_name}'` in `'{pipeline_name}'` {nl} "
            + f"{nl} {(nl).join(rep_strs)}"
        )
    return formatted_result


def default_formatter(pipeline_name, sample_name, res_id, value) -> str:
    """
    Returns formatted value as string
    """
    # Assume default method desired
    nl = "\n"
    rep_strs = [f"{res_id}: {value}"]
    formatted_result = (
        f"Reported records for '{sample_name}' in '{pipeline_name}' "
        + f":{nl} - {(nl + ' - ').join(rep_strs)}"
    )
    return formatted_result
