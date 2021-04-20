import logging
from re import findall
from typing import Any, Dict

import jsonschema
import sqlalchemy.orm
from oyaml import safe_load
from psycopg2 import sql
from ubiquerg import expandpath

from .const import *

_LOGGER = logging.getLogger(__name__)


def get_status_table_schema(status_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update and return a status_table_schema based on user-provided status schema

    :param Dict[str, Any] status_schema: status schema provided by the user
    :return Dict[str, Any]: status_schema status table scheme
        to use as a base for status table generation
    """
    defined_status_codes = list(status_schema.keys())
    _, status_table_schema = read_yaml_data(
        path=STATUS_TABLE_SCHEMA, what="status table schema"
    )
    status_table_schema["status"].update({"enum": defined_status_codes})
    _LOGGER.debug(f"Updated status table schema: {status_table_schema}")
    return status_table_schema


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
                    _LOGGER.error(
                        f"Could not cast the result into " f"required type: {str(e)}"
                    )
                else:
                    _LOGGER.debug(
                        f"Casted the reported result into required "
                        f"type: {str(cls_fun)}"
                    )
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


def preprocess_condition_pair(condition, condition_val):
    """
    Preprocess query condition and values to ensure sanity and compatibility

    :param str condition: condition string
    :param tuple condition_val: values to populate condition string with
    :return (psycopg2.sql.SQL, tuple): condition pair
    """

    def _check_semicolon(x):
        """
        recursively check for semicolons in an object

        :param aby x: object to inspect
        :raises ValueError: if semicolon detected
        """
        if isinstance(x, str):
            assert ";" not in x, ValueError(
                f"semicolons are not permitted in condition values: '{str(x)}'"
            )
        if isinstance(x, list):
            list(map(lambda v: _check_semicolon(v), x))

    if condition:
        if not isinstance(condition, str):
            raise TypeError("Condition has to be a string")
        else:
            _check_semicolon(condition)
            placeholders = findall("%s", condition)
            condition = sql.SQL(condition)
        if not condition_val:
            raise ValueError("condition provided but condition_val missing")
        assert isinstance(condition_val, list), TypeError(
            "condition_val has to be a list"
        )
        condition_val = tuple(condition_val)
        assert len(placeholders) == len(condition_val), ValueError(
            f"Number of condition ({len(condition_val)}) values not equal "
            f"number of placeholders in: {condition}"
        )
    return condition, condition_val


def paginate_query(query, offset, limit):
    """
    Apply offset and limit to the query string

    :param sql.SQL query: query string to apply limit and offset to
    :param int offset: offset to apply; no. of records to skip
    :param int limit: limit to apply; max no. of records to return
    :return sql.SQL: a possibly paginated query
    """
    if offset is not None:
        assert isinstance(offset, int), TypeError(
            f"Provided offset ({offset}) must be an int"
        )
        query += sql.SQL(f" OFFSET {offset}")
    if limit is not None:
        assert isinstance(limit, int), TypeError(
            f"Provided limit ({limit}) must be an int"
        )
        query += sql.SQL(f" LIMIT {limit}")
    return query


from typing import Dict, List, Optional, Tuple, Union

from sqlalchemy.orm import DeclarativeMeta, Query


def dynamic_filter(
    ORM: DeclarativeMeta,
    query: Query,
    filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
    json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
) -> sqlalchemy.orm.Query:
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
        try:
            e1, e2, e3 = x
            return e1, e2, e3
        except ValueError:
            raise Exception(f"Invalid tripartite element: {x}")

    if filter_conditions is not None:
        for filter_condition in filter_conditions:
            key, op, value = _unpack_tripartite(filter_condition)
            column = getattr(ORM, key, None)
            if column is None:
                raise Exception(f"Invalid filter column: {key}")
            if op == "in":
                if isinstance(value, list):
                    filt = column.in_(value)
                else:
                    filt = column.in_(value.split(","))
            else:
                try:
                    attr = (
                        list(
                            filter(
                                lambda e: hasattr(column, e % op),
                                ["%s", "%s_", "__%s__"],
                            )
                        )[0]
                        % op
                    )
                except IndexError:
                    raise Exception(f"Invalid filter operator: {op}")
                if value == "null":
                    value = None
                filt = getattr(column, attr)(value)
            query = query.filter(filt)

    if json_filter_conditions is not None:
        for json_filter_condition in json_filter_conditions:
            col, key, value = _unpack_tripartite(json_filter_condition)
            query = query.filter(getattr(ORM, col)[key].astext == value)
    return query
