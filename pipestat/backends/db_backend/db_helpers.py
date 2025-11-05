# DB Sepcific imports
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote_plus

from sqlmodel import Boolean, Float, Integer, String, and_, or_

from pipestat.exceptions import MissingConfigDataError


def construct_db_url(dbconf):
    """Builds database URL from config settings.

    Args:
        dbconf (dict): Database configuration dictionary.

    Returns:
        str: Database URL.

    Raises:
        MissingConfigDataError: If required configuration data is missing.
    """
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


def selection_filter(
    ORM: Any,
    statement: Any,
    filter_conditions: Optional[List[Dict[str, Union[str, List[str]]]]] = None,
    bool_operator: Optional[str] = "AND",
) -> Any:
    """Return filtered query based on condition.

    Args:
        ORM (sqlalchemy.orm.DeclarativeMeta): Sqlalchemy ORM object.
        statement (sqlalchemy.orm.Query): Sqlalchemy select statement (e.g. select([ORM]).
        filter_conditions (list): List of filter conditions with structure:
            [{key: key, operator: operator, value: value}]
            Supported operators:
            - eq for ==
            - lt for <
            - ge for >=
            - in for in_
        bool_operator (str): Boolean operator for combining filter conditions.

    Returns:
        Any: Query statement.
    """

    if bool_operator.lower() == "or":
        sqlmodel_operator = or_
    elif bool_operator.lower() == "and":
        sqlmodel_operator = and_
    else:
        # Create warning here
        sqlmodel_operator = and_

    if filter_conditions is not None:
        filter_list = []
        for filter_condition in filter_conditions:
            if list(filter_condition.keys()) != ["key", "operator", "value"]:
                raise ValueError(
                    "Filter conditions must be a dictionary with keys 'key', 'operator', and 'value'"
                )

            if isinstance(filter_condition["key"], list):
                if len(filter_condition["key"]) == 1:
                    column = getattr(ORM, filter_condition["key"][0], None)
                else:
                    column = get_nested_column(
                        getattr(ORM, filter_condition["key"][0], None),
                        filter_condition["key"][1:],
                    ).astext.cast(define_sqlalchemy_type(filter_condition["value"]))

            elif isinstance(filter_condition["key"], str):
                column = getattr(ORM, filter_condition["key"], None)

            else:
                raise ValueError("Filter condition key must be a string or list of strings")

            op = filter_condition["operator"]
            value = filter_condition["value"]

            if column is None:
                raise ValueError(
                    f"Selected filter column does not exist: {filter_condition['key']}"
                )
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
            filter_list.append(filt)

        statement = statement.where(sqlmodel_operator(*filter_list))

    return statement


def get_nested_column(ORM_column, key_list):
    """Create statement for nested column in the database, column with json content inside.

    Args:
        ORM_column (sqlalchemy.orm.DeclarativeMeta): Column attribute on the current ORM.
        key_list (list): List of keys, e.g. if the column contains a complex object.

    Returns:
        sqlalchemy.orm.DeclarativeMeta: Column attribute of ORM.
    """
    if len(key_list) == 1:
        return ORM_column[key_list[0]]
    else:
        return get_nested_column(ORM_column[key_list[0]], key_list[1:])


def define_sqlalchemy_type(value: Any) -> Any:
    """Determine the type of the column (record) and return the corresponding sqlalchemy type.

    Args:
        value (Any): Value of the column.

    Returns:
        Any: Sqlalchemy type.
    """
    if isinstance(value, (list, tuple)):
        value = value[0]
    if isinstance(value, int):
        return Integer
    elif isinstance(value, float):
        return Float
    elif isinstance(value, str):
        return String
    elif isinstance(value, bool):
        return Boolean
    else:
        raise ValueError(f"Value type {type(value)} not supported")
