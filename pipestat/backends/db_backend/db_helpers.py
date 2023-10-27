# DB Sepcific imports
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote_plus

try:
    import sqlalchemy.orm
    import sqlmodel.sql.expression
    from sqlmodel.main import SQLModel
    from sqlmodel.sql.expression import SelectOfScalar
    from sqlmodel import and_, or_, Integer, Float, String, Boolean
except:
    pass

from pipestat.exceptions import MissingConfigDataError


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
    ORM: Any,
    statement: Any,
    filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
    json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
) -> Any:
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


def selection_filter(
    ORM: Any,
    statement: Any,
    filter_conditions: Optional[List[Dict[str, Union[str, List[str]]]]] = None,
    bool_operator: Optional[str] = "AND",
) -> Any:
    """
    Return filtered query based on condition.

    :param sqlalchemy.orm.DeclarativeMeta ORM:
    :param sqlalchemy.orm.Query query: takes query
    :param [{key: key,
            operator: operator,
            value: value}]
    filter_conditions:
        - eq for ==
        - lt for <
        - ge for >=
        - in for in_
    :return: query
    """

    if bool_operator.lower() == "or":
        sqlmodel_operator = or_
    elif bool_operator.lower() == "and":
        sqlmodel_operator = and_
    else:
        # Create warning here
        sqlmodel_operator = and_

    def get_nested_column(ORM_column, key_list):
        if len(key_list) == 1:
            return ORM_column[key_list[0]]
        else:
            return get_nested_column(ORM_column[key_list[0]], key_list[1:])

    def define_sqlalchemy_type(value: Any):
        if isinstance(value, Union[list, tuple]):
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
                        getattr(ORM, filter_condition["key"][0], None), filter_condition["key"][1:]
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
