# DB Sepcific imports
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote_plus
import json

try:
    import sqlalchemy.orm
    import sqlmodel.sql.expression
    from sqlmodel.main import SQLModel
    from sqlmodel.sql.expression import SelectOfScalar
    from sqlmodel import and_, or_
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
    filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
    json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
    bool_operator: Optional[str] = "AND",
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

    if bool_operator.lower() == "or":
        sqlmodel_operator = or_
    elif bool_operator.lower() == "and":
        sqlmodel_operator = and_
    else:
        # Create warning here
        sqlmodel_operator = and_


    def _unpack_tripartite(x):
        if not (isinstance(x, List) or isinstance(x, Tuple)):
            raise TypeError("Wrong filter class; a List or Tuple is required")
        if len(x) != 3:
            raise ValueError(
                f"Invalid filter value: {x}. The filter must be a tripartite iterable"
            )
        return tuple(x)

    if filter_conditions is not None:
        filter_list = []
        for filter_condition in filter_conditions: # These are ANDs
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
            filter_list.append(filt)

        statement = statement.where(sqlmodel_operator(*filter_list))

    if json_filter_conditions is not None:
        for json_filter_condition in json_filter_conditions: #These are ANDs
            col, key, value = _unpack_tripartite(json_filter_condition)
            column = getattr(ORM, col)
            #statement = statement.where(getattr(ORM, col) == value)

            # This needs error handling so that the user understands when they give it a string that cannot be converted
            value = json.loads(value)
            statement = statement.where(column.contains(value))
            print(statement)

    return statement