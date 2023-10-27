import sys
import datetime


from logging import getLogger

from contextlib import contextmanager

# try:
from sqlalchemy import text
from sqlmodel import Session, create_engine, select as sql_select

# except:
#     pass

from pipestat.helpers import *
from pipestat.backends.db_backend.db_helpers import *
from pipestat.backends.abstract import PipestatBackend

if int(sys.version.split(".")[1]) < 9:
    from typing import List, Dict, Any, Optional, Union
else:
    from typing import *

_LOGGER = getLogger(PKG_NAME)


class DBBackend(PipestatBackend):
    def __init__(
        self,
        record_identifier: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        show_db_logs: bool = False,
        pipeline_type: Optional[str] = False,
        parsed_schema: Optional[str] = None,
        status_schema: Optional[str] = None,
        db_url: Optional[str] = None,
        status_schema_source: Optional[dict] = None,
        result_formatter: Optional[staticmethod] = None,
    ):
        """
        Class representing a Database backend
        :param str record_identifier: record identifier to report for. This
            creates a weak bound to the record, which can be overridden in
            this object method calls
        :param project_name: project name associated with the record
        :param str pipeline_name: name of pipeline associated with result
        :param str show_db_logs: Defaults to False, toggles showing database logs
        :param str pipeline_type: "sample" or "project"
        :param str parsed_schema: results output schema. Used to construct DB columns.
        :param str status_schema: schema containing pipeline statuses e.g. 'running'
        :param str db_url: url used for connection to Postgres DB
        :param dict status_schema_source: filepath of status schema
        :param str result_formatter: function for formatting result
        """
        _LOGGER.warning(f"Initializing DBBackend for pipeline '{pieline_name}'")
        self.pipeline_name = pipeline_name
        self.pipeline_type = pipeline_type or "sample"
        self.record_identifier = record_identifier
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.db_url = db_url
        self.show_db_logs = show_db_logs
        self.status_schema_source = status_schema_source
        self.result_formatter = result_formatter

        self.orms = self._create_orms(pipeline_type=pipeline_type)
        self.table_name = list(self.orms.keys())[0]
        SQLModel.metadata.create_all(self._engine)

    def check_record_exists(
        self,
        record_identifier: str,
        table_name: str,
    ) -> bool:
        """
        Check if the specified record exists in the table

        :param str record_identifier: record to check for
        :param str table_name: table name to check
        :return bool: whether the record exists in the table
        """

        query_hit = self.get_one_record(rid=record_identifier, table_name=table_name)
        return query_hit is not None

    def count_records(self):
        """
        Count rows in a selected table
        :return int: number of records
        """

        mod = self.get_model(table_name=self.table_name)
        with self.session as s:
            stmt = sql_select(mod)
            records = s.exec(stmt).all()
            return len(records)

    def get_model(self, table_name: str):
        """
        Get model based on table_name
        :param str table_name: pipelinename__sample or pipelinename__project
        :return mod: model/orm associated with the table name
        """
        if self.orms is None:
            raise PipestatDatabaseError("Object relational mapper classes not defined.")

        mod = self.orms.get(table_name)

        if mod is None:
            raise PipestatDatabaseError(
                f"No object relational mapper class defined for table '{table_name}'. "
                f"{len(self.orms)} defined: {', '.join(self.orms.keys())}"
            )
        return mod

    def get_one_record(
        self,
        table_name: str,
        rid: Optional[str] = None,
    ):
        """
        Retrieve single record from SQL table

        :param str table_name: table name to check
        :param str rid: record to check for
        :return Any: Record object
        """

        models = (
            [self.get_model(table_name=table_name)] if table_name else list(self.orms.values())
        )
        with self.session as s:
            for mod in models:
                stmt = sql_select(mod).where(mod.record_identifier == rid)
                record = s.exec(stmt).first()

                if record:
                    return record

    def get_records(
        self,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0,
    ) -> Optional[dict]:
        """Returns list of records

        :param int limit: limit number of records to this amount
        :param int offset: offset records by this amount
        :return dict records_dict: dictionary of records
        {
          "count": x,
          "limit": l,
          "offset": o,
          "records": [...]
        }
        """

        mod = self.get_model(table_name=self.table_name)

        with self.session as s:
            total_count = len(s.exec(sql_select(mod)).all())
            sample_list = []
            stmt = sql_select(mod).offset(offset).limit(limit)
            records = s.exec(stmt).all()
            for i in records:
                sample_list.append(i.record_identifier)

        records_dict = {
            "count": total_count,
            "limit": limit,
            "offset": offset,
            "records": sample_list,
        }

        return records_dict

    def get_status(self, record_identifier: str) -> Optional[str]:
        """
        Get pipeline status

        :param str record_identifier: record identifier to set the
            pipeline status for
        :return str status
        """

        try:
            result = self.retrieve(
                result_identifier=STATUS,
                record_identifier=record_identifier,
            )
        except RecordNotFoundError:
            return None
        return result

    def list_recent_results(
        self,
        limit: Optional[int] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        type: Optional[str] = None,
    ) -> Optional[dict]:
        """Lists recent results based on start and end time filter
        :param int  limit: limit number of results returned
        :param datetime.datetime start: most recent result to filter on, defaults to now, e.g. 2023-10-16 13:03:04.680400
        :param datetime.datetime end: oldest result to filter on, e.g. 1970-10-16 13:03:04.680400
        :param str type: created or modified
        :return dict results: a dict containing start, end, num of records, and list of retrieved records
        """
        mod = self.get_model(table_name=self.table_name)

        with self.session as s:
            records_list = []
            if type == "modified":
                stmt = (
                    sql_select(mod)
                    .where(mod.pipestat_modified_time <= start)
                    .where(mod.pipestat_modified_time >= end)
                    .limit(limit)
                )
            else:
                stmt = (
                    sql_select(mod)
                    .where(mod.pipestat_created_time <= start)
                    .where(mod.pipestat_created_time >= end)
                    .limit(limit)
                )
            records = s.exec(stmt).all()
            if records:
                for i in reversed(records):
                    if type == "modified":
                        records_list.append((i.record_identifier, i.pipestat_modified_time))
                    else:
                        records_list.append((i.record_identifier, i.pipestat_created_time))

        records_dict = {
            "count": len(records),
            "start": start,
            "end": end,
            "type": type,
            "records": records_list,
        }

        return records_dict

    def list_results(
        self,
        restrict_to: Optional[List[str]] = None,
        record_identifier: str = None,
    ) -> List[str]:
        """
        Check if the specified results exist in the table

        :param List[str] restrict_to: results identifiers to check for
        :param str record_identifier: record to check for
        :return List[str] existing: if no result identifier specified, return all results for the record
        :return List[str]: results identifiers that exist
        """

        rid = record_identifier
        record = self.get_one_record(rid=rid, table_name=self.table_name)

        if restrict_to is None:
            return (
                [
                    key
                    for key in self.parsed_schema.results_data.keys()
                    if getattr(record, key, None) is not None
                ]
                if record
                else []
            )
        else:
            return (
                [r for r in restrict_to if getattr(record, r, None) is not None] if record else []
            )

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :return bool: whether the result has been removed
        """

        record_identifier = record_identifier or self.record_identifier

        rm_record = True if result_identifier is None else False

        if not self.check_record_exists(
            record_identifier=record_identifier,
            table_name=self.table_name,
        ):
            _LOGGER.error(f"Record '{record_identifier}' not found")
            return False

        if result_identifier and not self.check_result_exists(
            result_identifier=result_identifier,
            record_identifier=record_identifier,
        ):
            _LOGGER.error(f"'{result_identifier}' has not been reported for '{record_identifier}'")
            return False

        try:
            ORMClass = self.get_model(table_name=self.table_name)
            if self.check_record_exists(
                record_identifier=record_identifier,
                table_name=self.table_name,
            ):
                with self.session as s:
                    records = s.query(ORMClass).filter(
                        getattr(ORMClass, "record_identifier") == record_identifier
                    )
                    if rm_record is True:
                        self.remove_record(
                            record_identifier=record_identifier,
                            rm_record=rm_record,
                        )
                    else:
                        if not self.check_result_exists(
                            record_identifier=record_identifier,
                            result_identifier=result_identifier,
                        ):
                            raise RecordNotFoundError(
                                f"Result '{result_identifier}' not found for record "
                                f"'{record_identifier}'"
                            )
                        setattr(records.first(), result_identifier, None)
                    s.commit()
            else:
                raise RecordNotFoundError(f"Record '{record_identifier}' not found")
        except Exception as e:
            _LOGGER.error(f"Could not remove the result from the database. Exception: {e}")
            raise

        return True

    def remove_record(
        self,
        record_identifier: Optional[str] = None,
        rm_record: Optional[bool] = False,
    ) -> bool:
        """
        Remove a record, requires rm_record to be True

        :param str record_identifier: unique identifier of the record
        :param bool rm_record: bool for removing record.
        :return bool: whether the result has been removed
        """

        record_identifier = record_identifier or self.record_identifier
        if rm_record:
            try:
                ORMClass = self.get_model(table_name=self.table_name)
                if self.check_record_exists(
                    record_identifier=record_identifier,
                    table_name=self.table_name,
                ):
                    with self.session as s:
                        records = s.query(ORMClass).filter(
                            getattr(ORMClass, "record_identifier") == record_identifier
                        )
                        records.delete()
                        s.commit()
                else:
                    raise RecordNotFoundError(f"Record '{record_identifier}' not found")
            except Exception as e:
                _LOGGER.error(f"Could not remove the result from the database. Exception: {e}")
                raise
        else:
            _LOGGER.info(f" rm_record flag False, aborting Removing '{record_identifier}' record")

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str,
        force_overwrite: bool = False,
        result_formatter: Optional[staticmethod] = None,
    ) -> Union[List[str], bool]:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        :param str record_identifier: unique identifier of the record
        :param bool force_overwrite: force overwriting of results, defaults to False.
        :param str result_formatter: function for formatting result
        :return list[str] list results_formatted | bool: return list of formatted string
        """

        record_identifier = record_identifier or self.record_identifier

        result_formatter = result_formatter or self.result_formatter
        results_formatted = []
        result_identifiers = list(values.keys())
        if self.parsed_schema is None:
            raise SchemaNotFoundError("DB Backend report results requires schema")
        self.assert_results_defined(results=result_identifiers, pipeline_type=self.pipeline_type)

        existing = self.list_results(
            record_identifier=record_identifier,
            restrict_to=result_identifiers,
        )
        if existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(f"These results exist for '{record_identifier}': {existing_str}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")

        try:
            ORMClass = self.get_model(table_name=self.table_name)
            values.update({RECORD_IDENTIFIER: record_identifier})

            if not self.check_record_exists(
                record_identifier=record_identifier,
                table_name=self.table_name,
            ):
                current_time = datetime.datetime.now()
                values.update({CREATED_TIME: current_time})
                values.update({MODIFIED_TIME: current_time})
                new_record = ORMClass(**values)
                with self.session as s:
                    s.add(new_record)
                    s.commit()
            else:
                with self.session as s:
                    record_to_update = (
                        s.query(ORMClass)
                        .filter(getattr(ORMClass, RECORD_IDENTIFIER) == record_identifier)
                        .first()
                    )
                    values.update({MODIFIED_TIME: datetime.datetime.now()})
                    for result_id, result_value in values.items():
                        setattr(record_to_update, result_id, result_value)
                    s.commit()

            for res_id, val in values.items():
                results_formatted.append(
                    result_formatter(
                        pipeline_name=self.pipeline_name,
                        record_identifier=record_identifier,
                        res_id=res_id,
                        value=val,
                    )
                )
            return results_formatted
        except Exception as e:
            _LOGGER.error(f"Could not insert the result into the database. Exception: {e}")
            raise

    def retrieve(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
    ) -> Union[Any, Dict[str, Any]]:
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :return any | Dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """

        record_identifier = record_identifier or self.record_identifier

        if result_identifier is not None:
            existing = self.list_results(
                record_identifier=record_identifier,
                restrict_to=[result_identifier],
            )
            if not existing:
                raise RecordNotFoundError(
                    f"Result '{result_identifier}' not found for record " f"'{record_identifier}'"
                )

        with self.session as s:
            record = (
                s.query(self.get_model(table_name=self.table_name))
                .filter_by(record_identifier=record_identifier)
                .first()
            )

        if record is not None:
            if result_identifier is not None:
                return getattr(record, result_identifier)
            return {
                column: getattr(record, column)
                for column in [c.name for c in record.__table__.columns]
                if getattr(record, column, None) is not None
            }
        raise RecordNotFoundError(f"Record '{record_identifier}' not found")

    def retrieve_multiple(
        self,
        record_identifier: Optional[List[str]] = None,
        result_identifier: Optional[List[str]] = None,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0,
    ) -> Union[Any, Dict[str, Any]]:
        """
        :param List[str] record_identifier: list of record identifiers
        :param List[str] result_identifier: list of result identifiers to be retrieved
        :param int limit: limit number of records to this amount
        :param int offset: offset records by this amount
        :return Dict[str, any]: a mapping with filtered results reported for the record
        """

        record_list = []

        if result_identifier == []:
            result_identifier = None
        if record_identifier == []:
            record_identifier = None

        ORM = self.get_model(table_name=self.table_name)

        if record_identifier is not None:
            for r_id in record_identifier:
                filter = [("record_identifier", "eq", r_id)]
                result = self.select(
                    columns=result_identifier, filter_conditions=filter, limit=limit, offset=offset
                )
                retrieved_record = {}
                result_dict = dict(result[0])
                for k, v in list(result_dict.items()):
                    if k not in self.parsed_schema.results_data.keys():
                        result_dict.pop(k)
                retrieved_record.update({r_id: result_dict})
                record_list.append(retrieved_record)
        if record_identifier is None:
            if result_identifier is not None:
                result_identifier = ["record_identifier"] + result_identifier
            record_list = []
            records = self.select(
                columns=result_identifier, filter_conditions=None, limit=limit, offset=offset
            )
            for record in records:
                retrieved_record = {}
                r_id = record.record_identifier
                record_dict = dict(record)
                for k, v in list(record_dict.items()):
                    if k not in self.parsed_schema.results_data.keys():
                        record_dict.pop(k)
                retrieved_record.update({r_id: record_dict})
                record_list.append(retrieved_record)

        records_dict = {
            "count": len(record_list),
            "limit": limit,
            "offset": offset,
            "record_identifiers": record_identifier,
            "result_identifiers": result_identifier
            or list(self.parsed_schema.results_data.keys()) + [CREATED_TIME] + [MODIFIED_TIME],
            "records": record_list,
        }

        return records_dict

    def select(
        self,
        columns: Optional[List[str]] = None,
        filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
        json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Any]:
        """
        Perform a `SELECT` on the table

        :param List[str] columns: columns to include in the result
        :param [(key,operator,value)] filter_conditions: e.g. [("id", "eq", 1)], operator list:
            - eq for ==
            - lt for <
            - ge for >=
            - in for in_
            - like for like
        :param [(col,key,value)] json_filter_conditions: conditions for JSONB column to
            query that include JSON column name, key withing the JSON object in that
            column and the value to check the identity against. Therefore only '==' is
            supported in non-nested checks, e.g. [("other", "genome", "hg38")]
        :param int offset: skip this number of rows
        :param int limit: include this number of rows
        """

        ORM = self.get_model(table_name=self.table_name)

        with self.session as s:
            if columns is not None:
                statement = sqlmodel.select(*[getattr(ORM, column) for column in columns])
            else:
                statement = sqlmodel.select(ORM)

            statement = dynamic_filter(
                ORM=ORM,
                statement=statement,
                filter_conditions=filter_conditions,
                json_filter_conditions=json_filter_conditions,
            )
            if isinstance(offset, int):
                statement = statement.offset(offset)
            if isinstance(limit, int):
                statement = statement.limit(limit)
            results = s.exec(statement)
            result = results.all()

        return result

    def select_txt(
        self,
        columns: Optional[List[str]] = None,
        filter_templ: Optional[str] = "",
        filter_params: Optional[Dict[str, Any]] = {},
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Any]:
        """
        Execute a query with a textual filter. Returns all results.

        To retrieve all table contents, leave the filter arguments out.
        Table name uses pipeline_type

        :param List[str] columns: columns to include in the result
        :param str filter_templ: filter template with value placeholders,
             formatted as follows `id<:value and name=:name`
        :param Dict[str, Any] filter_params: a mapping keys specified in the `filter_templ`
            to parameters that are supposed to replace the placeholders
        :param int offset: skip this number of rows
        :param int limit: include this number of rows
        :return List[Any]: a list of matched records
        """

        ORM = self.get_model(table_name=self.table_name)
        with self.session as s:
            if columns is not None:
                q = (
                    s.query(*[getattr(ORM, column) for column in columns])
                    .filter(text(filter_templ))
                    .params(**filter_params)
                )
            else:
                q = s.query(ORM).filter(text(filter_templ)).params(**filter_params)
            if isinstance(offset, int):
                q = q.offset(offset)
            if isinstance(limit, int):
                q = q.limit(limit)
            results = q.all()
        return results

    def select_distinct(
        self,
        columns,
    ) -> List[Tuple]:
        """
        Perform a `SELECT DISTINCT` on given table and column

        :param List[str] columns: columns to include in the result
        :return List[Tuple]: returns distinct values.
        """

        ORM = self.get_model(table_name=self.table_name)
        with self.session as s:
            query = s.query(*[getattr(ORM, column) for column in columns])
            query = query.distinct()
            result = query.all()
        return result

    def set_status(
        self,
        status_identifier: str,
        record_identifier: str = None,
    ) -> None:
        """
        Set pipeline run status.

        The status identifier needs to match one of identifiers specified in
        the status schema. A basic, ready to use, status schema is shipped with
        this package.

        :param str status_identifier: status to set, one of statuses defined
            in the status schema
        :param str record_identifier: record identifier to set the
            pipeline status for
        """

        record_identifier = record_identifier or self.record_identifier
        known_status_identifiers = self.status_schema.keys()
        if status_identifier not in known_status_identifiers:
            raise UnrecognizedStatusError(
                f"'{status_identifier}' is not a defined status identifier. "
                f"These are allowed: {known_status_identifiers}"
            )
        prev_status = self.get_status(record_identifier)
        try:
            self.report(
                values={STATUS: status_identifier},
                record_identifier=record_identifier,
            )
        except Exception as e:
            _LOGGER.error(
                f"Could not insert into the status table ('{self.table_name}'). Exception: {e}"
            )
            raise
        if prev_status:
            _LOGGER.debug(f"Changed status from '{prev_status}' to '{status_identifier}'")

    def _create_orms(self, pipeline_type):
        """Create ORMs."""
        _LOGGER.debug(f"Creating models for '{self.pipeline_name}' table in '{PKG_NAME}' database")
        model = self.parsed_schema.build_model(pipeline_type=pipeline_type)
        table_name = self.parsed_schema._table_name(pipeline_type)
        # TODO reconsider line below. Why do we need to return a dict?
        if model:
            return {table_name: model}
        else:
            raise SchemaError(
                f"Neither project nor samples model could be built from schema source: {self.status_schema_source}"
            )

    @property
    def _engine(self):
        """Access the database engine backing this manager."""
        try:
            return self.db_engine_key
        except AttributeError:
            # Do it this way rather than .setdefault to avoid evaluating
            # the expression for the default argument (i.e., building
            # the engine) if it's not necessary.
            self.db_engine_key = create_engine(self.db_url, echo=self.show_db_logs)
            return self.db_engine_key

    @property
    @contextmanager
    def session(self):
        """
        Provide a transactional scope around a series of query
        operations.
        """
        session = Session(self._engine)
        _LOGGER.debug("Created session")
        try:
            yield session
        except:
            _LOGGER.info("session.rollback")
            session.rollback()
            raise
        finally:
            _LOGGER.info("session.close")
            session.close()
        _LOGGER.debug("Ending session")
