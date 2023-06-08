import sys
import os
from abc import ABC
from glob import glob
from logging import getLogger
from yacman import YAMLConfigManager
from ubiquerg import create_lock, remove_lock, expandpath
from contextlib import contextmanager

from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine, select as sql_select

from pipestat.const import *
from pipestat.exceptions import *
from pipestat.helpers import *
from .abstract import PipestatBackend

if int(sys.version.split(".")[1]) < 9:
    from typing import List, Dict, Any, Optional, Union
else:
    from typing import *

_LOGGER = getLogger(PKG_NAME)


class DBBackend(PipestatBackend):
    def __init__(
        self,
        record_identifier: Optional[str] = None,
        schema_path: Optional[str] = None,
        project_name: Optional[str] = None,
        config_file: Optional[str] = None,
        config_dict: Optional[dict] = None,
        show_db_logs: bool = False,
        pipeline_type: Optional[str] = False,
        parsed_schema: Optional[str] = None,
        status_schema: Optional[str] = None,
        db_url: Optional[str] = None,
        status_schema_source: Optional[str] = None,
    ):
        """
        Class representing a Database backend
        """
        _LOGGER.warning("Initialize DBBackend")
        self.project_name = project_name
        self.pipeline_type = pipeline_type or "sample"
        self.record_identifier = record_identifier
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.db_url = db_url
        self.show_db_logs = show_db_logs
        self.status_schema_source = status_schema_source

        self.orms = self._create_orms()
        SQLModel.metadata.create_all(self._engine)

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

    def _create_orms(self):
        """Create ORMs."""
        _LOGGER.debug(f"Creating models for '{self.project_name}' table in '{PKG_NAME}' database")
        project_mod = self.parsed_schema.build_project_model()
        samples_mod = self.parsed_schema.build_sample_model()
        if project_mod and samples_mod:
            return {
                self.parsed_schema.sample_table_name: samples_mod,
                self.parsed_schema.project_table_name: project_mod,
            }
        elif samples_mod:
            return {self.project_name: samples_mod}
        elif project_mod:
            return {self.project_name: project_mod}
        else:
            raise SchemaError(
                f"Neither project nor samples model could be built from schema source: {self.status_schema_source}"
            )

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str,
        pipeline_type: Optional[str] = None,
        force_overwrite: bool = False,
    ) -> None:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        :param str pipeline_type: "sample" or "project"
        :param bool force_overwrite: force overwriting of results, defaults to False.
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier
        result_identifiers = list(values.keys())
        self.assert_results_defined(results=result_identifiers, pipeline_type=pipeline_type)

        table_name = self.get_table_name(pipeline_type=pipeline_type)

        existing = self.list_results(
            record_identifier=record_identifier,
            restrict_to=result_identifiers,
            pipeline_type=pipeline_type,
        )
        if existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(f"These results exist for '{record_identifier}': {existing_str}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")
        try:
            ORMClass = self.get_orm(table_name=table_name)
            values.update({RECORD_ID: record_identifier})
            values.update({"project_name": self.project_name})

            if not self.check_record_exists(
                record_identifier=record_identifier, table_name=table_name
            ):
                new_record = ORMClass(**values)
                with self.session as s:
                    s.add(new_record)
                    s.commit()
                    returned_id = new_record.id

            else:
                with self.session as s:
                    record_to_update = (
                        s.query(ORMClass)
                        .filter(getattr(ORMClass, RECORD_ID) == record_identifier)
                        .first()
                    )
                    for result_id, result_value in values.items():
                        setattr(record_to_update, result_id, result_value)
                    s.commit()
                    returned_id = record_to_update.id
            _LOGGER.warning(returned_id)
            return returned_id
        except Exception as e:
            _LOGGER.error(f"Could not insert the result into the database. Exception: {e}")
            raise

    def retrieve(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> Union[Any, Dict[str, Any]]:
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :param str pipeline_type: "sample" or "project"
        :return any | Dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier
        tn = self.get_table_name(pipeline_type=pipeline_type)

        if result_identifier is not None:
            existing = self.list_results(
                record_identifier=record_identifier,
                restrict_to=[result_identifier],
                pipeline_type=pipeline_type,
            )
            if not existing:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record " f"'{record_identifier}'"
                )

        with self.session as s:
            record = (
                s.query(self.get_orm(table_name=tn))
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
        raise PipestatDatabaseError(f"Record '{record_identifier}' not found")

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :param str pipeline_type: "sample" or "project"
        :return bool: whether the result has been removed
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier

        rm_record = True if result_identifier is None else False

        table_name = self.get_table_name(pipeline_type=pipeline_type)

        if not self.check_record_exists(
            record_identifier=record_identifier,
            table_name=table_name,
        ):
            _LOGGER.error(f"Record '{record_identifier}' not found")
            return False

        if result_identifier and not self.check_result_exists(
            result_identifier=result_identifier,
            record_identifier=record_identifier,
            pipeline_type=pipeline_type,
        ):
            _LOGGER.error(f"'{result_identifier}' has not been reported for '{record_identifier}'")
            return False

        try:
            ORMClass = self.get_orm(table_name=table_name)
            if self.check_record_exists(
                record_identifier=record_identifier, table_name=table_name
            ):
                with self.session as s:
                    records = s.query(ORMClass).filter(
                        getattr(ORMClass, RECORD_ID) == record_identifier
                    )

                    if rm_record is True:
                        self.remove_record(
                            record_identifier=record_identifier,
                            pipeline_type=pipeline_type,
                            rm_record=rm_record,
                        )
                    else:
                        if not self.check_result_exists(
                            record_identifier=record_identifier,
                            result_identifier=result_identifier,
                            pipeline_type=pipeline_type,
                        ):
                            raise PipestatDatabaseError(
                                f"Result '{result_identifier}' not found for record "
                                f"'{record_identifier}'"
                            )
                        setattr(records.first(), result_identifier, None)
                    s.commit()
            else:
                raise PipestatDatabaseError(f"Record '{record_identifier}' not found")
        except Exception as e:
            _LOGGER.error(f"Could not remove the result from the database. Exception: {e}")
            raise

        return True

    def remove_record(
        self,
        record_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        rm_record: Optional[bool] = False,
    ) -> bool:
        """
        Remove a record, requires rm_record to be True

        :param str record_identifier: unique identifier of the record
        :param str pipeline_type: "sample" or "project"
        :param bool rm_record: bool for removing record.
        :return bool: whether the result has been removed
        """
        pipeline_type = pipeline_type or self.pipeline_type
        table_name = self.get_table_name(pipeline_type=pipeline_type)
        record_identifier = record_identifier or self.record_identifier
        if rm_record:
            try:
                ORMClass = self.get_orm(table_name=table_name)
                if self.check_record_exists(
                    record_identifier=record_identifier, table_name=table_name
                ):
                    with self.session as s:
                        records = s.query(ORMClass).filter(
                            getattr(ORMClass, RECORD_ID) == record_identifier
                        )
                        records.delete()
                        s.commit()
                else:
                    raise PipestatDatabaseError(f"Record '{record_identifier}' not found")
            except Exception as e:
                _LOGGER.error(f"Could not remove the result from the database. Exception: {e}")
                raise
        else:
            _LOGGER.info(f" rm_record flag False, aborting Removing '{record_identifier}' record")

    def get_model(self, table_name: str, strict: bool):
        """
        Get model based on table_name
        :param str table_name: "sample" or "project"
        :return model
        """
        orms = self.orms

        mod = orms.get(table_name)

        if strict and mod is None:
            raise PipestatDatabaseError(
                f"No object relational mapper class defined for table '{table_name}'. "
                f"{len(orms)} defined: {', '.join(orms.keys())}"
            )
        return mod

    def get_orm(self, table_name: str) -> Any:
        """
        Get an object relational mapper class

        :param str table_name: table name to get a class for
        :return Any: Object relational mapper class
        """
        if self.orms is None:
            raise PipestatDatabaseError("Object relational mapper classes not defined")
        mod = self.get_model(table_name=table_name, strict=True)
        return mod

    def get_one_record(self, table_name: str, rid: Optional[str] = None):
        """
        Retrieve single record from SQL table

        :param str rid: record to check for
        :param str table_name: table name to check
        :return Any: Record object
        """

        models = [self.get_orm(table_name=table_name)] if table_name else list(self.orms.values())
        with self.session as s:
            for mod in models:
                stmt = sql_select(mod).where(mod.record_identifier == rid)
                record = s.exec(stmt).first()

                if record:
                    return record

    def get_table_name(self, pipeline_type: Optional[str] = None):
        """
        Get tablename based on pipeline_type
        :param str pipeline_type: "sample" or "project"
        :return str table name: "pipeline_id__sample" or "pipeline_id__project"
        """

        pipeline_type = pipeline_type or self.pipeline_type

        mods = self.orms
        if len(mods) == 1:
            return list(mods.keys())[0]
        elif len(mods) == 2:
            if pipeline_type is None:
                raise Exception(
                    f"Cannot determine table suffix with 2 models present and no project-level flag"
                )
            prelim = (
                self.parsed_schema.project_table_name
                if pipeline_type == "project"
                else self.parsed_schema.sample_table_name
            )
            if prelim in mods:
                return prelim
            raise Exception(
                f"Determined table name '{prelim}', which is not stored among these: {', '.join(mods.keys())}"
            )
        raise Exception(f"Cannot determine table suffix with {len(mods)} model(s) present.")

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

    def count_records(self, pipeline_type: Optional[str] = None):
        """
        Count rows in a selected table
        :param str pipeline_type: sample vs project designator needed to count records in table
        :return int: number of records
        """
        pipeline_type = pipeline_type or self.pipeline_type
        table_name = self.get_table_name(pipeline_type)
        mod = self.get_model(table_name=table_name, strict=True)
        with self.session as s:
            stmt = sql_select(mod)
            records = s.exec(stmt).all()
            return len(records)

    def list_results(
        self,
        restrict_to: Optional[List[str]] = None,
        record_identifier: str = None,
        pipeline_type: str = None,
    ) -> List[str]:
        """
        Check if the specified results exist in the table

        :param List[str] restrict_to: results identifiers to check for
        :param str record_identifier: record to check for
        :param str pipeline_type: "sample" or "project"
        :return List[str] existing: if no result identifier specified, return all results for the record
        :return List[str]: results identifiers that exist
        """
        # rid = self._strict_record_id(rid)
        table_name = self.get_table_name(pipeline_type=pipeline_type)
        rid = record_identifier
        record = self.get_one_record(rid=rid, table_name=table_name)

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

    def select(
        self,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
        json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        pipeline_type: Optional[str] = None,
    ) -> List[Any]:
        """
        Perform a `SELECT` on the table

        :param str table_name: name of the table to SELECT from
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
        :param str pipeline_type: "sample" or "project"
        """
        pipeline_type = pipeline_type or self.pipeline_type
        table_name = table_name or self.get_table_name(pipeline_type)

        ORM = self.get_orm(table_name=table_name)

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
        table_name: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        pipeline_type: Optional[str] = None,
    ) -> List[Any]:
        """
        Execute a query with a textual filter. Returns all results.

        To retrieve all table contents, leave the filter arguments out.
        Table name uses pipeline_type

        :param str filter_templ: filter template with value placeholders,
             formatted as follows `id<:value and name=:name`
        :param Dict[str, Any] filter_params: a mapping keys specified in the `filter_templ`
            to parameters that are supposed to replace the placeholders
        :param str table_name: name of the table to query
        :param int offset: skip this number of rows
        :param int limit: include this number of rows
        :param str pipeline_type: sample vs project pipeline
        :return List[Any]: a list of matched records
        """
        pipeline_type = pipeline_type or self.pipeline_type
        table_name = table_name or self.get_table_name(pipeline_type)
        ORM = self.get_orm(table_name=table_name)
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
        table_name,
        columns,
        pipeline_type: Optional[str] = None,
    ) -> List[Any]:
        """
        Perform a `SELECT DISTINCT` on given table and column

        :param str table_name: name of the table to SELECT from
        :param List[str] columns: columns to include in the result
        :param str pipeline_type: "sample" or "project"
        :return List[Any]: returns distinct values.
        """
        pipeline_type = pipeline_type or self.pipeline_type
        table_name = table_name or self.get_table_name(pipeline_type)
        ORM = self.get_orm(table_name=table_name)
        with self.session as s:
            query = s.query(*[getattr(ORM, column) for column in columns])
            query = query.distinct()
            result = query.all()
        return result

    def set_status(
        self,
        status_identifier: str,
        record_identifier: str = None,
        pipeline_type: Optional[str] = None,
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
        :param str pipeline_type: whether status is being set for a project-level pipeline, or sample-level
        """
        pipeline_type = pipeline_type or self.pipeline_type
        table_name = self.get_table_name(pipeline_type)
        record_identifier = record_identifier or self.record_identifier
        # r_id = self._strict_record_id(record_identifier)
        known_status_identifiers = self.status_schema.keys()
        if status_identifier not in known_status_identifiers:
            raise PipestatError(
                f"'{status_identifier}' is not a defined status identifier. "
                f"These are allowed: {known_status_identifiers}"
            )
        prev_status = self.get_status(record_identifier, pipeline_type)
        try:
            self.report(
                values={STATUS: status_identifier},
                record_identifier=record_identifier,
                pipeline_type=pipeline_type,
            )
        except Exception as e:
            _LOGGER.error(
                f"Could not insert into the status table ('{table_name}'). Exception: {e}"
            )
            raise
        if prev_status:
            _LOGGER.debug(f"Changed status from '{prev_status}' to '{status_identifier}'")

    def get_status(
        self, record_identifier: str, pipeline_type: Optional[str] = None
    ) -> Optional[str]:
        """
        Get pipeline status

        :param str record_identifier: record identifier to set the
            pipeline status for
        :param str pipeline_type: whether status is being set for a project-level pipeline, or sample-level
        :return str status
        """
        pipeline_type = pipeline_type or self.pipeline_type
        try:
            result = self.retrieve(
                result_identifier=STATUS,
                record_identifier=record_identifier,
                pipeline_type=pipeline_type,
            )
        except PipestatDatabaseError:
            return None
        return result

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
