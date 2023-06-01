import sys
import os
from abc import ABC
from glob import glob
from logging import getLogger
from yacman import YAMLConfigManager
from ubiquerg import create_lock, remove_lock, expandpath
from contextlib import contextmanager

from sqlmodel import Session, select as sql_select

from .const import *
from .exceptions import *
from .helpers import *

if int(sys.version.split(".")[1]) < 9:
    from typing import List, Dict, Any, Optional, Union
else:
    from typing import *

_LOGGER = getLogger(PKG_NAME)


def set_var_priority(func):
    """Decorator to set variable priority."""

    def inner_func(self, *args, **kwargs):
        for i in args.items():
            if args[i] in ["pipeline_type", "record_identifier"]:
                args[i] = args[i] or getattr(self, args[i])
        return func(self, *args, **kwargs)

    return inner_func


class PipestatBackend(ABC):
    """Abstract class representing a pipestat backend"""

    def __init__(self, pipeline_type):
        _LOGGER.warning("Initialize PipestatBackend")
        self.pipeline_type = pipeline_type

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str = None,
        force_overwrite: bool = False,
        strict_type: bool = True,
        return_id: bool = False,
        pipeline_type: Optional[str] = None,
    ) -> Union[bool, int]:
        _LOGGER.warning("report not implemented yet for this backend")

    def check_result_exists(
        self,
        result_identifier: str,
        record_identifier: str = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the result has been reported

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to check
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        # record_identifier = self._strict_record_id(record_identifier)
        return (
            len(
                self.check_which_results_exist(
                    results=[result_identifier],
                    result_identifier=record_identifier,
                    pipeline_type=pipeline_type,
                )
            )
            > 0
        )

    def check_which_results_exist(self) -> List[str]:
        pass

    def assert_results_defined(self, results: List[str], pipeline_type: str) -> None:
        """
        Assert provided list of results is defined in the schema

        :param List[str] results: list of results to
            check for existence in the schema
        :raises SchemaError: if any of the results is not defined in the schema
        """

        # take project level input and look for keys in the specific schema.
        # warn if you are trying to report a sample to a project level and vice versa.

        if pipeline_type == "sample":
            known_results = self.parsed_schema.sample_level_data.keys()
        if pipeline_type == "project":
            known_results = self.parsed_schema.project_level_data.keys()

        # known_results = self.result_schemas.keys()

        for r in results:
            assert r in known_results, SchemaError(
                f"'{r}' is not a known result. Results defined in the "
                f"schema are: {list(known_results)}."
            )

    def retrieve(self):
        pass

    def set_status(
        self,
        status_identifier: str,
        record_identifier: str = None,
        pipeline_type: Optional[str] = None,
    ) -> None:
        _LOGGER.warning("report not implemented yet for this backend")

    def get_status(self, record_identifier: str) -> Optional[str]:
        _LOGGER.warning("report not implemented yet for this backend")

    def clear_status(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        _LOGGER.warning("report not implemented yet for this backend")

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        _LOGGER.warning("debug remove function abstract class")

    def count_record(self):
        pass


class FileBackend(PipestatBackend):
    def __init__(
        self,
        results_file_path: str,
        record_identifier: Optional[str] = None,
        schema_path=None,
        project_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        parsed_schema: Optional[str] = None,
        status_schema: Optional[str] = None,
        status_file_dir: Optional[str] = None,
    ):
        _LOGGER.warning("Initialize FileBackend")

        self.results_file_path = results_file_path + ".new"
        self.project_name = project_name
        self.pipeline_type = pipeline_type
        self.record_identifier = record_identifier
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.status_file_dir = status_file_dir

        # From: _init_results_file
        _LOGGER.info(f"Initializing results file '{self.results_file_path}'")
        self.DATA_KEY = YAMLConfigManager(
            entries={project_name: {}}, filepath=self.results_file_path, create_file=True
        )
        self.DATA_KEY.setdefault(self.project_name, {})
        self.DATA_KEY[self.project_name].setdefault("project", {})
        self.DATA_KEY[self.project_name].setdefault("sample", {})
        with self.DATA_KEY as data_locked:
            data_locked.write()

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str,
        pipeline_type: Optional[str] = None,
        force_overwrite: bool = False,
        # strict_type: bool = True,
    ) -> None:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        :param str table_name: name of the table to report the result in
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier

        result_identifiers = list(values.keys())
        self.assert_results_defined(results=result_identifiers, pipeline_type=pipeline_type)
        existing = self.check_which_results_exist(
            result_identifier=record_identifier,
            results=result_identifiers,
            pipeline_type=pipeline_type,
        )
        if existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(f"These results exist for '{record_identifier}': {existing_str}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")
        # for r in result_identifiers:
        #     validate_type(value=values[r], schema=self.parsed_schema.results_data[r], strict_type=strict_type)

        _LOGGER.warning("Writing to locked data...")

        self.DATA_KEY[self.project_name][pipeline_type].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self.DATA_KEY[self.project_name][pipeline_type][record_identifier][res_id] = val

        with self.DATA_KEY as locked_data:
            locked_data.write()

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
        :return any | Dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier

        if record_identifier not in self.DATA_KEY[self.project_name][pipeline_type]:
            raise PipestatDatabaseError(f"Record '{record_identifier}' not found")
        if result_identifier is None:
            return self.DATA_KEY.exp[self.project_name][pipeline_type][record_identifier]
        if (
            result_identifier
            not in self.DATA_KEY[self.project_name][pipeline_type][record_identifier]
        ):
            raise PipestatDatabaseError(
                f"Result '{result_identifier}' not found for record '{record_identifier}'"
            )
        return self.DATA_KEY[self.project_name][pipeline_type][record_identifier][
            result_identifier
        ]

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
        :return bool: whether the result has been removed
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier

        # TODO revisit strict_record_id here

        # r_id = self._strict_record_id(record_identifier)
        # r_id = record_identifier

        rm_record = True if result_identifier is None else False

        if not self.check_record_exists(
            record_identifier=record_identifier,
            pipeline_type=pipeline_type,
        ):
            _LOGGER.error(f"Record '{record_identifier}' not found")
            return False

        if result_identifier and not self.check_result_exists(
            result_identifier, record_identifier, pipeline_type=pipeline_type
        ):
            _LOGGER.error(f"'{result_identifier}' has not been reported for '{record_identifier}'")
            return False

        if rm_record:
            _LOGGER.info(f"Removing '{record_identifier}' record")
            del self.DATA_KEY[self.project_name][pipeline_type][record_identifier]
        else:
            val_backup = self.DATA_KEY[self.project_name][pipeline_type][record_identifier][
                result_identifier
            ]
            # self.DATA_KEY[self.project_name][pipeline_type][record_identifier][res_id] = val
            del self.DATA_KEY[self.project_name][pipeline_type][record_identifier][
                result_identifier
            ]
            _LOGGER.info(
                f"Removed result '{result_identifier}' for record "
                f"'{record_identifier}' from '{self.project_name}' namespace"
            )
            if not self.DATA_KEY[self.project_name][pipeline_type][record_identifier]:
                _LOGGER.info(
                    f"Last result removed for '{record_identifier}'. " f"Removing the record"
                )
                del self.DATA_KEY[self.project_name][pipeline_type][record_identifier]
                rm_record = True

            with self.DATA_KEY as locked_data:
                locked_data.write()
        return True

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
        # r_id = self._strict_record_id(record_identifier)

        r_id = record_identifier
        known_status_identifiers = self.status_schema.keys()
        if status_identifier not in known_status_identifiers:
            raise PipestatError(
                f"'{status_identifier}' is not a defined status identifier. "
                f"These are allowed: {known_status_identifiers}"
            )
        prev_status = self.get_status(r_id)

        # TODO: manage project-level flag here.
        if prev_status is not None:
            prev_flag_path = self.get_status_flag_path(prev_status, record_identifier)
            os.remove(prev_flag_path)
        flag_path = self.get_status_flag_path(status_identifier, record_identifier)
        create_lock(flag_path)
        with open(flag_path, "w") as f:
            f.write(status_identifier)
        remove_lock(flag_path)

        if prev_status:
            _LOGGER.debug(f"Changed status from '{prev_status}' to '{status_identifier}'")

    def get_status(self, record_identifier: str) -> Optional[str]:
        """
        Get the current pipeline status

        :return str: status identifier, like 'running'
        """
        # r_id = self._strict_record_id(record_identifier)
        r_id = record_identifier
        flag_file = self.get_flag_file(record_identifier=record_identifier)
        if flag_file is not None:
            assert isinstance(flag_file, str), TypeError(
                "Flag file path is expected to be a str, were multiple flags found?"
            )
            with open(flag_file, "r") as f:
                status = f.read()
            return status
        _LOGGER.debug(
            f"Could not determine status for '{r_id}' record. "
            f"No flags found in: {self.status_file_dir}"
        )
        return None

    def clear_status(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        """
        Remove status flags

        :param str record_identifier: name of the record to remove flags for
        :param Iterable[str] flag_names: Names of flags to remove, optional; if
            unspecified, all schema-defined flag names will be used.
        :return List[str]: Collection of names of flags removed
        """

        flag_names = flag_names or list(self.status_schema.keys())
        if isinstance(flag_names, str):
            flag_names = [flag_names]
        removed = []
        for f in flag_names:
            path_flag_file = self.get_status_flag_path(
                status_identifier=f, record_identifier=record_identifier
            )
            try:
                os.remove(path_flag_file)
            except:
                pass
            else:
                _LOGGER.info(f"Removed existing flag: {path_flag_file}")
                removed.append(f)
        return removed

    def check_which_results_exist(
        self,
        results: List[str],
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> List[str]:
        """
        Check which results have been reported

        :param List[str] results: names of the results to check
        :param str rid: unique identifier of the record
        :param str table_name: name of the table for which to check results
        :return List[str]: names of results which exist
        """

        # pipeline_type = pipeline_type or self.pipeline_type
        # rid = self._strict_record_id(rid)

        if self.project_name not in self.DATA_KEY:
            return []

        return [
            r
            for r in results
            if result_identifier in self.DATA_KEY[self.project_name][pipeline_type]
            and r in self.DATA_KEY[self.project_name][pipeline_type][result_identifier]
        ]

    def check_record_exists(
        self,
        record_identifier: str,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the specified record exists in the table

        :param str record_identifier: record to check for
        :param str table_name: table name to check
        :return bool: whether the record exists in the table
        """
        pipeline_type = pipeline_type or self.pipeline_type

        return (
            self.project_name in self.DATA_KEY
            and record_identifier in self.DATA_KEY[self.project_name][pipeline_type]
        )

    def get_flag_file(self, record_identifier: str = None) -> Union[str, List[str], None]:
        """
        Get path to the status flag file for the specified record

        :param str record_identifier: unique record identifier
        :return str | list[str] | None: path to the status flag file
        """
        # r_id = self._strict_record_id(record_identifier)
        r_id = record_identifier
        regex = os.path.join(self.status_file_dir, f"{self.project_name}_{r_id}_*.flag")
        file_list = glob(regex)
        if len(file_list) > 1:
            _LOGGER.warning("Multiple flag files found")
            return file_list
        elif len(file_list) == 1:
            return file_list[0]
        else:
            _LOGGER.debug("No flag files found")
            return None
        pass

    def get_status_flag_path(self, status_identifier: str, record_identifier=None) -> str:
        """
        Get the path to the status file flag

        :param str status_identifier: one of the defined status IDs in schema
        :param str record_identifier: unique record ID, optional if
            specified in the object constructor
        :return str: absolute path to the flag file or None if object is
            backed by a DB
        """
        # r_id = self._strict_record_id(record_identifier)
        r_id = record_identifier
        return os.path.join(
            self.status_file_dir, f"{self.project_name}_{r_id}_{status_identifier}.flag"
        )

    def count_record(self):
        return len(self.DATA_KEY[self.project_name])


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
        orms: Optional[dict] = None,
        _engine: Any = None,
    ):
        _LOGGER.warning("Initialize DBBackend")
        self.project_name = project_name
        self.pipeline_type = pipeline_type
        self.record_identifier = record_identifier
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.orms = orms
        self._engine = _engine
        # self.schema =parsed_schema

        print("DEBUG")

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
        :param str table_name: name of the table to report the result in
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier
        result_identifiers = list(values.keys())
        self.assert_results_defined(results=result_identifiers, pipeline_type=pipeline_type)

        tn = self.get_table_name(pipeline_type=pipeline_type)

        existing = self.check_which_results_exist(
            record_identifier=record_identifier,
            results=result_identifiers,
            table_name=tn,
        )
        if existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(f"These results exist for '{record_identifier}': {existing_str}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")
        # for r in result_identifiers:
        #     validate_type(value=values[r], schema=self.parsed_schema.results_data[r], strict_type=strict_type)
        # check if results exist here
        try:
            updated_ids = self.report_db(
                record_identifier=record_identifier, values=values, table_name=tn
            )
        except Exception as e:
            _LOGGER.error(f"Could not insert the result into the database. Exception: {e}")
            raise

    def get_table_name(self, pipeline_type: Optional[str] = None):
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

    def report_db(self, values: Dict[str, Any], record_identifier: str, table_name: str) -> int:
        """
        Report a result to a database.

        :param Dict[str, Any] values: values to report
        :param str record_identifier: record to report the result for
        :param str table_name: name of the table to report the result in
        :return int: updated/inserted row
        """
        # record_identifier = self._strict_record_id(record_identifier)
        record_identifier = record_identifier
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
        return returned_id

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

    def get_model(self, table_name: str, strict: bool):
        orms = self.orms

        # table_name = self.get_table_name()

        mod = orms.get(table_name)

        if strict and mod is None:
            raise PipestatDatabaseError(
                f"No object relational mapper class defined for table '{table_name}'. "
                f"{len(orms)} defined: {', '.join(orms.keys())}"
            )
        return mod

    def check_record_exists(
        self,
        record_identifier: str,
        table_name: str,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the specified record exists in the table

        :param str record_identifier: record to check for
        :param str table_name: table name to check
        :return bool: whether the record exists in the table
        """
        pipeline_type = pipeline_type or self.pipeline_type
        query_hit = self.get_one_record(rid=record_identifier, table_name=table_name)
        return query_hit is not None

    def get_one_record(self, table_name: str, rid: Optional[str] = None):
        models = [self.get_orm(table_name=table_name)] if table_name else list(self.orms.values())
        with self.session as s:
            for mod in models:
                # record = sql_select(mod).where(mod.record_identifier == rid).first()
                # record = s.query(mod).where(mod.record_identifier == rid).first()
                stmt = sql_select(mod).where(mod.record_identifier == rid)
                # stmt = sql_select(mod)
                record = s.exec(stmt).first()
                # record = (
                #     s.query(mod)
                #     .filter_by(record_identifier=rid)
                #     .first()
                # )
                if record:
                    return record

    def check_which_results_exist(
        self, results: List[str], record_identifier: str = None, table_name: str = None
    ) -> List[str]:
        """
        Check if the specified results exist in the table

        :param List[str] results: results identifiers to check for
        :param str rid: record to check for
        :param str table_name: name of the table to search for results in
        :return List[str]: results identifiers that exist
        """
        # table_name = table_name or self.namespace
        # rid = self._strict_record_id(rid)
        rid = record_identifier
        record = self.get_one_record(rid=rid, table_name=table_name)
        return [r for r in results if getattr(record, r, None) is not None] if record else []

    def count_record(self):
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: number of rows in the selected table
        """
        table_name = self.get_table_name()
        mod = self.get_model(table_name=table_name, strict=True)
        with self.session as s:
            stmt = sql_select(mod)
            records = s.exec(stmt).all()
            return len(records)

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