import sys
import os
from abc import ABC
from glob import glob
from logging import getLogger
from yacman import YAMLConfigManager
from ubiquerg import create_lock, remove_lock, expandpath

from .const import *
from .exceptions import *

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

    def clear_status(self):
        pass

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        _LOGGER.warning("debug remove function abstract class")


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

        self.DATA_KEY[self.project_name][pipeline_type].setdefault(record_identifier, {})
        # self.DATA_KEY[self.project_name].setdefault(pipeline_type, {})
        # self.DATA_KEY[self.project_name][pipeline_type].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self.DATA_KEY[self.project_name][pipeline_type][record_identifier][res_id] = val
            # self.DATA_KEY[self.project_name][record_identifier][res_id] = val

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


class DBBackend(PipestatBackend):
    def __init__(
        self,
        record_identifier: Optional[str] = None,
        schema_path: Optional[str] = None,
        results_file_path: Optional[str] = None,
        config_file: Optional[str] = None,
        config_dict: Optional[dict] = None,
        flag_file_dir: Optional[str] = None,
        show_db_logs: bool = False,
        pipeline_type: Optional[str] = False,
    ):
        _LOGGER.warning("Initialize DBBackend")
