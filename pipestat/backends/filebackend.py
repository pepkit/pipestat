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


class FileBackend(PipestatBackend):
    def __init__(
        self,
        results_file_path: str,
        record_identifier: Optional[str] = None,
        schema_path=None,
        pipeline_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        parsed_schema: Optional[str] = None,
        status_schema: Optional[str] = None,
        status_file_dir: Optional[str] = None,
    ):
        """
        Class representing a File backend
        """
        _LOGGER.warning("Initialize FileBackend")

        self.results_file_path = results_file_path
        self.pipeline_name = pipeline_name
        self.pipeline_type = pipeline_type
        self.record_identifier = record_identifier
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.status_file_dir = status_file_dir

        if not os.path.exists(self.results_file_path):
            _LOGGER.debug(
                f"Results file doesn't yet exist. Initializing: {self.results_file_path}"
            )
            self._init_results_file()
        else:
            _LOGGER.debug(f"Loading results file: {self.results_file_path}")
            self._load_results_file()

    def _init_results_file(self) -> None:
        """
        Initialize YAML results file if it does not exist.
        Read the data stored in the existing file into the memory otherwise.

        :return bool: whether the file has been created
        """
        _LOGGER.info(f"Initializing results file '{self.results_file_path}'")
        self._data = YAMLConfigManager(
            entries={self.pipeline_name: {}}, filepath=self.results_file_path, create_file=True
        )
        self._data.setdefault(self.pipeline_name, {})
        self._data[self.pipeline_name].setdefault("project", {})
        self._data[self.pipeline_name].setdefault("sample", {})
        with self._data as data_locked:
            data_locked.write()

    def _load_results_file(self) -> None:
        _LOGGER.debug(f"Reading data from '{self.results_file_path}'")
        data = YAMLConfigManager(filepath=self.results_file_path)
        if not bool(data):
            self._data = data
            self._data.setdefault(self.pipeline_name, {})
            self._data[self.pipeline_name].setdefault("project", {})
            self._data[self.pipeline_name].setdefault("sample", {})
            with self._data as data_locked:
                data_locked.write()
        namespaces_reported = [k for k in data.keys() if not k.startswith("_")]
        num_namespaces = len(namespaces_reported)
        if num_namespaces == 0:
            self._data = data
        elif num_namespaces == 1:
            previous = namespaces_reported[0]
            if self.pipeline_name != previous:
                msg = f"'{self.results_file_path}' is already used to report results for a different (not {self.pipeline_name}) namespace: {previous}"
                raise PipestatError(msg)
            self._data = data
        else:
            raise PipestatError(
                f"'{self.results_file_path}' is in use for {num_namespaces} namespaces: {', '.join(namespaces_reported)}"
            )

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

        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        :param str record_identifier: unique identifier of the record
        :param str pipeline_type: "sample" or "project"
        :param bool force_overwrite: Toggles force overwriting results, defaults to False
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier

        result_identifiers = list(values.keys())
        self.assert_results_defined(results=result_identifiers, pipeline_type=pipeline_type)
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

        _LOGGER.warning("Writing to locked data...")

        self._data[self.pipeline_name][pipeline_type].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self._data[self.pipeline_name][pipeline_type][record_identifier][res_id] = val

        with self._data as locked_data:
            locked_data.write()

        _LOGGER.warning(self._data)

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

        if record_identifier not in self._data[self.pipeline_name][pipeline_type]:
            raise PipestatDataError(f"Record '{record_identifier}' not found")
        if result_identifier is None:
            return self._data.exp[self.pipeline_name][pipeline_type][record_identifier]
        if (
            result_identifier
            not in self._data[self.pipeline_name][pipeline_type][record_identifier]
        ):
            raise PipestatDataError(
                f"Result '{result_identifier}' not found for record '{record_identifier}'"
            )
        return self._data[self.pipeline_name][pipeline_type][record_identifier][result_identifier]

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
            self.remove_record(
                record_identifier=record_identifier,
                pipeline_type=pipeline_type,
                rm_record=rm_record,
            )
        else:
            val_backup = self._data[self.pipeline_name][pipeline_type][record_identifier][
                result_identifier
            ]
            del self._data[self.pipeline_name][pipeline_type][record_identifier][result_identifier]
            _LOGGER.info(
                f"Removed result '{result_identifier}' for record "
                f"'{record_identifier}' from '{self.pipeline_name}' namespace"
            )
            if not self._data[self.pipeline_name][pipeline_type][record_identifier]:
                _LOGGER.info(
                    f"Last result removed for '{record_identifier}'. " f"Removing the record"
                )
                rm_record = True
                self.remove_record(
                    record_identifier=record_identifier,
                    pipeline_type=pipeline_type,
                    rm_record=rm_record,
                )
            with self._data as locked_data:
                locked_data.write()
        return True

    def remove_record(
        self,
        record_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        project_name: Optional[str] = None,
        rm_record: Optional[bool] = False,
    ) -> bool:
        """
        Remove a record, requires rm_record to be True

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :param str pipeline_type: "sample" or "project"
        :param bool rm_record: bool for removing record.
        :return bool: whether the result has been removed
        """
        if rm_record:
            try:
                _LOGGER.info(f"Removing '{record_identifier}' record")
                del self._data[self.pipeline_name][pipeline_type][record_identifier]
                with self._data as locked_data:
                    locked_data.write()
                return True
            except:
                _LOGGER.warning(
                    f" Unable to remove record, aborting Removing '{record_identifier}' record"
                )
                return False
        else:
            _LOGGER.info(f" rm_record flag False, aborting Removing '{record_identifier}' record")

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

    def get_status(
        self, record_identifier: str, pipeline_type: Optional[str] = None
    ) -> Optional[str]:
        """
        Get the current pipeline status

        :param str record_identifier: record identifier to set the
            pipeline status for
        :param str pipeline_type: "sample" or "project"
        :return str: status identifier, like 'running'
        """
        r_id = record_identifier or self.record_identifier
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
        :param str pipeline_type: "sample" or "project"
        """
        pipeline_type = pipeline_type or self.pipeline_type
        r_id = record_identifier or self.record_identifier
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

    def count_records(self, pipeline_type: Optional[str] = None):
        """
        Count records
        :param str pipeline_type: sample vs project designator needed to count records
        :return int: number of records
        """

        return len(self._data[self.pipeline_name])

    def check_record_exists(
        self,
        record_identifier: str,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the specified record exists in self._data

        :param str record_identifier: record to check for
        :param str pipeline_type: project or sample pipeline
        :return bool: whether the record exists in the table
        """
        pipeline_type = pipeline_type or self.pipeline_type

        return (
            self.pipeline_name in self._data
            and record_identifier in self._data[self.pipeline_name][pipeline_type]
        )

    def get_flag_file(self, record_identifier: str = None) -> Union[str, List[str], None]:
        """
        Get path to the status flag file for the specified record

        :param str record_identifier: unique record identifier
        :return str | list[str] | None: path to the status flag file
        """
        # r_id = self._strict_record_id(record_identifier)
        r_id = record_identifier
        regex = os.path.join(self.status_file_dir, f"{self.pipeline_name}_{r_id}_*.flag")
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
            self.status_file_dir, f"{self.pipeline_name}_{r_id}_{status_identifier}.flag"
        )

    def list_results(
        self,
        restrict_to: Optional[List[str]] = None,
        record_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> List[str]:
        """
        Lists all, or a selected set of, reported results

        :param List[str] restrict_to: selected subset of names of results to list
        :param str record_identifier: unique identifier of the record
        :param str pipeline_type: "sample" or "project"
        :return List[str]: names of results which exist
        """

        pipeline_type = pipeline_type or self.pipeline_type
        record_identifier = record_identifier or self.record_identifier

        try:
            results = list(self._data[self.pipeline_name][pipeline_type][record_identifier].keys())
        except KeyError:
            return []
        if restrict_to:
            return [r for r in restrict_to if r in results]
        return results
