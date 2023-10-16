import os.path
import sys

from glob import glob
from logging import getLogger
from yacman import YAMLConfigManager
from ubiquerg import create_lock, remove_lock

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
        pipeline_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        parsed_schema: Optional[str] = None,
        status_schema: Optional[str] = None,
        status_file_dir: Optional[str] = None,
        result_formatter: Optional[staticmethod] = None,
        multi_pipelines: Optional[bool] = None,
    ):
        """
        Class representing a File backend
        :param str results_file_path: YAML file to report into, if file is
            used as the object back-end
        :param str record_identifier: record identifier to report for. This
            creates a weak bound to the record, which can be overridden in
            this object method calls
        :param str pipeline_name: name of pipeline associated with result
        :param str pipeline_type: "sample" or "project"
        :param str parsed_schema: results output schema. Used to construct DB columns.
        :param str status_schema: schema containing pipeline statuses e.g. 'running'
        :param str status_file_dir: directory for placing status flags
        :param str result_formatter: function for formatting result
        :param bool multi_pipelines: allows for running multiple pipelines for one file backend

        """
        _LOGGER.warning("Initialize FileBackend")

        self.results_file_path = results_file_path
        self.pipeline_name = pipeline_name
        self.pipeline_type = pipeline_type
        self.record_identifier = record_identifier
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.status_file_dir = status_file_dir
        self.result_formatter = result_formatter
        self.multi_pipelines = multi_pipelines

        if not os.path.exists(self.results_file_path):
            _LOGGER.debug(
                f"Results file doesn't yet exist. Initializing: {self.results_file_path}"
            )
            self._init_results_file()
        else:
            _LOGGER.debug(f"Loading results file: {self.results_file_path}")
            self._load_results_file()

    def check_record_exists(
        self,
        record_identifier: str,
    ) -> bool:
        """
        Check if the specified record exists in self._data

        :param str record_identifier: record to check for
        :return bool: whether the record exists in the table
        """

        return (
            self.pipeline_name in self._data
            and record_identifier in self._data[self.pipeline_name][self.pipeline_type]
        )

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

    def count_records(self):
        """
        Count records
        :return int: number of records
        """

        return len(self._data[self.pipeline_name])

    def get_flag_file(self, record_identifier: str = None) -> Union[str, List[str], None]:
        """
        Get path to the status flag file for the specified record

        :param str record_identifier: unique record identifier
        :return str | list[str] | None: path to the status flag file
        """

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
        record_list = []
        for k in list(self._data.data[self.pipeline_name][self.pipeline_type].keys())[
            offset : offset + limit
        ]:
            record_list.append(k)

        records_dict = {
            "count": len(record_list),
            "limit": limit,
            "offset": offset,
            "records": record_list,
        }

        return records_dict

    def get_status(self, record_identifier: str) -> Optional[str]:
        """
        Get the current pipeline status

        :param str record_identifier: record identifier to set the
            pipeline status for
        :return str: status identifier, e.g. 'running'
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

    def get_status_flag_path(self, status_identifier: str, record_identifier=None) -> str:
        """
        Get the path to the status file flag

        :param str status_identifier: one of the defined status IDs in schema
        :param str record_identifier: unique record ID
        :return str: absolute path to the flag file or None if object is
            backed by a DB
        """

        r_id = record_identifier
        return os.path.join(
            self.status_file_dir, f"{self.pipeline_name}_{r_id}_{status_identifier}.flag"
        )

    def list_recent_results(self, limit, range) -> List[str]:
        """Lists recent results based on time filter

        limit number of results

        range  = list -> [start, end]

        """

        results = ["demo results"]
        return results

    def list_results(
        self,
        restrict_to: Optional[List[str]] = None,
        record_identifier: Optional[str] = None,
    ) -> List[str]:
        """
        Lists all, or a selected set of, reported results

        :param List[str] restrict_to: selected subset of names of results to list
        :param str record_identifier: unique identifier of the record
        :return List[str]: names of results which exist
        """
        record_identifier = record_identifier or self.record_identifier

        try:
            results = list(
                self._data[self.pipeline_name][self.pipeline_type][record_identifier].keys()
            )
        except KeyError:
            return []
        if restrict_to:
            return [r for r in restrict_to if r in results]
        return results

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
        ):
            _LOGGER.error(f"Record '{record_identifier}' not found")
            return False

        if result_identifier and not self.check_result_exists(
            result_identifier, record_identifier
        ):
            _LOGGER.error(f"'{result_identifier}' has not been reported for '{record_identifier}'")
            return False

        if rm_record:
            self.remove_record(
                record_identifier=record_identifier,
                rm_record=rm_record,
            )
        else:
            val_backup = self._data[self.pipeline_name][self.pipeline_type][record_identifier][
                result_identifier
            ]
            del self._data[self.pipeline_name][self.pipeline_type][record_identifier][
                result_identifier
            ]
            _LOGGER.info(
                f"Removed result '{result_identifier}' for record "
                f"'{record_identifier}' from '{self.pipeline_name}' namespace"
            )
            if not self._data[self.pipeline_name][self.pipeline_type][record_identifier]:
                _LOGGER.info(
                    f"Last result removed for '{record_identifier}'. " f"Removing the record"
                )
                rm_record = True
                self.remove_record(
                    record_identifier=record_identifier,
                    rm_record=rm_record,
                )
            with self._data as locked_data:
                locked_data.write()
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
        if rm_record:
            try:
                _LOGGER.info(f"Removing '{record_identifier}' record")
                del self._data[self.pipeline_name][self.pipeline_type][record_identifier]
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

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: Optional[str] = None,
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
        :param bool force_overwrite: Toggles force overwriting results, defaults to False
        :param str result_formatter: function for formatting result
        :return bool | list[str] results_formatted: return list of formatted string
        """

        # record_identifier = record_identifier or self.record_identifier
        record_identifier = record_identifier
        result_formatter = result_formatter or self.result_formatter
        results_formatted = []

        result_identifiers = list(values.keys())
        if self.parsed_schema is not None:
            self.assert_results_defined(
                results=result_identifiers, pipeline_type=self.pipeline_type
            )
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

        self._data[self.pipeline_name][self.pipeline_type].setdefault(record_identifier, {})

        for res_id, val in values.items():
            self._data[self.pipeline_name][self.pipeline_type][record_identifier][res_id] = val
            results_formatted.append(
                result_formatter(
                    pipeline_name=self.pipeline_name,
                    record_identifier=record_identifier,
                    res_id=res_id,
                    value=val,
                )
            )

        with self._data as locked_data:
            locked_data.write()

        return results_formatted

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

        if record_identifier not in self._data[self.pipeline_name][self.pipeline_type]:
            raise RecordNotFoundError(f"Record '{record_identifier}' not found")
        if result_identifier is None:
            return self._data.exp[self.pipeline_name][self.pipeline_type][record_identifier]
        if (
            result_identifier
            not in self._data[self.pipeline_name][self.pipeline_type][record_identifier]
        ):
            raise RecordNotFoundError(
                f"Result '{result_identifier}' not found for record '{record_identifier}'"
            )
        return self._data[self.pipeline_name][self.pipeline_type][record_identifier][
            result_identifier
        ]

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
        r_id = record_identifier or self.record_identifier
        if self.status_schema is not None:
            known_status_identifiers = self.status_schema.keys()
            if status_identifier not in known_status_identifiers:
                raise UnrecognizedStatusError(
                    f"'{status_identifier}' is not a defined status identifier. "
                    f"These are allowed: {known_status_identifiers}"
                )
        prev_status = self.get_status(r_id)

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

    def _htmlreportbuilder(self):
        """
        build html report based on all reported results
        """

        # build new folder for the report
        self.reports_dir = os.path.join(self.results_file_path, "reports")
        _LOGGER.debug(f"Reports dir: {self.reports_dir}")

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
        elif num_namespaces > 0:
            if self.pipeline_name in namespaces_reported:
                self._data = data
            elif self.pipeline_name not in namespaces_reported and self.multi_pipelines is True:
                self._data = data
                self._data.setdefault(self.pipeline_name, {})
                self._data[self.pipeline_name].setdefault("project", {})
                self._data[self.pipeline_name].setdefault("sample", {})
                _LOGGER.warning("MULTI PIPELINES FOR SINGLE RESULTS FILE")
            else:
                raise PipestatError(
                    f"'{self.results_file_path}' is already in use for {num_namespaces} namespaces: {', '.join(namespaces_reported)} and multi_pipelines = False."
                )
