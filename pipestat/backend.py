import sys

from abc import ABC
from logging import getLogger
from yacman import YAMLConfigManager

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

    def check_result_exists():
        pass

    def check_which_results_exist():
        pass

    def retrieve():
        pass

    def set_status():
        pass

    def get_status():
        pass

    def clear_status():
        pass

    def remove():
        pass


class FileBackend(PipestatBackend):
    def __init__(
        self,
        results_file_path: str,
        record_identifier: Optional[str] = None,
        schema_path=None,
        project_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ):
        _LOGGER.warning("Initialize FileBackend")

        self.results_file_path = results_file_path + ".new"
        self.project_name = project_name
        self.pipeline_type = pipeline_type
        self.record_identifier = record_identifier

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

        # r_id = self._strict_record_id(record_identifier)
        # r_id = record_identifier

        rm_record = True if result_identifier is None else False

        # if not self.check_record_exists(
        #     record_identifier=r_id,
        #     table_name=self.namespace,
        #     pipeline_type=pipeline_type,
        # ):
        #     _LOGGER.error(f"Record '{r_id}' not found")
        #     return False
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

        # if self.file is None:
        #     try:
        #         self._remove_db(
        #             record_identifier=r_id,
        #             result_identifier=None if rm_record else result_identifier,
        #         )
        #     except Exception as e:
        #         _LOGGER.error(f"Could not remove the result from the database. Exception: {e}")
        #         if not self[DB_ONLY_KEY] and not rm_record:
        #             self[DATA_KEY][self.namespace][pipeline_type][r_id][
        #                 result_identifier
        #             ] = val_backup
        #         raise
        return True

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
