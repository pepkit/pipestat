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
