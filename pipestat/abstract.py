from abc import ABC
from .const import *

from logging import getLogger
from yacman import YAMLConfigManager
import sys 
if int(sys.version.split(".")[1]) < 9:
    from typing import List, Dict, Any, Optional, Union

_LOGGER = getLogger(PKG_NAME)

class PipestatBackend(ABC):
    """ Abstract class representing a pipestat backend"""
    def __init__(self):
        pass

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str = None,
        force_overwrite: bool = False,
        strict_type: bool = True,
        return_id: bool = False,
        pipeline_type: Optional[str] = None,
    ) -> Union[bool, int]:
        _LOGGER.warning("Initialize PipestatBackend")

class FileBackend(PipestatBackend):

    def __init__(
        self,
        record_identifier: Optional[str] = None,
        schema_path: Optional[str] = None,
        results_file_path: Optional[str] = None,
        namespace: Optional[str] = None
    ):
        _LOGGER.warning("Initialize FileBackend")
        _LOGGER.info(f"Initializing results file '{results_file_path}'")
        data = YAMLConfigManager(
            entries={namespace: "{}"}, filepath=results_file_path, create_file=True
        )
        with data as data_locked:
            data_locked.write()
        self.DATA_KEY = data
        self.DATA_KEY = YAMLConfigManager()


    def _report_data_element(
        self,
        record_identifier: str,
        values: Dict[str, Any],
        pipeline_type: Optional[str] = None,
        table_name: Optional[bool] = None,
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

        # TODO: update to disambiguate sample- / project-level
        self.DATA_KEY.setdefault(self.namespace, {})
        # self.DATA_KEY[self.namespace].setdefault(record_identifier, {})
        self.DATA_KEY[self.namespace].setdefault(pipeline_type, {})
        self.DATA_KEY[self.namespace][pipeline_type].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self.DATA_KEY[self.namespace][pipeline_type][record_identifier][res_id] = val
            # self.DATA_KEY[self.namespace][record_identifier][res_id] = val

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

