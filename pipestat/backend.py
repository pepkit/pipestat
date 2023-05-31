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
        

class FileBackend(PipestatBackend):

    def __init__(
        self,
        results_file_path: str,
        record_identifier: Optional[str] = None,
        schema_path = None,
        pipeline_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ):
        _LOGGER.warning("Initialize FileBackend")
        self.results_file_path = results_file_path + ".new"
        self.pipeline_name = pipeline_name
        self.pipeline_type = pipeline_type
        # From: _init_results_file
        _LOGGER.info(f"Initializing results file '{self.results_file_path}'")
        self.DATA_KEY = YAMLConfigManager(
            entries={pipeline_name: {}}, filepath=self.results_file_path, create_file=True
        )
        self.DATA_KEY.setdefault(self.pipeline_name, {})
        self.DATA_KEY[self.pipeline_name].setdefault("project", {})
        self.DATA_KEY[self.pipeline_name].setdefault("sample", {})
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
        self.DATA_KEY[self.pipeline_name][pipeline_type].setdefault(record_identifier, {})
        # self.DATA_KEY[self.pipeline_name].setdefault(pipeline_type, {})
        # self.DATA_KEY[self.pipeline_name][pipeline_type].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self.DATA_KEY[self.pipeline_name][pipeline_type][record_identifier][res_id] = val
            # self.DATA_KEY[self.pipeline_name][record_identifier][res_id] = val
            
        with self.DATA_KEY as locked_data:
            locked_data.write()            

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

