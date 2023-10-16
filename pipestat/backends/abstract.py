import sys
import datetime
from abc import ABC
from logging import getLogger

from pipestat.helpers import *

if int(sys.version.split(".")[1]) < 9:
    from typing import List, Dict, Any, Optional, Union
else:
    from typing import *

_LOGGER = getLogger(PKG_NAME)


class PipestatBackend(ABC):
    """Abstract class representing a pipestat backend"""

    def __init__(self, pipeline_type):
        _LOGGER.warning("Initialize PipestatBackend")
        self.pipeline_type = pipeline_type

    def assert_results_defined(self, results: List[str], pipeline_type: str) -> None:
        """
        Assert provided list of results is defined in the schema

        :param List[str] results: list of results to
            check for existence in the schema
        :param str pipeline_type: "sample" or "project"
        :raises SchemaError: if any of the results is not defined in the schema
        """

        # take project level input and look for keys in the specific schema.
        # warn if you are trying to report a sample to a project level and vice versa.

        if pipeline_type == "sample":
            known_results = self.parsed_schema.sample_level_data.keys()
        if pipeline_type == "project":
            known_results = self.parsed_schema.project_level_data.keys()
        if STATUS in results:
            known_results = [STATUS]

        for r in results:
            assert r in known_results, SchemaError(
                f"'{r}' is not a known result. Results defined in the "
                f"schema are: {list(known_results)}."
            )

    def check_result_exists(
        self,
        result_identifier: str,
        record_identifier: Optional[str] = None,
    ) -> bool:
        """
        Check if the result has been reported
        :param str result_identifier: name of the result to check
        :param str record_identifier: unique identifier of the record
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        record_identifier = record_identifier or self.record_identifier

        return (
            len(
                self.list_results(
                    restrict_to=[result_identifier],
                    record_identifier=record_identifier,
                )
            )
            > 0
        )

    def check_record_exists(self, record_identifier: str) -> bool:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def count_records(self):
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def get_records(
        self,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0,
    ) -> Optional[dict]:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def get_status(self, record_identifier: str) -> Optional[str]:
        _LOGGER.warning("Not implemented yet for this backend")

    def link(self, link_dir) -> str:
        """
        This function creates a link structure such that results are organized by type.
        :param str link_dir: path to desired symlink output directory (does not have to be absolute)
        :return str link_dir: returns absolute path to symlink directory
        """

        def get_all_paths(parent_key, result_identifier_value):
            """If the result identifier is a complex object which contains nested paths"""

            key_value_pairs = []

            for k, v in result_identifier_value.items():
                if isinstance(v, dict):
                    key_value_pairs.extend(get_all_paths(k, v))
                elif k == "path":
                    key_value_pairs.append((parent_key, v))
            return key_value_pairs

        unique_result_identifiers = []

        all_records = self.get_records()

        for record in all_records["records"]:
            result_identifiers = self.retrieve(record_identifier=record)
            for k, v in result_identifiers.items():
                if type(v) == dict:
                    all_paths = get_all_paths(k, v)
                    for path in all_paths:
                        file = os.path.basename(path[1])
                        if k not in unique_result_identifiers:
                            sub_dir_for_type = os.path.join(link_dir, k)
                            unique_result_identifiers.append((k, sub_dir_for_type))
                            try:
                                os.mkdir(sub_dir_for_type)
                            except:
                                pass
                        for subdir in unique_result_identifiers:
                            if k == subdir[0]:
                                target_dir = subdir[1]
                        linkname = os.path.join(target_dir, record + "_" + path[0] + "_" + file)
                        src = os.path.abspath(path[1])
                        src_rel = os.path.relpath(src, os.path.dirname(linkname))
                        force_symlink(src_rel, linkname)

        return link_dir

    def clear_status(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        _LOGGER.warning("Not implemented yet for this backend")

    def set_status(
        self,
        status_identifier: str,
        record_identifier: Optional[str] = None,
    ) -> None:
        _LOGGER.warning("Not implemented yet for this backend")

    def list_results(self) -> List[str]:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def list_recent_results(
        self,
        limit: Optional[int] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        type: Optional[str] = None,
    ) -> List[str]:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str,
        force_overwrite: bool = False,
        result_formatter: Optional[staticmethod] = None,
    ) -> str:
        _LOGGER.warning("Not implemented yet for this backend")

    def retrieve_distinct(
        self,
        columns: Optional[List[str]] = None,
    ) -> List[Any]:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def retrieve(
        self, record_identifier: Optional[str] = None, result_identifier: Optional[str] = None
    ) -> Union[Any, Dict[str, Any]]:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
    ) -> bool:
        _LOGGER.warning("Not implemented yet for this backend")

    def remove_record(
        self,
        record_identifier: Optional[str] = None,
        rm_record: Optional[bool] = False,
    ) -> bool:
        _LOGGER.warning("Not implemented yet for this backend")
