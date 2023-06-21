import sys

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
        sample_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the result has been reported

        :param str sample_name: unique identifier of the record
        :param str result_identifier: name of the result to check
        :param str pipeline_type: "sample" or "project"
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        pipeline_type = pipeline_type or self.pipeline_type
        sample_name = sample_name or self.sample_name

        return (
            len(
                self.list_results(
                    restrict_to=[result_identifier],
                    sample_name=sample_name,
                    pipeline_type=pipeline_type,
                )
            )
            > 0
        )

    def check_record_exists(self) -> bool:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def count_records(
        self,
        pipeline_type: Optional[str] = None,
    ):
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def get_status(self, sample_name: str, pipeline_type: Optional[str] = None) -> Optional[str]:
        _LOGGER.warning("Not implemented yet for this backend")

    def clear_status(
        self, sample_name: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        _LOGGER.warning("Not implemented yet for this backend")

    def set_status(
        self,
        status_identifier: str,
        sample_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> None:
        _LOGGER.warning("Not implemented yet for this backend")

    def list_results(self) -> List[str]:
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def report(
        self,
        values: Dict[str, Any],
        sample_name: Optional[str] = None,
        force_overwrite: bool = False,
        strict_type: bool = True,
        return_id: bool = False,
        pipeline_type: Optional[str] = None,
    ) -> str:
        _LOGGER.warning("Not implemented yet for this backend")

    def retrieve(self):
        _LOGGER.warning("Not implemented yet for this backend")
        pass

    def remove(
        self,
        sample_name: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        _LOGGER.warning("Not implemented yet for this backend")

    def remove_record(
        self,
        sample_name: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        _LOGGER.warning("Not implemented yet for this backend")
