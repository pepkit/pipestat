import datetime
from logging import getLogger

import pephubclient
from pephubclient.constants import RegistryPath
from pephubclient.exceptions import ResponseError
from ubiquerg import parse_registry_path

from ...backends.abstract import PipestatBackend
from ...const import PKG_NAME
from typing import List, Dict, Any, Optional, Union, NoReturn, Tuple


from pephubclient import PEPHubClient


_LOGGER = getLogger(PKG_NAME)

class PEPHUBBACKEND(PipestatBackend):
    def __init__(
        self,
        record_identifier: Optional[str] = None,
        pephub_path: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
        parsed_schema: Optional[str] = None,
        status_schema: Optional[str] = None,
    ):
        """
        ADD DOCSTRINGS!

        """
        super().__init__(pipeline_type)

        self.phc = PEPHubClient()
        self.pipeline_name = pipeline_name
        self.parsed_schema = parsed_schema

        # Test Registry Path
        _LOGGER.warning(f"Is pephub registry path? {pephubclient.is_registry_path(pephub_path)}")

        if pephubclient.is_registry_path(pephub_path):
            # Deconstruct registry path so that phc can use it to create/update/delete samples
            _LOGGER.warning("Initialize PEPHub Backend")

            self.pep_registry = RegistryPath(**parse_registry_path(pephub_path))
            _LOGGER.warning(f"Registry namespace: {self.pep_registry.namespace} item: {self.pep_registry.item} tag: {self.pep_registry.tag}")


        else:
            raise Exception


    def report(
        self,
        values: Dict[str, Any],
        record_identifier: Optional[str] = None,
        force_overwrite: bool = True,
        result_formatter: Optional[staticmethod] = None,
        history_enabled: bool = True,
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
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        result_identifiers = list(values.keys())

        if self.parsed_schema is not None:
            self.assert_results_defined(
                results=result_identifiers, pipeline_type=self.pipeline_type
            )

        # existing = self.list_results(
        #     record_identifier=record_identifier,
        #     restrict_to=result_identifiers,
        # )
        existing = False

        if existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(f"These results exist for '{record_identifier}': {existing_str}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")

        if not existing:
            # self._config.phc.sample.update(
            #     namespace=self._config.config.phc.namespace,
            #     name=self._config.config.phc.name,
            #     tag=self._config.config.phc.tag,
            #     sample_name=identifier,
            #     sample_dict=metadata,
            # )

            try:
                self.phc.sample.update(
                    namespace=self.pep_registry.namespace,
                    name="TEST_PIPESTAT",
                    tag=self.pep_registry.tag,
                    sample_name=record_identifier,
                    sample_dict=values,

                )
            except ResponseError:
                _LOGGER.warning("Login to pephubclient is required. phc login")


            # results_formatted.append(
            #     result_formatter(
            #         pipeline_name=self.pipeline_name,
            #         record_identifier=record_identifier,
            #         res_id=res_id,
            #         value=val,
            #     )
            # )


        return True