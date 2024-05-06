import copy
import datetime
import operator
from logging import getLogger

import pephubclient
from pephubclient.constants import RegistryPath
from pephubclient.exceptions import ResponseError
from ubiquerg import parse_registry_path

from ...backends.abstract import PipestatBackend
from ...const import PKG_NAME
from typing import List, Dict, Any, Optional, Union, NoReturn, Tuple, Literal


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
        self.pephub_path = pephub_path

        # Test Registry Path
        _LOGGER.warning(f"Is pephub registry path? {pephubclient.is_registry_path(pephub_path)}")

        if pephubclient.is_registry_path(pephub_path):
            # Deconstruct registry path so that phc can use it to create/update/delete samples
            _LOGGER.warning("Initialize PEPHub Backend")

            self.pep_registry = RegistryPath(**parse_registry_path(pephub_path))
            _LOGGER.warning(
                f"Registry namespace: {self.pep_registry.namespace} item: {self.pep_registry.item} tag: {self.pep_registry.tag}"
            )

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

            # try:
            self.phc.sample.create(
                namespace=self.pep_registry.namespace,
                name=self.pep_registry.item,
                tag=self.pep_registry.tag,
                sample_name=record_identifier,
                sample_dict=values,
                overwrite=force_overwrite,
            )
            # except ResponseError:
            #     _LOGGER.warning("Login to pephubclient is required. phc login")

            # results_formatted.append(
            #     result_formatter(
            #         pipeline_name=self.pipeline_name,
            #         record_identifier=record_identifier,
            #         res_id=res_id,
            #         value=val,
            #     )
            # )

        return True

    def select_records(
        self,
        columns: Optional[List[str]] = None,
        filter_conditions: Optional[List[Dict[str, Any]]] = None,
        limit: Optional[int] = 1000,
        cursor: Optional[int] = None,
        bool_operator: Optional[str] = "AND",
    ) -> Dict[str, Any]:
        """
        Perform a `SELECT` on the table

        :param list[str] columns: columns to include in the result
        :param list[dict]  filter_conditions: e.g. [{"key": ["id"], "operator": "eq", "value": 1)], operator list:
            - eq for ==
            - lt for <
            - ge for >=
            - in for in_
            - like for like
        :param int limit: maximum number of results to retrieve per page
        :param int cursor: cursor position to begin retrieving records
        :param bool bool_operator: Perform filtering with AND or OR Logic.
        :return dict records_dict = {
            "total_size": int,
            "page_size": int,
            "next_page_token": int,
            "records": List[Dict[{key, Any}]],
        }
        """

        if cursor:
            # TODO can we support cursor through pephubclient?
            _LOGGER.warning("Cursor not supported for PEPHubBackend, ignoring cursor")

        def get_operator(op: Literal["eq", "lt", "ge", "gt", "in"]) -> Any:
            """
            Get python operator for a given string

            :param str op: desired operator, "eq", "lt"
            :return: operator function
            """

            if op == "eq":
                return operator.__eq__
            if op == "lt":
                return operator.__lt__
            if op == "ge":
                return operator.__ge__
            if op == "gt":
                return operator.__gt__
            if op == "in":
                return operator.contains
            raise ValueError(f"Invalid filter operator: {op}")

        # Can we use query_param to do cursor/limit operations if the PEP is very large?
        project = self.phc.load_project(project_registry_path=self.pephub_path)
        print(project)

        # PEPHub uses sample_name not record_identifier
        # Just get the items from the sample table because its a dataframe and return the dict to the end user
        if columns is not None:
            columns = copy.deepcopy(columns)
            for i in ["sample_name"]:  # Must add id, need it for cursor
                if i not in columns:
                    columns.insert(0, i)
            df = project.sample_table[columns]
        else:
            df = project.sample_table

        total_count = len(df)

        records_list = []
        if filter_conditions:
            for filter_condition in filter_conditions:
                retrieved_operator = get_operator(filter_condition["operator"])
                retrieved_results = []

        #
        # filtered_df = df[(df['sample_type'] == 'sample_type1') & (df['genome'] == 'genome1')]
        #
        # df[df['sample_name'] == 'sample1']

        records_dict = {
            "total_size": total_count,
            "page_size": limit,
            "next_page_token": 0,
            "records": records_list,
        }

        return records_dict
