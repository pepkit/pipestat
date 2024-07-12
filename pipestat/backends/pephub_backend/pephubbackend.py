import copy
import datetime
import operator
from logging import getLogger

import pephubclient
from pephubclient.constants import RegistryPath
from pephubclient.exceptions import ResponseError
from ubiquerg import parse_registry_path

from ...backends.abstract import PipestatBackend
from ...const import PKG_NAME, STATUS
from typing import List, Dict, Any, Optional, Union, NoReturn, Tuple, Literal


from pephubclient import PEPHubClient

from ...exceptions import UnrecognizedStatusError, RecordNotFoundError, ColumnNotFoundError

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
        result_formatter: Optional[staticmethod] = None,
    ):
        """
        ADD DOCSTRINGS!

        """
        super().__init__(pipeline_type)

        self.phc = PEPHubClient()
        self.record_identifier = record_identifier
        self.pephub_path = pephub_path
        self.pipeline_name = pipeline_name
        self.parsed_schema = parsed_schema
        self.status_schema = status_schema
        self.result_formatter = result_formatter

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

    def check_record_exists(
        self,
        record_identifier: str,
    ) -> bool:
        """
        Check if the specified record exists in the table

        :param str record_identifier: record to check for
        :return bool: whether the record exists in the table
        """

        query_hit = self.select_records(
            filter_conditions=[
                {
                    "key": "record_identifier",
                    "operator": "eq",
                    "value": record_identifier,
                }
            ]
        )

        return bool(query_hit["records"])

    def list_results(
        self,
        restrict_to: Optional[List[str]] = None,
        record_identifier: str = None,
    ) -> List[str]:
        """
        Check if the specified results exist in the table

        :param List[str] restrict_to: results identifiers to check for
        :param str record_identifier: record to check for
        :return List[str] existing: if no result identifier specified, return all results for the record
        :return List[str]: results identifiers that exist
        """
        rid = record_identifier
        record = self.select_records(
            filter_conditions=[
                {
                    "key": "record_identifier",
                    "operator": "eq",
                    "value": rid,
                }
            ]
        )
        try:
            # TODO had to add rid to step deeper into data structure vs dbbackend implementation. Why are they different?
            record = record["records"][0][rid]
        except IndexError:
            return []

        if restrict_to is None:
            return (
                [
                    key
                    for key in self.parsed_schema.results_data.keys()
                    if getattr(record, key, None) is not None
                ]
                if record
                else []
            )
        else:
            return [r for r in restrict_to if record.get(r, None) is not None] if record else []

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified, the entire record
        will be removed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :return bool: whether the result has been removed
        """

        record_identifier = record_identifier or self.record_identifier

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

        if result_identifier:
            values = {result_identifier: ""}
            self.phc.sample.update(
                namespace=self.pep_registry.namespace,
                name=self.pep_registry.item,
                tag=self.pep_registry.tag,
                sample_name=record_identifier,
                sample_dict=values,
            )
            return True
        else:
            self.remove_record(
                record_identifier=record_identifier,
                rm_record=True,
            )
            return True

    def remove_record(
        self,
        record_identifier: Optional[str] = None,
        rm_record: Optional[bool] = False,
    ) -> NoReturn:
        """
        Remove a record, requires rm_record to be True

        :param str record_identifier: unique identifier of the record
        :param bool rm_record: bool for removing record.
        :return bool: whether the result has been removed
        :raises RecordNotFoundError: if record not found
        """
        if rm_record:
            self.phc.sample.remove(
                namespace=self.pep_registry.namespace,
                name=self.pep_registry.item,
                tag=self.pep_registry.tag,
                sample_name=record_identifier,
            )
        else:
            _LOGGER.info(f" rm_record flag False, aborting Removing '{record_identifier}' record")

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: Optional[str] = None,
        force_overwrite: bool = True,
        result_formatter: Optional[staticmethod] = None,
        history_enabled: Optional[bool] = False,
    ) -> Union[List[str], bool]:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param history_enabled: this parameter is currently ignored as PEPHub
        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        :param str record_identifier: unique identifier of the record
        :param bool force_overwrite: Toggles force overwriting results, defaults to False
        :param str result_formatter: function for formatting result
        :return bool | list[str] results_formatted: return list of formatted string
        """
        if history_enabled:
            _LOGGER.warning(
                msg="history_enabled set to true but this feature is handled by PEPHub and not Pipestat"
            )

        record_identifier = record_identifier or self.record_identifier
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

        if not existing:

            # try:
            self.phc.sample.create(
                namespace=self.pep_registry.namespace,
                name=self.pep_registry.item,
                tag=self.pep_registry.tag,
                sample_name=record_identifier,
                sample_dict=values,
                overwrite=force_overwrite,
            )

        elif existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(f"These results exist for '{record_identifier}': {existing_str}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")

            self.phc.sample.update(
                namespace=self.pep_registry.namespace,
                name=self.pep_registry.item,
                tag=self.pep_registry.tag,
                sample_name=record_identifier,
                sample_dict=values,
            )

        for res_id, val in values.items():
            results_formatted.append(
                result_formatter(
                    pipeline_name=self.pipeline_name,
                    record_identifier=record_identifier,
                    res_id=res_id,
                    value=val,
                )
            )
        return results_formatted

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

        record_identifier = record_identifier or self.record_identifier
        known_status_identifiers = self.status_schema.keys()
        if status_identifier not in known_status_identifiers:
            raise UnrecognizedStatusError(
                f"'{status_identifier}' is not a defined status identifier. "
                f"These are allowed: {known_status_identifiers}"
            )
        prev_status = self.get_status(record_identifier)
        try:
            self.report(
                values={STATUS: status_identifier},
                record_identifier=record_identifier,
            )
        except Exception as e:
            _LOGGER.error(
                f"Could not insert into the status table ('{self.table_name}'). Exception: {e}"
            )
            raise
        if prev_status:
            _LOGGER.debug(f"Changed status from '{prev_status}' to '{status_identifier}'")

    def get_status(self, record_identifier: str) -> Optional[str]:
        """
        Get pipeline status

        :param str record_identifier: record identifier to set the
            pipeline status for
        :return str status
        """

        try:
            result = self.select_records(
                columns=[STATUS],
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": record_identifier,
                    }
                ],
            )
        except RecordNotFoundError:
            return None
        try:
            status = result["records"][0][record_identifier]["status"]
        except IndexError or KeyError:
            status = None

        if status == "":  # PEPhub returns '' for empty cell
            status = None
        return status

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
                return "=="
            if op == "lt":
                return "<"
            if op == "ge":
                return ">="
            if op == "gt":
                return ">"
            if op == "in":
                return "in"
            raise ValueError(f"Invalid filter operator: {op}")

        # Can we use query_param to do cursor/limit operations if the PEP is very large?
        project = self.phc.load_project(project_registry_path=self.pephub_path)
        print(project)

        # PEPHub uses sample_name not record_identifier
        # Just get the items from the sample table because it is a dataframe and return the dict to the end user
        if columns is not None:
            columns = copy.deepcopy(columns)
            for i in ["sample_name"]:  # Must add id, need it for cursor
                if i not in columns:
                    columns.insert(0, i)
            try:
                df = project.sample_table[columns]
            except KeyError:
                records_dict = {
                    "total_size": 0,
                    "page_size": limit,
                    "next_page_token": 0,
                    "records": [],
                }
                return records_dict

        else:
            df = project.sample_table

        total_count = len(df)

        if filter_conditions:
            filter_expression = ""
            all_filter_expressions = []
            for filter_condition in filter_conditions:
                retrieved_operator = get_operator(filter_condition["operator"])
                if filter_condition["key"] == "record_identifier":
                    filter_condition["key"] = "sample_name"

                key = filter_condition["key"]
                value = filter_condition["value"]
                # Create querry for df based on filter conditions

                if isinstance(value, list):
                    filter_expression = f"{key} {retrieved_operator} {value}"
                else:
                    filter_expression = f"{key} {retrieved_operator} '{value}'"
                all_filter_expressions.append(filter_expression)

            if len(all_filter_expressions) > 1:

                # This is for AND logic
                if bool_operator == "AND":
                    for filter in all_filter_expressions:
                        df = df.query(filter)
                if bool_operator == "OR":
                    filter = f"({' | '.join(str(cond) for cond in all_filter_expressions)})"
                    df = df.query(filter)

            else:
                df = df.query(filter_expression)

            print("done")

        # Once we have the dataframe (filtered or unfiltered), convert to a dict using the sample_name/record_identifier as the primary key
        df2dict = df.set_index("sample_name").transpose().to_dict(orient="dict")

        # Filter out columns
        # Must do this to align output structure with that of db_backend and file_backends
        records_list = []
        for key, value in df2dict.items():
            records_list.append({key: value})

        records_dict = {
            "total_size": total_count,
            "page_size": limit,
            "next_page_token": 0,
            "records": records_list,
        }

        return records_dict
