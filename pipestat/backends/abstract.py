import os
from abc import ABC
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, Union

from ubiquerg import expandpath

from ..const import PKG_NAME, STATUS
from ..exceptions import SchemaError
from ..helpers import force_symlink

_LOGGER = getLogger(PKG_NAME)


class PipestatBackend(ABC):
    """Abstract class representing a pipestat backend."""

    def __init__(self, pipeline_type):
        _LOGGER.debug("Initialize PipestatBackend")
        self.pipeline_type = pipeline_type

    def assert_results_defined(self, results: List[str], pipeline_type: str) -> None:
        """Assert provided list of results is defined in the schema.

        Args:
            results (List[str]): List of results to check for existence in the schema.
            pipeline_type (str): "sample" or "project".

        Raises:
            SchemaError: If any of the results is not defined in the schema.
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
        """Check if the result has been reported.

        Args:
            result_identifier (str): Name of the result to check.
            record_identifier (str, optional): Unique identifier of the record. Defaults to None.

        Returns:
            bool: Whether the specified result has been reported for the indicated record in current namespace.
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

    def get_status(self, record_identifier: str) -> Optional[str]:
        _LOGGER.warning("Not implemented yet for this backend")

    def link(self, link_dir) -> str:
        """This function creates a link structure such that results are organized by type.

        Args:
            link_dir (str): Path to desired symlink output directory (does not have to be absolute).

        Returns:
            str: Absolute path to symlink directory.
        """

        def get_all_paths(parent_key, result_identifier_value):
            """If the result identifier is a complex object which contains nested paths.

            Args:
                parent_key: Parent key name.
                result_identifier_value: Result identifier value to extract paths from.

            Returns:
                list: List of (key, path) tuples.
            """

            key_value_pairs = []

            for k, v in result_identifier_value.items():
                if isinstance(v, dict):
                    key_value_pairs.extend(get_all_paths(k, v))
                elif k == "path":
                    key_value_pairs.append((parent_key, v))
            return key_value_pairs

        unique_result_identifiers = []

        link_dir = expandpath(link_dir)

        all_records = self.select_records()

        for record in all_records["records"]:
            # result_identifiers = record.keys() #self.select_records(record_identifier=record["record_identifier"])
            for k, v in record.items():
                if isinstance(v, dict):
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
                        linkname = os.path.join(
                            target_dir,
                            record["record_identifier"] + "_" + path[0] + "_" + file,
                        )
                        src = os.path.abspath(path[1])
                        src_rel = os.path.relpath(src, os.path.dirname(linkname))
                        force_symlink(src_rel, linkname)

        return link_dir

    def clear_status(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        """Clear status flags (not implemented in abstract backend).

        Args:
            record_identifier (str, optional): Record identifier. Defaults to None.
            flag_names (List[str], optional): Names of flags to clear. Defaults to None.

        Returns:
            List[Union[str, None]]: Collection of cleared flag names.
        """
        _LOGGER.warning("Not implemented yet for this backend")

    def set_status(
        self,
        status_identifier: str,
        record_identifier: Optional[str] = None,
    ) -> None:
        """Set pipeline status (not implemented in abstract backend).

        Args:
            status_identifier (str): Status identifier to set.
            record_identifier (str, optional): Record identifier. Defaults to None.
        """
        _LOGGER.warning("Not implemented yet for this backend")

    def list_results(self) -> List[str]:
        """List results (not implemented in abstract backend).

        Returns:
            List[str]: List of result identifiers.
        """
        _LOGGER.warning("Not implemented yet for this backend")

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str,
        force_overwrite: bool = False,
        result_formatter: Optional[staticmethod] = None,
        history_enabled: bool = True,
    ) -> str:
        """Report results (not implemented in abstract backend).

        Args:
            values (Dict[str, Any]): Dictionary of result-value pairs.
            record_identifier (str): Record identifier.
            force_overwrite (bool, optional): Whether to overwrite existing results. Defaults to False.
            result_formatter (staticmethod, optional): Function for formatting results. Defaults to None.
            history_enabled (bool, optional): Should history be enabled. Defaults to True.

        Returns:
            str: Formatted report string.
        """
        _LOGGER.warning("Not implemented yet for this backend")

    def retrieve_distinct(
        self,
        columns: Optional[List[str]] = None,
    ) -> List[Any]:
        """Retrieve distinct values (not implemented in abstract backend).

        Args:
            columns (List[str], optional): Columns to retrieve distinct values for. Defaults to None.

        Returns:
            List[Any]: List of distinct values.
        """
        _LOGGER.warning("Not implemented yet for this backend")

    def remove(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
    ) -> bool:
        """Remove results (not implemented in abstract backend).

        Args:
            record_identifier (str, optional): Record identifier. Defaults to None.
            result_identifier (str, optional): Result identifier. Defaults to None.

        Returns:
            bool: Whether removal was successful.
        """
        _LOGGER.warning("Not implemented yet for this backend")

    def remove_record(
        self,
        record_identifier: Optional[str] = None,
        rm_record: Optional[bool] = False,
    ) -> bool:
        """Remove record (not implemented in abstract backend).

        Args:
            record_identifier (str, optional): Record identifier. Defaults to None.
            rm_record (bool, optional): Whether to remove record. Defaults to False.

        Returns:
            bool: Whether removal was successful.
        """
        _LOGGER.warning("Not implemented yet for this backend")


def select_records(
    self,
    columns: Optional[List[str]] = None,
    filter_conditions: Optional[List[Dict[str, Any]]] = None,
    limit: Optional[int] = 1000,
    cursor: Optional[int] = None,
    bool_operator: Optional[str] = "AND",
) -> Dict[str, Any]:
    """Select records (not implemented in abstract backend).

    Args:
        columns (List[str], optional): Columns to select. Defaults to None.
        filter_conditions (List[Dict[str, Any]], optional): Filter conditions. Defaults to None.
        limit (int, optional): Maximum number of records to return. Defaults to 1000.
        cursor (int, optional): Cursor for pagination. Defaults to None.
        bool_operator (str, optional): Boolean operator for filters. Defaults to "AND".

    Returns:
        Dict[str, Any]: Dictionary containing selected records.
    """
    _LOGGER.warning("Not implemented yet for this backend")


def select_distinct(
    self,
    columns,
) -> List[Tuple]:
    """Select distinct values (not implemented in abstract backend).

    Args:
        columns: Columns to select distinct values from.

    Returns:
        List[Tuple]: List of tuples containing distinct values.
    """
    _LOGGER.warning("Not implemented yet for this backend")
