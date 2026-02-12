import datetime
import os
from abc import ABC
from collections.abc import Callable, Iterator, MutableMapping
from copy import deepcopy
from logging import getLogger
from typing import Any

from jsonschema import validate
from ubiquerg import mkabs
from yacman import YAMLConfigManager, load_yaml, select_config

from pipestat.backends.file_backend.filebackend import FileBackend

from .const import (
    CFG_DATABASE_KEY,
    CFG_SCHEMA,
    CONFIG_KEY,
    CREATED_TIME,
    DB_ONLY_KEY,
    DB_URL,
    DEFAULT_PIPELINE_NAME,
    ENV_VARS,
    FILE_KEY,
    MODIFIED_TIME,
    MULTI_PIPELINE,
    OUTPUT_DIR,
    PIPELINE_NAME,
    PIPELINE_TYPE,
    PKG_NAME,
    PROJECT_NAME,
    RECORD_IDENTIFIER,
    RESULT_FORMATTER,
    SAMPLE_NAME_ID_KEY,
    SCHEMA_KEY,
    SCHEMA_PATH,
    STATUS_FILE_DIR,
    STATUS_SCHEMA,
    STATUS_SCHEMA_KEY,
    STATUS_SCHEMA_SOURCE_KEY,
)
from .exceptions import (
    ColumnNotFoundError,
    InvalidTimeFormatError,
    NoBackendSpecifiedError,
    PipestatDatabaseError,
    PipestatDependencyError,
    PipestatSummarizeError,
    RecordNotFoundError,
    SchemaNotFoundError,
)
from .helpers import default_formatter, make_subdirectories, validate_type, zip_report
from .reports import HTMLReportBuilder, _create_stats_objs_summaries

try:
    from pipestat.backends.db_backend.db_parsed_schema import ParsedSchemaDB as ParsedSchema
except ImportError:
    from .parsed_schema import ParsedSchema

try:
    from pipestat.backends.db_backend.db_helpers import construct_db_url
    from pipestat.backends.db_backend.dbbackend import DBBackend
except ImportError:
    # We let this pass, but if the user attempts to create DBBackend, check_dependencies raises exception.
    pass

try:
    from pipestat.backends.pephub_backend.pephubbackend import PEPHUBBACKEND
except ImportError:
    # Let this pass, if phc dependencies cannot be imported, raise exception
    pass


_LOGGER = getLogger(PKG_NAME)


def check_dependencies(dependency_list: list | None = None, msg: str | None = None) -> Callable:
    """Decorator to check that the dependency list has successfully been imported.

    Args:
        dependency_list (list, optional): List of dependencies to check for import.
        msg (str, optional): Error message to display if dependencies are not satisfied.

    Returns:
        function: Decorated function that checks dependencies before execution.

    Raises:
        PipestatDependencyError: If any required dependencies are missing.
    """

    def wrapper(func: Callable) -> Callable:
        def inner(*args, **kwargs) -> Any:
            dependencies_satisfied = True
            if dependency_list is not None:
                for i in dependency_list:
                    if i not in globals():
                        _LOGGER.warning(msg=f"Missing dependency: {i}")
                        dependencies_satisfied = False
            if dependencies_satisfied is False:
                raise PipestatDependencyError(msg=msg)
            return func(*args, **kwargs)

        return inner

    return wrapper


def require_backend(func: Callable) -> Callable:
    """Decorator to ensure a backend exists for functions that require one.

    Args:
        func (function): Function that requires a backend.

    Returns:
        function: Decorated function that checks for backend existence.

    Raises:
        NoBackendSpecifiedError: If no backend is configured.
    """

    def inner(self, *args, **kwargs) -> Any:
        if not self.backend:
            raise NoBackendSpecifiedError
        return func(self, *args, **kwargs)

    return inner


class PipestatManager(MutableMapping):
    """Pipestat standardizes reporting of pipeline results and pipeline status management.

    It formalizes a way for pipeline developers and downstream tools developers to
    communicate -- results produced by a pipeline can easily and reliably become an
    input for downstream analyses. A PipestatManager object exposes an API for
    interacting with the results and pipeline status and can be backed by either
    a YAML-formatted file or a database.
    """

    def __init__(
        self,
        project_name: str | None = None,
        record_identifier: str | None = None,
        schema_path: str | None = None,
        results_file_path: str | None = None,
        database_only: bool | None = True,
        config_file: str | None = None,
        config_dict: dict | None = None,
        flag_file_dir: str | None = None,
        show_db_logs: bool = False,
        pipeline_type: str | None = None,
        pipeline_name: str | None = None,
        result_formatter: Callable = default_formatter,
        multi_pipelines: bool = False,
        output_dir: str | None = None,
        pephub_path: str | None = None,
        lenient: bool = False,
    ) -> None:
        """Initialize the PipestatManager object.

        Args:
            project_name (str, optional): Name of the project.
            record_identifier (str, optional): Record identifier to report for. This creates
                a weak bound to the record, which can be overridden in object method calls.
            schema_path (str, optional): Path to the output schema that formalizes the results structure.
            results_file_path (str, optional): YAML file to report into, if file is used as the object back-end.
            database_only (bool, optional): Whether the reported data should not be stored in memory, but only in the database. Defaults to True.
            config_file (str, optional): Path to the configuration file.
            config_dict (dict, optional): A mapping with the config file content.
            flag_file_dir (str, optional): Path to directory containing flag files.
            show_db_logs (bool, optional): Toggles showing database logs. Defaults to False.
            pipeline_type (str, optional): "sample" or "project".
            pipeline_name (str, optional): Name of the current pipeline.
            result_formatter (staticmethod, optional): Function for formatting result. Defaults to default_formatter.
            multi_pipelines (bool, optional): Allows for running multiple pipelines for one file backend. Defaults to False.
            output_dir (str, optional): Target directory for report generation via summarize and table generation via table.
            pephub_path (str, optional): Path to PEPHub registry.
            lenient (bool, optional): Allow reporting results without schema validation. Requires file backend. Defaults to False.
        """

        super(PipestatManager, self).__init__()

        # Initialize the cfg dict as an attribute that holds all configuration data
        self.cfg = {}
        self.cfg["lenient"] = lenient

        # Load and validate database configuration
        # If results_file_path exists, backend is a file else backend is database.

        self.cfg["config_path"] = select_config(config_file, ENV_VARS["config"])

        if config_dict is not None:
            self.cfg[CONFIG_KEY] = YAMLConfigManager.from_obj(entries=config_dict)
        elif self.cfg["config_path"] is not None:
            self.cfg[CONFIG_KEY] = YAMLConfigManager.from_yaml_file(
                filepath=self.cfg["config_path"]
            )
        else:
            self.cfg[CONFIG_KEY] = YAMLConfigManager()

        cfg_schema = load_yaml(CFG_SCHEMA)
        validate(self.cfg[CONFIG_KEY].exp, cfg_schema)

        self.cfg["pephub_path"] = self.cfg[CONFIG_KEY].priority_get(
            "pephub_path", override=pephub_path
        )

        self.cfg[SCHEMA_PATH] = self.cfg[CONFIG_KEY].priority_get(
            "schema_path", env_var=ENV_VARS["schema"], override=schema_path
        )
        self.process_schema(schema_path)

        if self.cfg.get(SCHEMA_KEY):
            _LOGGER.debug(
                f"Schema loaded: {len(self.cfg[SCHEMA_KEY].sample_level_data)} sample-level keys, "
                f"{len(self.cfg[SCHEMA_KEY].project_level_data)} project-level keys"
            )

        self.cfg[RECORD_IDENTIFIER] = self.cfg[CONFIG_KEY].priority_get(
            "record_identifier", env_var=ENV_VARS["record_identifier"], override=record_identifier
        )

        # TODO this is a work around for Looper ~ https://github.com/pepkit/looper/issues/492, sharing pipeline names
        # In the future, we should get pipeline name only from output schema.
        if pipeline_name:
            self.cfg[PIPELINE_NAME] = pipeline_name
        elif self.cfg[CONFIG_KEY].get(PIPELINE_NAME):
            self.cfg[PIPELINE_NAME] = self.cfg[CONFIG_KEY].get(PIPELINE_NAME)
        elif self.cfg[SCHEMA_KEY] and self.cfg[SCHEMA_KEY].pipeline_name:
            self.cfg[PIPELINE_NAME] = self.cfg[SCHEMA_KEY].pipeline_name
        else:
            self.cfg[PIPELINE_NAME] = DEFAULT_PIPELINE_NAME

        self.cfg[PROJECT_NAME] = self.cfg[CONFIG_KEY].priority_get(
            "project_name", env_var=ENV_VARS["project_name"], override=project_name
        )

        self.cfg[SAMPLE_NAME_ID_KEY] = self.cfg[CONFIG_KEY].priority_get(
            "record_identifier",
            env_var=ENV_VARS["sample_name"],
            override=record_identifier,
        )

        self.cfg[DB_ONLY_KEY] = database_only

        self.cfg[PIPELINE_TYPE] = self.cfg[CONFIG_KEY].priority_get(
            "pipeline_type", default="sample", override=pipeline_type
        )

        self.cfg[FILE_KEY] = mkabs(
            self.resolve_results_file_path(
                self.cfg[CONFIG_KEY].priority_get(
                    "results_file_path",
                    env_var=ENV_VARS["results_file"],
                    override=results_file_path,
                )
            ),
            self.cfg["config_path"],
        )

        if "{record_identifier}" in str(self.cfg[FILE_KEY]):
            # In the special case where the user wants to use {record_identifier} in file path
            pass
        else:
            make_subdirectories(self.cfg[FILE_KEY])

        self.cfg[RESULT_FORMATTER] = result_formatter

        self.cfg[MULTI_PIPELINE] = multi_pipelines

        self.cfg["multi_result_files"] = None

        self.cfg[OUTPUT_DIR] = self.cfg[CONFIG_KEY].priority_get("output_dir", override=output_dir)

        # Validate lenient mode requirements
        if self.cfg["lenient"]:
            if not self.cfg[FILE_KEY]:
                raise PipestatDatabaseError(
                    "Lenient mode requires file backend. "
                    "Use 'pipestat infer-schema' to generate a schema from your results, "
                    "then switch to database backend with the generated schema."
                )

        if self.cfg[FILE_KEY]:
            self.initialize_filebackend(record_identifier, results_file_path, flag_file_dir)

        elif self.cfg["pephub_path"]:
            self.initialize_pephubbackend(record_identifier, self.cfg["pephub_path"])
        else:
            self.initialize_dbbackend(record_identifier, show_db_logs)

    def __str__(self):
        """Generate string representation of the object.

        Returns:
            str: String representation of the object.
        """
        res = f"{self.__class__.__name__} ({self.cfg[PIPELINE_NAME]})"
        res += "\nBackend: {}".format(
            f"File\n - results: {self.cfg[FILE_KEY]}\n - status: {self.cfg[STATUS_FILE_DIR]}"
            if self.cfg[FILE_KEY]
            else f"Database (dialect: {self.backend.db_engine_key})"
        )
        if self.cfg[FILE_KEY]:
            res += f"\nMultiple Pipelines Allowed: {self.cfg[MULTI_PIPELINE]}"
        else:
            res += f"\nProject Name: {self.cfg[PROJECT_NAME]}"
            res += f"\nDatabase URL: {self.cfg[DB_URL]}"
            res += f"\nConfig File: {self.config_path}"

        res += f"\nPipeline name: {self.cfg[PIPELINE_NAME]}"
        res += f"\nPipeline type: {self.cfg[PIPELINE_TYPE]}"
        if self.cfg[SCHEMA_PATH] is not None:
            res += "\nProject Level Data:"
            for k, v in self.cfg[SCHEMA_KEY].project_level_data.items():
                res += f"\n {k} : {v}"
            res += "\nSample Level Data:"
            for k, v in self.cfg[SCHEMA_KEY].sample_level_data.items():
                res += f"\n {k} : {v}"
        res += f"\nStatus Schema key: {self.cfg[STATUS_SCHEMA_KEY]}"
        res += f"\nResults formatter: {str(self.cfg[RESULT_FORMATTER].__name__)}"
        res += f"\nResults schema source: {self.cfg[SCHEMA_PATH]}"
        res += f"\nStatus schema source: {self.cfg[STATUS_SCHEMA_SOURCE_KEY]}"
        res += f"\nRecords count: {self.record_count}"
        if self.cfg[SCHEMA_PATH] is not None:
            high_res = self.highlighted_results
        else:
            high_res = None
        if high_res:
            res += f"\nHighlighted results: {', '.join(high_res)}"
        return res

    def __getitem__(self, key: str) -> Any:
        # This is a wrapper for the retrieve function:
        result = self.retrieve_one(record_identifier=key)
        return result

    def __setitem__(self, key: str, value: Any) -> list[str] | bool:
        # This is a wrapper for the report function:
        result = self.report(record_identifier=key, values=value)
        return result

    def __delitem__(self, key: str) -> bool:
        # This is a wrapper for the remove function; it removes the entire record:
        result = self.remove(record_identifier=key)
        return result

    def __iter__(
        self,
        limit: int | None = 1000,
        cursor: int | None = None,
    ) -> Iterator:
        """Wrapper around select_records that creates an iterator of records.

        Args:
            limit (int, optional): Maximum number of results to retrieve per page. Defaults to 1000.
            cursor (int, optional): Cursor position to begin retrieving records.

        Returns:
            Iterator: Iterator over records.
        """
        if self.file:
            # File backend does not support cursor-based paging
            return iter(self.select_records(limit=limit)["records"])
        else:
            return iter(self.select_records(limit=limit, cursor=cursor)["records"])

    def __len__(self) -> int:
        return len(self.cfg)

    def resolve_results_file_path(self, results_file_path: str | None) -> str | None:
        """Replace {record_identifier} in results_file_path if it exists.

        Args:
            results_file_path (str): YAML file to report into, if file is used as the object back-end.

        Returns:
            str: Resolved file path with record_identifier substituted if applicable.
        """
        # Save for later when assessing if there may be multiple result files
        if results_file_path:
            assert isinstance(results_file_path, str), TypeError("Path is expected to be a str")
            if self.record_identifier:
                try:
                    self.cfg["unresolved_result_path"] = results_file_path
                    results_file_path = results_file_path.format(
                        record_identifier=self.record_identifier
                    )
                    return results_file_path
                except AttributeError:
                    self.cfg["unresolved_result_path"] = results_file_path
                    return results_file_path
            else:
                self.cfg["unresolved_result_path"] = results_file_path
                return results_file_path
        return results_file_path

    def initialize_filebackend(
        self,
        record_identifier: str | None = None,
        results_file_path: str | None = None,
        flag_file_dir: str | None = None,
    ) -> None:
        """Initializes the file backend.

        Args:
            record_identifier (str, optional): The record identifier.
            results_file_path (str, optional): The path to the results file used for the backend.
            flag_file_dir (str, optional): The path to the flag file directory.
        """

        # Check if there will be multiple results_file_paths
        _LOGGER.debug(f"Determined file as backend: {results_file_path}")

        if self.cfg[DB_ONLY_KEY]:
            _LOGGER.debug(
                "Running in database only mode does not make sense with a YAML file as a backend. "
                "Changing back to using memory."
            )
            self.cfg[DB_ONLY_KEY] = False

        flag_file_dir = self.cfg[CONFIG_KEY].priority_get(
            "flag_file_dir",
            override=flag_file_dir,
            default=os.path.dirname(self.cfg[FILE_KEY]),
        )
        self.cfg[STATUS_FILE_DIR] = mkabs(flag_file_dir, self.config_path or self.cfg[FILE_KEY])
        make_subdirectories(self.cfg[STATUS_FILE_DIR])

        self.backend = FileBackend(
            self.cfg[FILE_KEY],
            record_identifier,
            self.cfg[PIPELINE_NAME],
            self.cfg[PIPELINE_TYPE],
            self.cfg[SCHEMA_KEY],
            self.cfg[STATUS_SCHEMA_KEY],
            self.cfg[STATUS_FILE_DIR],
            self.cfg[RESULT_FORMATTER],
            self.cfg[MULTI_PIPELINE],
            self.cfg.get("lenient", False),
        )

        return

    def initialize_pephubbackend(
        self, record_identifier: str | None = None, pephub_path: str | None = None
    ) -> None:
        """Initializes the pephub backend.

        Args:
            record_identifier (str, optional): The record identifier.
            pephub_path (str, optional): The path to the pephub registry.
        """
        self.backend = PEPHUBBACKEND(
            record_identifier,
            pephub_path,
            self.cfg[PIPELINE_NAME],
            self.cfg[PIPELINE_TYPE],
            self.cfg[SCHEMA_KEY],
            self.cfg[STATUS_SCHEMA_KEY],
            self.cfg[RESULT_FORMATTER],
        )

    @check_dependencies(
        dependency_list=["DBBackend"],
        msg="Missing required dependencies for this usage, e.g. try pip install pipestat['dbbackend']",
    )
    def initialize_dbbackend(
        self, record_identifier: str | None = None, show_db_logs: bool = False
    ) -> None:
        """Initializes the database backend.

        Args:
            record_identifier (str, optional): The record identifier.
            show_db_logs (bool, optional): Boolean to show database logs. Defaults to False.

        Raises:
            SchemaNotFoundError: If output schema is not supplied for DB backends.
            NoBackendSpecifiedError: If database configuration is missing.
            PipestatDatabaseError: If database configuration is invalid.
        """
        _LOGGER.debug("Determined database as backend")
        if self.cfg[SCHEMA_KEY] is None:
            raise SchemaNotFoundError("Output schema must be supplied for DB backends.")
        if CFG_DATABASE_KEY not in self.cfg[CONFIG_KEY]:
            raise NoBackendSpecifiedError()
        try:
            dbconf = self.cfg[CONFIG_KEY].exp[
                CFG_DATABASE_KEY
            ]  # the .exp expands the paths before url construction
            if "sqlite_url" in dbconf:
                sqlite_url = f"sqlite:///{dbconf['sqlite_url']}"
                self.cfg[DB_URL] = sqlite_url
            else:
                self.cfg[DB_URL] = construct_db_url(dbconf)
        except KeyError:
            raise PipestatDatabaseError(f"No database section ('{CFG_DATABASE_KEY}') in config")
        self._show_db_logs = show_db_logs

        self.backend = DBBackend(
            record_identifier,
            self.cfg[PIPELINE_NAME],
            show_db_logs,
            self.cfg[PIPELINE_TYPE],
            self.cfg[SCHEMA_KEY],
            self.cfg[STATUS_SCHEMA_KEY],
            self.cfg[DB_URL],
            self.cfg[STATUS_SCHEMA_SOURCE_KEY],
            self.cfg[RESULT_FORMATTER],
        )

    @require_backend
    def clear_status(
        self,
        record_identifier: str = None,
        flag_names: list[str] = None,
    ) -> list[str | None]:
        """Remove status flags.

        Args:
            record_identifier (str, optional): Name of the sample_level record to remove flags for.
            flag_names (List[str], optional): Names of flags to remove; if unspecified,
                all schema-defined flag names will be used.

        Returns:
            List[Union[str, None]]: Collection of names of flags removed.
        """

        r_id = record_identifier or self.record_identifier
        return self.backend.clear_status(record_identifier=r_id, flag_names=flag_names)

    @require_backend
    def count_records(self) -> int:
        """Count records.

        Returns:
            int: Number of records.
        """
        return self.backend.count_records()

    @require_backend
    def get_status(
        self,
        record_identifier: str = None,
    ) -> str | None:
        """Get the current pipeline status.

        Args:
            record_identifier (str, optional): Name of the sample_level record.

        Returns:
            str: Status identifier, e.g. 'running'.
        """

        r_id = record_identifier or self.record_identifier
        return self.backend.get_status(record_identifier=r_id)

    @require_backend
    def list_recent_results(
        self,
        limit: int | None = 1000,
        start: str | None = None,
        end: str | None = None,
        time_column: str | None = "modified",
    ) -> dict:
        """List recent results within a time range.

        Args:
            limit (int, optional): Limit number of results returned. Defaults to 1000.
            start (str, optional): Most recent result to filter on, defaults to now.
                Format: YYYY-MM-DD HH:MM:SS, e.g. 2023-10-16 13:03:04.
            end (str, optional): Oldest result to filter on.
                Format: YYYY-MM-DD HH:MM:SS, e.g. 1970-10-16 13:03:04.
            time_column (str, optional): Created or modified column/attribute to filter on. Defaults to "modified".

        Returns:
            dict: A dict containing start, end, num of records, and list of retrieved records.

        Raises:
            InvalidTimeFormatError: If start or end time format is incorrect.
        """

        if self.cfg["pephub_path"]:
            _LOGGER.warning("List recent results not supported for PEPHub backend")
            return {}
        date_format = "%Y-%m-%d %H:%M:%S"
        if start is None:
            start = datetime.datetime.now()
        else:
            try:
                start = datetime.datetime.strptime(start, date_format)
            except ValueError:
                raise InvalidTimeFormatError(msg=f"Incorrect time format, requires:{date_format}")

        if end is None:
            end = datetime.datetime.strptime("1900-01-01 00:00:00", date_format)
        else:
            try:
                end = datetime.datetime.strptime(end, date_format)
            except ValueError:
                raise InvalidTimeFormatError(msg=f"Incorrect time format, requires: {date_format}")

        if time_column == "created":
            col_name = CREATED_TIME
        else:
            col_name = MODIFIED_TIME

        results = self.select_records(
            limit=limit,
            filter_conditions=[
                {
                    "key": col_name,
                    "operator": "lt",
                    "value": start,
                },
                {
                    "key": col_name,
                    "operator": "gt",
                    "value": end,
                },
            ],
        )
        return results

    def process_schema(self, schema_path: str | None) -> None:
        # Load pipestat schema in two parts: 1) main and 2) status
        self._schema_path = self.cfg[CONFIG_KEY].priority_get(
            "schema_path", env_var=ENV_VARS["schema"], override=schema_path
        )

        if self._schema_path is None:
            _LOGGER.warning("No pipestat output schema was supplied to PipestatManager.")
            self.cfg[SCHEMA_KEY] = None
            self.cfg[STATUS_SCHEMA_KEY] = None
            self.cfg[STATUS_SCHEMA_SOURCE_KEY] = None
            # return None
            # raise SchemaNotFoundError("PipestatManager creation failed; no schema")
        else:
            # Main schema
            schema_to_read = mkabs(self._schema_path, self.cfg["config_path"])
            self._schema_path = schema_to_read
            parsed_schema = ParsedSchema(schema_to_read)
            self.cfg[SCHEMA_KEY] = parsed_schema

            # Status schema
            self.cfg[STATUS_SCHEMA_KEY] = parsed_schema.status_data
            if not self.cfg[STATUS_SCHEMA_KEY]:
                self.cfg[STATUS_SCHEMA_SOURCE_KEY] = STATUS_SCHEMA
                self.cfg[STATUS_SCHEMA_KEY] = load_yaml(filepath=STATUS_SCHEMA)
            else:
                self.cfg[STATUS_SCHEMA_SOURCE_KEY] = schema_to_read

    @require_backend
    def remove(
        self,
        record_identifier: str = None,
        result_identifier: str = None,
    ) -> bool:
        """Remove a result.

        If no result ID specified or last result is removed, the entire record will be removed.

        Args:
            record_identifier (str, optional): Name of the sample_level record.
            result_identifier (str, optional): Name of the result to be removed or None
                if the record should be removed.

        Returns:
            bool: Whether the result has been removed.
        """

        r_id = record_identifier or self.cfg[RECORD_IDENTIFIER]
        return self.backend.remove(
            record_identifier=r_id,
            result_identifier=result_identifier,
        )

    @require_backend
    def remove_record(
        self,
        record_identifier: str | None = None,
        rm_record: bool | None = False,
    ) -> bool:
        return self.backend.remove_record(
            record_identifier=record_identifier,
            rm_record=rm_record,
        )

    @require_backend
    def report(
        self,
        values: dict[str, Any],
        record_identifier: str | None = None,
        force_overwrite: bool = True,
        result_formatter: Callable | None = None,
        strict_type: bool = True,
        history_enabled: bool = True,
    ) -> list[str] | bool:
        """Report a result.

        Args:
            values (Dict[str, Any]): Dictionary of result-value pairs.
            record_identifier (str, optional): Unique identifier of the record, value in
                'record_identifier' column to look for to determine if the record already exists.
            force_overwrite (bool, optional): Whether to overwrite the existing record. Defaults to True.
            result_formatter (staticmethod, optional): Function for formatting result.
            strict_type (bool, optional): Whether the type of the reported values should remain as is.
                Pipestat would attempt to convert to the schema-defined one otherwise. Defaults to True.
            history_enabled (bool, optional): Should history of reported results be enabled? Defaults to True.

        Returns:
            Union[List[str], bool]: List of formatted strings for reported results.

        Raises:
            NotImplementedError: If no record identifier is supplied.
            ColumnNotFoundError: If a result attribute is not defined in the output schema.
        """

        result_formatter = result_formatter or self.cfg[RESULT_FORMATTER]
        values = deepcopy(values)
        r_id = record_identifier or self.cfg[RECORD_IDENTIFIER]
        if r_id is None:
            raise NotImplementedError("You must supply a record identifier to report results")

        result_identifiers = list(values.keys())

        # Handle lenient mode: auto-wrap file paths and skip validation for unknown keys
        if self.cfg.get("lenient"):
            for r in result_identifiers:
                values[r] = self._infer_and_wrap(r, values[r])

        if self.cfg[SCHEMA_KEY] is not None:
            for r in result_identifiers:
                # First confirm this property is defined in the schema
                if r not in self.result_schemas:
                    if self.cfg.get("lenient"):
                        _LOGGER.warning(
                            f"Result '{r}' not in schema; storing as-is (lenient mode)"
                        )
                        continue  # skip validation, store raw
                    raise ColumnNotFoundError(
                        f"Can't report a result for attribute '{r}'; it is not defined in the output schema."
                    )

                validate_type(
                    value=values[r],
                    schema=self.result_schemas[r],
                    strict_type=strict_type,
                    record_identifier=record_identifier,
                )
        elif not self.cfg.get("lenient"):
            raise SchemaNotFoundError("No schema provided and lenient mode is disabled")
        # else: lenient mode with no schema - store everything as-is

        reported_results = self.backend.report(
            values=values,
            record_identifier=r_id,
            force_overwrite=force_overwrite,
            result_formatter=result_formatter,
            history_enabled=history_enabled,
        )

        return reported_results

    @require_backend
    def select_distinct(
        self,
        columns: str | list[str] | None = None,
    ) -> list[Any]:
        """Retrieves unique results for a list of attributes.

        Args:
            columns (Union[str, List[str]], optional): Columns to include in the result.

        Returns:
            List[Any]: List of distinct results.

        Raises:
            ValueError: If columns is not a list of strings or string.
        """
        if not isinstance(columns, list) and not isinstance(columns, str):
            raise ValueError(
                "Columns must be a list of strings or string, e.g. ['record_identifier', 'number_of_things']"
            )

        result = self.backend.select_distinct(columns=columns)
        return result

    @require_backend
    def select_records(
        self,
        columns: list[str] | None = None,
        filter_conditions: list[dict[str, Any]] | None = None,
        limit: int | None = 1000,
        cursor: int | None = None,
        bool_operator: str | None = "AND",
    ) -> dict[str, Any]:
        """Select records with optional filtering and pagination.

        Args:
            columns (List[str], optional): Columns to include in the result.
            filter_conditions (List[Dict[str, Any]], optional): Filter conditions.
                Format: [{"key": "id", "operator": "eq", "value": 1}].
                Supported operators:
                - eq for ==
                - lt for <
                - ge for >=
                - in for in_
                - like for like
            limit (int, optional): Maximum number of results to retrieve per page. Defaults to 1000.
            cursor (int, optional): Cursor position to begin retrieving records.
            bool_operator (str, optional): Perform filtering with AND or OR logic. Defaults to "AND".

        Returns:
            Dict[str, Any]: Dictionary containing:
                - total_size (int): Total number of records
                - page_size (int): Number of records in current page
                - next_page_token (int): Cursor for next page
                - records (List[Dict]): List of record dictionaries
        """

        return self.backend.select_records(
            columns=columns,
            filter_conditions=filter_conditions,
            limit=limit,
            cursor=cursor,
            bool_operator=bool_operator,
        )

    @require_backend
    def retrieve_one(
        self,
        record_identifier: str = None,
        result_identifier: str | list[str] | None = None,
    ) -> Any | dict[str, Any]:
        """Retrieve a single record.

        Args:
            record_identifier (str, optional): Single record_identifier.
            result_identifier (Union[str, List[str]], optional): Single result_identifier or list of result identifiers.

        Returns:
            Union[Any, Dict[str, Any]]: A mapping with filtered results reported for the record.

        Raises:
            RecordNotFoundError: If the record or results are not found.
            ValueError: If result_identifier is not a str or list[str].
        """
        record_identifier = record_identifier or self.record_identifier

        filter_conditions = [
            {
                "key": "record_identifier",
                "operator": "eq",
                "value": record_identifier,
            },
        ]
        if result_identifier:
            if isinstance(result_identifier, str):
                columns = [result_identifier]
            elif isinstance(result_identifier, list):
                columns = result_identifier
            else:
                raise ValueError("Result identifier must be a str or list[str]")
            result = self.select_records(filter_conditions=filter_conditions, columns=columns)[
                "records"
            ]
            if len(result) > 0:
                if len(columns) > 1:
                    try:
                        return result[0]
                    except IndexError:
                        raise RecordNotFoundError(
                            f"Results '{columns}' for '{record_identifier}' not found"
                        )
                try:
                    return result[0][columns[0]]
                except IndexError:
                    raise RecordNotFoundError(
                        f"Results '{columns}' for '{record_identifier}' not found"
                    )
            else:
                raise RecordNotFoundError(
                    f"Results '{columns}' for '{record_identifier}' not found"
                )
        else:
            try:
                result = self.select_records(filter_conditions=filter_conditions)["records"]
                if len(result) > 0:
                    try:
                        return result[0]
                    except IndexError:
                        raise RecordNotFoundError(f"Record '{record_identifier}' not found")
                else:
                    raise RecordNotFoundError(f"Record '{record_identifier}' not found")
            except IndexError:
                raise RecordNotFoundError(f"Record '{record_identifier}' not found")

    def retrieve_history(
        self,
        record_identifier: str = None,
        result_identifier: str | list[str] | None = None,
    ) -> Any | dict[str, Any]:
        """Retrieve a single record's history.

        Args:
            record_identifier (str, optional): Single record_identifier.
            result_identifier (Union[str, List[str]], optional): Single result_identifier or list of result identifiers.

        Returns:
            Dict[str, Any]: A mapping with filtered historical results.
        """

        record_identifier = record_identifier or self.record_identifier

        if self.file:
            result = self.backend.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": record_identifier,
                    }
                ],
                meta_data_bool=True,
            )["records"][0]

            if "meta" in result and "history" in result["meta"]:
                history = {}
                if isinstance(result_identifier, str) and result_identifier in result:
                    history.update(
                        {result_identifier: result["meta"]["history"][result_identifier]}
                    )
                elif isinstance(result_identifier, list):
                    for r in result_identifier:
                        if r in result["meta"]["history"]:
                            history.update({r: result["meta"]["history"][r]})
                else:
                    history = result["meta"]["history"]
            else:
                _LOGGER.warning(f"No history available for Record: {record_identifier}")
                return {}

        elif self.cfg["pephub_path"]:
            _LOGGER.warning("Retrieving history not supported for PEPHub backend")
            return None
        else:
            if result_identifier:
                history = self.backend.retrieve_history_db(record_identifier, result_identifier)[
                    "history"
                ]
            else:
                history = self.backend.retrieve_history_db(record_identifier)["history"]

            # DB backend returns some extra_keys that we can remove before returning them to the user.
            extra_keys_to_delete = [
                "id",
                "pipestat_created_time",
                "source_record_id",
                "record_identifier",
            ]
            history = {
                key: value for key, value in history.items() if key not in extra_keys_to_delete
            }

        return history

    def retrieve_many(
        self,
        record_identifiers: list[str],
        result_identifier: str | None = None,
    ) -> Any | dict[str, Any]:
        """Retrieve multiple records.

        Args:
            record_identifiers (List[str]): List of record identifiers.
            result_identifier (str, optional): Single result_identifier to filter results.

        Returns:
            Dict[str, Any]: A mapping with filtered results reported for the records.
        """

        filter = {
            "key": "record_identifier",
            "operator": "in",
            "value": record_identifiers,
        }
        if result_identifier:
            result = self.select_records(filter_conditions=[filter], columns=[result_identifier])
        else:
            result = self.select_records(filter_conditions=[filter])

        if len(result["records"]) == 0:
            RecordNotFoundError(f"Records, '{record_identifiers}',  not found")
        else:
            return result

    @require_backend
    def set_status(
        self,
        status_identifier: str,
        record_identifier: str = None,
    ) -> None:
        """Set pipeline run status.

        The status identifier needs to match one of identifiers specified in the status schema.
        A basic, ready to use, status schema is shipped with this package.

        Args:
            status_identifier (str): Status to set, one of statuses defined in the status schema.
            record_identifier (str, optional): Sample_level record identifier to set the pipeline status for.
        """
        r_id = record_identifier or self.record_identifier
        self.backend.set_status(status_identifier, r_id)

    @require_backend
    def link(self, link_dir: str) -> str | None:
        """Create a link structure such that results are organized by type.

        Args:
            link_dir (str): Path to desired symlink output directory.

        Returns:
            Union[str, None]: Path to symlink directory or None.
        """

        self.check_multi_results()
        linked_results_path = self.backend.link(link_dir=link_dir)

        return linked_results_path

    @require_backend
    def summarize(
        self,
        looper_samples: list | None = None,
        amendment: str | None = None,
        portable: bool | None = False,
        output_dir: str | None = None,
        mode: str = "table",
    ) -> str | None:
        """Build a browsable HTML report for reported results.

        Args:
            looper_samples (list, optional): List of looper Samples from PEP.
            amendment (str, optional): Name indicating amendment to use.
            portable (bool, optional): Moves figures and report files to directory for easy sharing. Defaults to False.
            output_dir (str, optional): Overrides output_dir set during pipestatManager creation.
            mode (str, optional): Report mode - "table" (default) or "gallery" for image-centric view.

        Returns:
            Union[str, None]: Path to the generated report or None.

        Raises:
            PipestatSummarizeError: If no results are found at the specified backend.
        """

        if output_dir:
            self.cfg[OUTPUT_DIR] = output_dir

        if self.cfg["pephub_path"]:
            if OUTPUT_DIR not in self.cfg:
                _LOGGER.warning("Output directory is required for pipestat summarize.")
                return None

        self.check_multi_results()

        # Before proceeding check if there are any results at the specified backend
        try:
            current_results = self.select_records()
            if len(current_results["records"]) < 1:
                raise PipestatSummarizeError("No results found at specified backend")
        except Exception as e:
            raise PipestatSummarizeError(f"PipestatSummarizeError due to exception: {e}")

        html_report_builder = HTMLReportBuilder(prj=self, portable=portable, mode=mode)
        report_path = html_report_builder(
            pipeline_name=self.cfg[PIPELINE_NAME],
            amendment=amendment,
            looper_samples=looper_samples,
        )

        if portable is True:
            report_path = zip_report(report_dir_name=os.path.dirname(report_path))

        return report_path

    def check_multi_results(self) -> None:
        # Check to see if the user used a path with "{record-identifier}"
        if self.file:
            if "{record_identifier}" in self.cfg["unresolved_result_path"]:
                # assume there are multiple result files in sub-directories
                self.cfg["multi_result_files"] = True
                results_directory = self.cfg["unresolved_result_path"].split(
                    "{record_identifier}"
                )[0]
                results_directory = mkabs(results_directory, self.cfg["config_path"])
                make_subdirectories(results_directory)
                self.backend.aggregate_multi_results(results_directory)

    @require_backend
    def table(
        self,
        output_dir: str | None = None,
    ) -> list[str]:
        """Generate stats (.tsv) and object (.yaml) files.

        Args:
            output_dir (str, optional): Overrides output_dir set during pipestatManager creation.

        Returns:
            List[str]: List containing output file paths of stats and objects.
        """
        if output_dir:
            self.cfg[OUTPUT_DIR] = output_dir

        self.check_multi_results()
        pipeline_name = self.cfg[PIPELINE_NAME]
        table_path_list = _create_stats_objs_summaries(self, pipeline_name)

        return table_path_list

    # File extensions for lenient mode auto-wrapping
    _IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp"}
    _FILE_EXTENSIONS = {".csv", ".tsv", ".json", ".pdf", ".txt", ".html", ".bed", ".bam"}

    def _infer_and_wrap(self, key: str, value: Any) -> Any:
        """In lenient mode, auto-wrap file paths as file/image objects.

        Args:
            key (str): Result key name (used as title).
            value (Any): The value to potentially wrap.

        Returns:
            Any: Original value or wrapped file/image object.
        """
        if isinstance(value, str):
            ext = os.path.splitext(value)[1].lower()
            if ext in self._IMAGE_EXTENSIONS:
                return {"path": value, "title": key}
            if ext in self._FILE_EXTENSIONS:
                return {"path": value, "title": key}
        return value

    def _get_attr(self, attr: str) -> Any:
        """Safely get the name of the selected attribute of this object.

        Args:
            attr (str): Attribute to select.

        Returns:
            Any: The value of the attribute.
        """
        return self.get(attr)

    @property
    def config_path(self) -> str:
        """Config path.

        Returns None if the config was not provided or if provided as a mapping of the config contents.

        Returns:
            str: Path to the provided config.
        """
        return self.cfg.get("config_path", None)

    @property
    def data(self) -> YAMLConfigManager:
        """Data object.

        Returns:
            yacman.YAMLConfigManager: The object that stores the reported data.
        """
        return self.backend._data

    @property
    def db_url(self) -> str:
        """Database URL, generated based on config credentials.

        Returns:
            str: Database URL.

        Raises:
            PipestatDatabaseError: If the object is not backed by a database.
        """
        return self.cfg[DB_URL]

    @property
    def file(self) -> str:
        """File path that the object is reporting the results into.

        Returns:
            str: File path that the object is reporting the results into.
        """
        return self.cfg[FILE_KEY]

    @property
    def lenient(self) -> bool:
        """Whether lenient mode is enabled.

        Returns:
            bool: True if lenient mode allows reporting without schema validation.
        """
        return self.cfg.get("lenient", False)

    @property
    def highlighted_results(self) -> list[str]:
        """Highlighted results.

        Returns:
            List[str]: A collection of highlighted results.
        """
        return [k for k, v in self.result_schemas.items() if v.get("highlight") is True]

    @property
    def output_dir(self) -> str:
        """Output directory for report and stats generation.

        Returns:
            str: Path to output_dir.
        """
        return self.cfg[OUTPUT_DIR]

    @property
    def pipeline_name(self) -> str:
        """Pipeline name.

        Returns:
            str: Pipeline name.
        """
        return self.cfg[PIPELINE_NAME]

    @property
    def project_name(self) -> str:
        """Project name the object writes the results to.

        Returns:
            str: Project name the object writes the results to.
        """
        return self.cfg[PROJECT_NAME]

    @property
    def pipeline_type(self) -> str:
        """Pipeline type: "sample" or "project".

        Returns:
            str: Pipeline type.
        """
        return self.cfg[PIPELINE_TYPE]

    @property
    def record_identifier(self) -> str:
        """Record identifier.

        Returns:
            str: Record identifier.
        """
        return self.cfg[RECORD_IDENTIFIER]

    @property
    def record_count(self) -> int:
        """Number of records reported.

        Returns:
            int: Number of records reported.
        """
        return self.count_records()

    @property
    def result_schemas(self) -> dict[str, Any]:
        """Result schema mappings for the current pipeline type.

        Returns schemas only for this manager's pipeline_type (sample or project),
        not a merged view of both levels.

        Returns:
            Dict[str, Any]: Schemas that formalize the structure of each result.
                Empty dict if no schema (lenient mode).
        """
        if self.cfg[SCHEMA_KEY] is None:
            return {}
        if self.pipeline_type == "project":
            return self.cfg[SCHEMA_KEY].project_level_data
        return self.cfg[SCHEMA_KEY].sample_level_data

    @property
    def all_result_schemas(self) -> dict[str, dict[str, Any]]:
        """All result schemas organized by level.

        Returns:
            Dict with 'sample' and 'project' keys, each containing that level's schemas.
                Empty dicts if no schema (lenient mode).
        """
        if self.cfg[SCHEMA_KEY] is None:
            return {"sample": {}, "project": {}}
        return {
            "sample": self.cfg[SCHEMA_KEY].sample_level_data,
            "project": self.cfg[SCHEMA_KEY].project_level_data,
        }

    @property
    def schema(self) -> ParsedSchema:
        """Schema mapping.

        Returns:
            ParsedSchema: Schema object that formalizes the results structure.
        """
        return self.cfg["_schema"]

    @property
    def schema_path(self) -> str:
        """Schema path.

        Returns:
            str: Path to the provided schema.
        """
        return self.cfg[SCHEMA_PATH]

    @property
    def status_schema(self) -> dict:
        """Status schema mapping.

        Returns:
            Dict: Schema that formalizes the pipeline status structure.
        """
        return self.cfg[STATUS_SCHEMA_KEY]

    @property
    def status_schema_source(self) -> dict:
        """Status schema source.

        Returns:
            Dict: Source of the schema that formalizes the pipeline status structure.
        """
        return self.cfg[STATUS_SCHEMA_SOURCE_KEY]


class SamplePipestatManager(PipestatManager):
    def __init__(self, **kwargs) -> None:
        PipestatManager.__init__(self, pipeline_type="sample", **kwargs)
        _LOGGER.warning("Initialize PipestatMgrSample")


class ProjectPipestatManager(PipestatManager):
    def __init__(self, **kwargs) -> None:
        PipestatManager.__init__(self, pipeline_type="project", **kwargs)
        _LOGGER.warning("Initialize PipestatMgrProject")


class PipestatBoss(ABC):
    """PipestatBoss simply holds Sample or Project Managers that are child classes of PipestatManager.

    Args:
        pipeline_list (List[str], optional): List that holds pipeline types, e.g. ['sample','project'].
        record_identifier (str, optional): Record identifier to report for. This creates
            a weak bound to the record, which can be overridden in object method calls.
        schema_path (str, optional): Path to the output schema that formalizes the results structure.
        results_file_path (str, optional): YAML file to report into, if file is used as the object back-end.
        database_only (bool, optional): Whether the reported data should not be stored in memory, but only in the database.
        config (Union[str, dict], optional): Path to the configuration file or a mapping with the config file content.
        flag_file_dir (str, optional): Path to directory containing flag files.
        show_db_logs (bool, optional): Toggles showing database logs. Defaults to False.
        pipeline_type (str, optional): "sample" or "project".
        result_formatter (function, optional): Function for formatting result.
        multi_pipelines (bool, optional): Allows for running multiple pipelines for one file backend.
        output_dir (str, optional): Target directory for report generation via summarize and table generation via table.
    """

    def __init__(self, pipeline_list: list | None = None, **kwargs) -> None:
        _LOGGER.warning("Initialize PipestatBoss")
        if len(pipeline_list) > 3:
            _LOGGER.warning(
                "PipestatBoss currently only supports one 'sample' and one 'project' pipeline. Ignoring extra types."
            )
        for i in pipeline_list:
            if i == "sample":
                self.samplemanager = SamplePipestatManager(**kwargs)
            elif i == "project":
                self.projectmanager = ProjectPipestatManager(**kwargs)
            else:
                _LOGGER.warning(f"This pipeline type is not supported. Pipeline supplied: {i}")

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)
