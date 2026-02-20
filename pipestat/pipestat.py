import datetime
import functools
import os
from collections.abc import Callable, Iterator
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
    EXTENDED_DATA,
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
        @functools.wraps(func)
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

    @functools.wraps(func)
    def inner(self, *args, **kwargs) -> Any:
        if not self.backend:
            raise NoBackendSpecifiedError(
                "No backend is configured on this PipestatManager. "
                "Initialize with a results_file_path, config_file, or pephub_path."
            )
        return func(self, *args, **kwargs)

    return inner


class PipestatManager:
    """Manage and store pipeline results with a standardized API.

    PipestatManager provides a unified interface for reporting, retrieving, and
    querying results from any computational pipeline. Results are validated against
    a JSON Schema and stored in either a YAML file or a PostgreSQL database.

    A pipeline author defines outputs in a schema, then uses PipestatManager to
    report results as the pipeline runs. Downstream tools can then reliably
    retrieve those results through the same API.

    Quick start (file backend):
        psm = PipestatManager(
            results_file_path="results.yaml",
            schema_path="output_schema.yaml",
        )
        psm.report(
            record_identifier="sample1",
            values={"alignment_rate": 0.95},
        )
        result = psm.retrieve_one(record_identifier="sample1")

    Dict-style access is supported as shorthand:
        psm["sample1"] = {"alignment_rate": 0.95}
        record = psm["sample1"]
        del psm["sample1"]

    To auto-generate a schema from existing results, use the CLI:
        pipestat infer-schema -f results.yaml -o schema.yaml
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
        validate_results: bool = True,
        additional_properties: bool | None = None,
        force_overwrite: bool = True,
    ) -> None:
        """Initialize the PipestatManager object.

        Create a results manager backed by a YAML file or database.

        Minimal file-backend usage (no schema required):

            psm = PipestatManager(
                results_file_path="results.yaml",
                pipeline_name="my_pipeline",
                validate_results=False,
            )
            psm.report(record_identifier="sample1", values={"my_result": 42})

        With schema validation:

            psm = PipestatManager(
                schema_path="output_schema.yaml",
                results_file_path="results.yaml",
            )

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
            validate_results (bool, optional): Whether to validate results against schema.
                Set to False for schema-optional mode where any key-value pair can be
                reported without a schema. Defaults to True.
            additional_properties (bool | None, optional): Override for allowing results not in schema.
                If None (default), uses schema's additionalProperties setting (defaults to True per JSON Schema).
                If True/False, overrides the schema setting.
            force_overwrite (bool, optional): Default for whether report() should overwrite existing results.
                Can be overridden per-call. Defaults to True.
        """

        if record_identifier is not None and not record_identifier:
            raise ValueError("record_identifier cannot be empty")

        # Initialize the cfg dict as an attribute that holds all configuration data
        self.cfg = {}

        self.cfg["force_overwrite"] = force_overwrite
        self.cfg["validate_results"] = validate_results
        # Store the override value; will resolve from schema after schema is loaded
        self._additional_properties_override = additional_properties

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

        # Resolve additional_properties: use override if provided, else use schema's setting
        if self._additional_properties_override is not None:
            self.cfg["additional_properties"] = self._additional_properties_override
        elif self.cfg.get(SCHEMA_KEY):
            # Will be resolved per-level at runtime, but store a default for backwards compat
            # Default to sample level for initial value (most common case)
            self.cfg["additional_properties"] = self.cfg[SCHEMA_KEY].sample_additional_properties
        else:
            # No schema, default to True
            self.cfg["additional_properties"] = True

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

        # Default project_name for project-level pipelines
        if self.cfg[PIPELINE_TYPE] == "project" and not self.cfg.get(PROJECT_NAME):
            _LOGGER.warning(
                "No project_name provided for project-level pipeline. Defaulting to 'project'."
            )
            self.cfg[PROJECT_NAME] = "project"

        # Auto-default record_identifier for project-level pipelines
        if self.cfg[PIPELINE_TYPE] == "project" and self.cfg[RECORD_IDENTIFIER] is None:
            self.cfg[RECORD_IDENTIFIER] = self.cfg[PROJECT_NAME]

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

        # Note: validate_results=False now works with all backends.
        # DB backend stores additional properties in _extended_data JSONB column.

        if self.cfg[FILE_KEY]:
            self.initialize_filebackend(record_identifier, results_file_path, flag_file_dir)
        elif self.cfg["pephub_path"]:
            self.initialize_pephubbackend(record_identifier, self.cfg["pephub_path"])
        elif CFG_DATABASE_KEY in self.cfg[CONFIG_KEY]:
            self.initialize_dbbackend(record_identifier, show_db_logs)
        else:
            raise NoBackendSpecifiedError()

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
        """Retrieve a record by identifier. Shorthand for retrieve_one(record_identifier=key).

        Example:
            record = psm["sample1"]
            # Returns: {"number_of_things": 42, "name_of_something": "foo", ...}

        Args:
            key: Record identifier.

        Returns:
            Dict of all reported results for the record.

        Raises:
            RecordNotFoundError: If no record with this identifier exists.
        """
        result = self.retrieve_one(record_identifier=key)
        return result

    def __setitem__(self, key: str, value: Any) -> list[str] | bool:
        """Report results for a record. Shorthand for report(record_identifier=key, values=value).

        Example:
            psm["sample1"] = {"alignment_rate": 0.95, "num_reads": 1000000}

        Args:
            key: Record identifier.
            value: Dict mapping result identifiers to their values.

        Returns:
            List of formatted result strings on success, or False if
            results already exist and force_overwrite is disabled.
        """
        result = self.report(record_identifier=key, values=value)
        return result

    def __delitem__(self, key: str) -> bool:
        """Remove an entire record. Shorthand for remove(record_identifier=key).

        Example:
            del psm["sample1"]

        Args:
            key: Record identifier to remove.

        Returns:
            True if the record was removed.

        Raises:
            RecordNotFoundError: If no record with this identifier exists.
        """
        result = self.remove(record_identifier=key)
        return result

    def iter_records(
        self,
        limit: int | None = 1000,
        cursor: int | None = None,
    ) -> Iterator:
        """Iterate over records with optional pagination.

        Args:
            limit: Maximum number of results to retrieve per page. Defaults to 1000.
            cursor: Cursor position to begin retrieving records (DB backend only).

        Returns:
            Iterator over records.
        """
        if self.file:
            return iter(self.select_records(limit=limit)["records"])
        else:
            return iter(self.select_records(limit=limit, cursor=cursor)["records"])

    def __iter__(self) -> Iterator[str]:
        """Iterate over record identifiers.

        Yields record_identifier strings for all records. For paginated access
        to full records, use select_records() directly.
        """
        records = self.select_records()["records"]
        return iter(r.get("record_identifier", "") for r in records)

    def __len__(self) -> int:
        """Return the number of records. Equivalent to record_count property."""
        return self.count_records()

    def __contains__(self, record_identifier: object) -> bool:
        """Check whether a record exists.

        Args:
            record_identifier: The record identifier to check.

        Returns:
            True if the record exists.
        """
        if not isinstance(record_identifier, str):
            return False
        try:
            self.retrieve_one(record_identifier=record_identifier)
            return True
        except RecordNotFoundError:
            return False

    def resolve_results_file_path(self, results_file_path: str | None) -> str | None:
        """Replace {record_identifier} in results_file_path if it exists.

        Args:
            results_file_path (str): YAML file to report into, if file is used as the object back-end.

        Returns:
            str: Resolved file path with record_identifier substituted if applicable.
        """
        # Save for later when assessing if there may be multiple result files
        if results_file_path:
            if not isinstance(results_file_path, str):
                raise TypeError(
                    f"results_file_path must be a string, got {type(results_file_path).__name__}: {results_file_path!r}"
                )
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
            self.cfg.get("validate_results", True),
            self._additional_properties_override,  # Pass override (None if not set)
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
        """Remove status flag files for a record.

        Example:
            # Remove all status flags for a record
            removed = psm.clear_status(record_identifier="sample1")
            # Returns: ["running", "completed"]  (names of flags that were removed)

            # Remove specific flags only
            psm.clear_status(record_identifier="sample1", flag_names=["running"])

        Args:
            record_identifier: Record to remove flags for. If None, uses the
                record_identifier set at init time.
            flag_names: Names of flags to remove. If None, all schema-defined
                flag names will be used.

        Returns:
            list[str | None]: Names of flags that were removed. None entries
                indicate flags that did not exist.
        """

        r_id = self._resolve_record_identifier(record_identifier)
        return self.backend.clear_status(record_identifier=r_id, flag_names=flag_names)

    @require_backend
    def count_records(self) -> int:
        """Count the number of records in the backend.

        Example:
            n = psm.count_records()
            # Returns: 42

        Returns:
            int: Total number of records. Returns 0 if no records exist.
        """
        return self.backend.count_records()

    @require_backend
    def get_status(
        self,
        record_identifier: str = None,
    ) -> str | None:
        """Get the current pipeline status for a record.

        Example:
            status = psm.get_status(record_identifier="sample1")
            # Returns: "running", "completed", "failed", "waiting", "partial", or None

        Args:
            record_identifier: Record to check. If None, uses the
                record_identifier set at init time.

        Returns:
            str: Status identifier (e.g. "running", "completed"), or None
                if no status has been set for the record.
        """

        r_id = self._resolve_record_identifier(record_identifier)
        return self.backend.get_status(record_identifier=r_id)

    @require_backend
    def list_recent_results(
        self,
        limit: int | None = 1000,
        start: str | None = None,
        end: str | None = None,
        time_column: str | None = "modified",
    ) -> dict:
        """List results within a time range, filtered by creation or modification time.

        Example:
            # All results modified in the last day
            result = psm.list_recent_results()

            # Results modified within a specific window
            result = psm.list_recent_results(
                start="2024-06-01 00:00:00",
                end="2024-05-01 00:00:00",
            )

            # Filter by creation time instead
            result = psm.list_recent_results(time_column="created")

        Args:
            limit: Maximum number of results to return. Defaults to 1000.
            start: Upper bound of time range (most recent). Defaults to now.
                Format: "YYYY-MM-DD HH:MM:SS", e.g. "2024-06-15 13:03:04".
            end: Lower bound of time range (oldest). Defaults to 1900-01-01.
                Format: "YYYY-MM-DD HH:MM:SS", e.g. "2024-01-01 00:00:00".
            time_column: Which timestamp to filter on: "modified" (default)
                or "created".

        Returns:
            dict: Same structure as select_records(): contains
                "total_size", "page_size", "next_page_token", and "records" keys.

        Raises:
            InvalidTimeFormatError: If start or end does not match "YYYY-MM-DD HH:MM:SS".
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
            _LOGGER.info(
                "No output schema supplied. Running in schema-optional mode "
                "(validate_results=%s). Results will not be validated against a schema.",
                self.cfg["validate_results"],
            )
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
        level: str | None = None,
    ) -> bool:
        """Remove a result or an entire record.

        Example:
            # Remove a single result
            psm.remove(record_identifier="sample1", result_identifier="number_of_things")

            # Remove an entire record (all results)
            psm.remove(record_identifier="sample1")

        If result_identifier is None, or if removing the last remaining result,
        the entire record is deleted.

        Args:
            record_identifier: Record to modify. If None, uses the
                record_identifier set at init time.
            result_identifier: Specific result key to remove, or None to
                remove the entire record.
            level: Pipeline level ("sample" or "project"). Temporarily overrides
                the pipeline_type for this single call.

        Returns:
            True if the result or record was removed.
        """
        # Temporarily swap level if specified
        orig_type = None
        orig_backend_type = None
        if level:
            orig_type = self.cfg[PIPELINE_TYPE]
            orig_backend_type = self.backend.pipeline_type
            self.cfg[PIPELINE_TYPE] = level
            self.backend.pipeline_type = level

        try:
            r_id = self._resolve_record_identifier(record_identifier)
            return self.backend.remove(
                record_identifier=r_id,
                result_identifier=result_identifier,
            )
        finally:
            if level:
                self.cfg[PIPELINE_TYPE] = orig_type
                self.backend.pipeline_type = orig_backend_type

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
        force_overwrite: bool | None = None,
        result_formatter: Callable | None = None,
        strict_type: bool = True,
        history_enabled: bool = True,
        level: str | None = None,
    ) -> list[str] | bool:
        """Report one or more results for a record.

        Example:
            psm.report(
                record_identifier="sample1",
                values={"alignment_rate": 0.95, "num_reads": 1000000},
            )

        The keys in `values` must match result identifiers defined in your
        output schema (unless additional_properties is enabled). Returns a
        list of formatted result strings on success. Returns False if the
        record already has values for those keys and force_overwrite is
        disabled.

        Args:
            values: Dict mapping result identifiers to their values.
                Scalar example: {"number_of_things": 42}
                File example: {"output_file": {"path": "/path/to/file.csv", "title": "Output"}}
                Image example: {"output_image": {"path": "fig.png", "thumbnail_path": "fig_thumb.png", "title": "Figure"}}
            record_identifier: Unique identifier for the record (e.g. sample name).
                If None, uses the record_identifier set at init time.
            force_overwrite: Whether to overwrite existing results for this record.
                If None (default), uses the manager-level default set at __init__
                (which itself defaults to True).
            result_formatter: Function for formatting each result into a display string.
                Defaults to the formatter set at init time.
            strict_type: If True (default), reported values must match the schema type
                exactly. If False, pipestat attempts type coercion.
            history_enabled: If True (default), previous values are saved in a
                history log before overwriting.
            level: Pipeline level ("sample" or "project"). Temporarily overrides
                the pipeline_type for this single call.

        Returns:
            list[str]: Formatted result strings, one per reported key,
                e.g. ["Reported records for 'sample1' in 'default_pipeline_name' namespace:\n- number_of_things: 42"].
            bool: False if results already exist and force_overwrite is False.

        Raises:
            NotImplementedError: If no record_identifier is provided or resolvable.
            ColumnNotFoundError: If a key in values is not in the schema and
                additional_properties is disabled.
            SchemaNotFoundError: If validate_results is True but no schema was provided.
        """
        # Temporarily swap level if specified
        orig_type = None
        orig_backend_type = None
        if level:
            orig_type = self.cfg[PIPELINE_TYPE]
            orig_backend_type = self.backend.pipeline_type
            self.cfg[PIPELINE_TYPE] = level
            self.backend.pipeline_type = level

        try:
            if force_overwrite is None:
                force_overwrite = self.cfg["force_overwrite"]
            result_formatter = result_formatter or self.cfg[RESULT_FORMATTER]
            values = deepcopy(values)
            r_id = self._resolve_record_identifier(record_identifier)
            if r_id is None:
                raise NotImplementedError("You must supply a record identifier to report results")

            result_identifiers = list(values.keys())

            # Track extra values that are not in schema (for DB backend's _extended_data)
            extra_values = {}

            # Handle validate_results=False: auto-wrap file paths and skip validation
            if not self.cfg.get("validate_results"):
                for r in result_identifiers:
                    values[r] = self._infer_and_wrap(r, values[r])

            # Determine additional_properties setting: use override if set, else from schema
            if self._additional_properties_override is not None:
                allow_additional = self._additional_properties_override
            elif self.cfg[SCHEMA_KEY] is not None:
                allow_additional = self.cfg[SCHEMA_KEY].additional_properties_for_level(
                    self.cfg[PIPELINE_TYPE]
                )
            else:
                allow_additional = True  # No schema, allow everything

            if self.cfg[SCHEMA_KEY] is not None:
                for r in list(
                    result_identifiers
                ):  # Use list() to allow modification during iteration
                    if r in self.result_schemas:
                        # Schema-defined result: validate if validate_results=True
                        if self.cfg.get("validate_results"):
                            validate_type(
                                value=values[r],
                                schema=self.result_schemas[r],
                                strict_type=strict_type,
                                record_identifier=record_identifier,
                            )
                    else:
                        # Not in schema
                        if allow_additional:
                            # Allow it - for DB backend, move to _extended_data
                            _LOGGER.debug(
                                f"Result '{r}' not in schema, storing as additional property"
                            )
                            if not self.file:
                                extra_values[r] = values.pop(r)
                            # For file backend, keep in values as-is
                        elif self.cfg.get("validate_results"):
                            # Strict mode (validate_results=True, additional_properties=False) - error
                            raise ColumnNotFoundError(
                                f"Can't report result '{r}'; not defined in schema. "
                                f"Use additional_properties=True to allow undefined results."
                            )
                        # else: validate_results=False, just store it
            elif self.cfg.get("validate_results"):
                raise SchemaNotFoundError("No schema provided and validation is enabled")
            # else: validate_results=False with no schema - store everything as-is

            # Handle _extended_data for DB backend
            if extra_values and not self.file:
                # Get existing extended data and merge
                existing_extended = self._get_extended_data(r_id) or {}
                existing_extended.update(extra_values)
                values[EXTENDED_DATA] = existing_extended

            reported_results = self.backend.report(
                values=values,
                record_identifier=r_id,
                force_overwrite=force_overwrite,
                result_formatter=result_formatter,
                history_enabled=history_enabled,
            )

            return reported_results
        finally:
            if level:
                self.cfg[PIPELINE_TYPE] = orig_type
                self.backend.pipeline_type = orig_backend_type

    @require_backend
    def select_distinct(
        self,
        columns: str | list[str] | None = None,
    ) -> list[Any]:
        """Retrieve unique values for one or more result attributes.

        Example:
            # Get all distinct values of a column
            distinct = psm.select_distinct(columns="name_of_something")
            # Returns: ["foo", "bar", "baz"]

            # Get distinct combinations of multiple columns
            distinct = psm.select_distinct(columns=["name_of_something", "number_of_things"])

        Args:
            columns: Column name (str) or list of column names to get distinct
                values for.

        Returns:
            list[Any]: List of distinct results.

        Raises:
            ValueError: If columns is not a str or list of strings.
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
        level: str | None = None,
        flatten_extended: bool = True,
    ) -> dict[str, Any]:
        """Select records with optional filtering and pagination.

        Example:
            # All records, default limit of 1000
            result = psm.select_records()

            # Filter by record identifier
            result = psm.select_records(
                filter_conditions=[
                    {"key": "record_identifier", "operator": "eq", "value": "sample1"}
                ],
            )

            # Filter with multiple conditions (AND logic)
            result = psm.select_records(
                columns=["number_of_things", "name_of_something"],
                filter_conditions=[
                    {"key": "number_of_things", "operator": "ge", "value": 10},
                    {"key": "name_of_something", "operator": "like", "value": "%test%"},
                ],
            )

            # Access results
            for record in result["records"]:
                print(record)

        Args:
            columns: Restrict output to these result keys. None returns all columns.
            filter_conditions: List of filter dicts. Each dict has:
                - "key" (str): column name to filter on
                - "operator" (str): one of "eq" (==), "lt" (<), "ge" (>=),
                  "in" (membership), "like" (SQL LIKE pattern)
                - "value": value to compare against
            limit: Maximum records per page. Defaults to 1000.
            cursor: Cursor position for pagination (DB backend only).
            bool_operator: Combine filters with "AND" (default) or "OR".
            level: Pipeline level ("sample" or "project"). Temporarily overrides
                the pipeline_type for this single call.
            flatten_extended: If True (default), merges _extended_data fields
                into each record dict (DB backend only).

        Returns:
            dict with keys:
                - "total_size" (int): total number of matching records
                - "page_size" (int): number of records in this page
                - "next_page_token" (int | None): cursor for next page, or None
                - "records" (list[dict]): list of record dicts
        """
        # Temporarily swap level if specified
        orig_type = None
        orig_backend_type = None
        if level:
            orig_type = self.cfg[PIPELINE_TYPE]
            orig_backend_type = self.backend.pipeline_type
            self.cfg[PIPELINE_TYPE] = level
            self.backend.pipeline_type = level

        try:
            result = self.backend.select_records(
                columns=columns,
                filter_conditions=filter_conditions,
                limit=limit,
                cursor=cursor,
                bool_operator=bool_operator,
            )

            # Flatten extended_data for DB backend
            if flatten_extended and not self.file:
                for record in result["records"]:
                    if EXTENDED_DATA in record:
                        if record[EXTENDED_DATA]:
                            record.update(record[EXTENDED_DATA])
                        del record[EXTENDED_DATA]

            return result
        finally:
            if level:
                self.cfg[PIPELINE_TYPE] = orig_type
                self.backend.pipeline_type = orig_backend_type

    @require_backend
    def retrieve_one(
        self,
        record_identifier: str = None,
        result_identifier: str | list[str] | None = None,
        level: str | None = None,
    ) -> Any | dict[str, Any]:
        """Retrieve results for a single record.

        Return type depends on result_identifier:
        - None: returns the full record as a dict.
        - str: returns that single result's value directly (unwrapped).
        - list[str]: returns a dict with only the requested keys.

        Example:
            # Full record
            psm.retrieve_one(record_identifier="sample1")
            # Returns: {"number_of_things": 42, "name_of_something": "foo"}

            # Single result (unwrapped scalar)
            psm.retrieve_one(record_identifier="sample1", result_identifier="number_of_things")
            # Returns: 42

            # Multiple specific results
            psm.retrieve_one(record_identifier="sample1", result_identifier=["number_of_things", "name_of_something"])
            # Returns: {"number_of_things": 42, "name_of_something": "foo"}

        Args:
            record_identifier: Record to retrieve. If None, uses the
                record_identifier set at init time.
            result_identifier: Single result key (str), list of keys, or None
                for all results.
            level: Pipeline level ("sample" or "project"). Temporarily overrides
                the pipeline_type for this single call.

        Returns:
            Any: The single result value when result_identifier is a str.
            dict[str, Any]: Record dict when result_identifier is None or a list.

        Raises:
            RecordNotFoundError: If the record does not exist.
            ValueError: If result_identifier is not a str, list[str], or None.
        """
        record_identifier = self._resolve_record_identifier(record_identifier)

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
            result = self.select_records(
                filter_conditions=filter_conditions, columns=columns, level=level
            )["records"]
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
                result = self.select_records(filter_conditions=filter_conditions, level=level)[
                    "records"
                ]
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
        """Retrieve the overwrite history for a record's results.

        Example:
            # Get all history for a record
            history = psm.retrieve_history(record_identifier="sample1")
            # Returns: {"number_of_things": [{"value": 10, "date": "2024-01-01"}, ...]}

            # Get history for a specific result
            history = psm.retrieve_history(
                record_identifier="sample1",
                result_identifier="number_of_things",
            )

        History is recorded when results are overwritten with history_enabled=True
        (the default in report()).

        Args:
            record_identifier: Record to get history for. If None, uses the
                record_identifier set at init time.
            result_identifier: Single result key (str), list of keys, or None
                for all results' history.

        Returns:
            dict[str, Any]: Mapping of result identifiers to their historical values.
                Empty dict if no history is available.
        """

        record_identifier = self._resolve_record_identifier(record_identifier)

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
        """Retrieve results for multiple records at once.

        Example:
            result = psm.retrieve_many(
                record_identifiers=["sample1", "sample2", "sample3"],
            )
            for record in result["records"]:
                print(record)

            # Retrieve a specific result across multiple records
            result = psm.retrieve_many(
                record_identifiers=["sample1", "sample2"],
                result_identifier="number_of_things",
            )

        Uses select_records() internally with an "in" filter on record_identifier.

        Args:
            record_identifiers: List of record identifiers to retrieve.
            result_identifier: Single result key to filter results. If None,
                returns all results for each record.

        Returns:
            dict[str, Any]: Same structure as select_records(): contains
                "total_size", "page_size", "next_page_token", and "records" keys.

        Raises:
            RecordNotFoundError: If none of the specified records exist.
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
            raise RecordNotFoundError(f"Records, '{record_identifiers}', not found")
        else:
            return result

    @require_backend
    def set_status(
        self,
        status_identifier: str,
        record_identifier: str = None,
    ) -> None:
        """Set the pipeline run status for a record.

        Example:
            psm.set_status(record_identifier="sample1", status_identifier="running")
            psm.set_status(record_identifier="sample1", status_identifier="completed")

        Built-in status identifiers (from the default status schema):
            - "running": the pipeline is running
            - "completed": the pipeline has completed
            - "failed": the pipeline has failed
            - "waiting": the pipeline is waiting
            - "partial": the pipeline stopped before completion point

        Custom status schemas can define additional identifiers.

        Args:
            status_identifier: Status to set. Must match an identifier in the
                status schema.
            record_identifier: Record to set status for. If None, uses the
                record_identifier set at init time.

        Raises:
            UnrecognizedStatusError: If status_identifier is not in the status schema.
        """
        r_id = self._resolve_record_identifier(record_identifier)
        self.backend.set_status(status_identifier, r_id)

    @require_backend
    def link(self, link_dir: str) -> str | None:
        """Create a symlink directory structure organizing results by type.

        Example:
            linked_path = psm.link(link_dir="/path/to/links")
            # Creates: /path/to/links/<result_type>/<record_id>_<filename>

        Creates symlinks to file and image results, grouped by result type,
        making it easy to browse outputs across records.

        Args:
            link_dir: Path to the desired symlink output directory.

        Returns:
            str | None: Path to the symlink directory, or None if no
                linkable results exist.
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

        Example:
            # Generate a table-based report
            report_path = psm.summarize(output_dir="/path/to/output")
            # Returns: "/path/to/output/default_pipeline_name/stats_summary.html"

            # Generate an image gallery report
            report_path = psm.summarize(output_dir="/path/to/output", mode="gallery")

            # Generate a portable ZIP archive
            report_path = psm.summarize(output_dir="/path/to/output", portable=True)

        Args:
            looper_samples: List of looper Sample objects from a PEP. Used to
                enrich the report with sample metadata.
            amendment: PEP amendment name to use.
            portable: If True, copies figures into the report directory and
                produces a ZIP archive for easy sharing. Defaults to False.
            output_dir: Override the output_dir set during PipestatManager creation.
            mode: Report mode -- "table" (default) for tabular layout, or
                "gallery" for image-centric view.

        Returns:
            str | None: Path to the generated HTML report (or ZIP if portable),
                or None if generation fails.

        Raises:
            PipestatSummarizeError: If no results are found at the backend.
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
        """Generate stats (.tsv) and object (.yaml) summary files.

        Example:
            paths = psm.table(output_dir="/path/to/output")
            # Returns: ["/path/to/output/stats.tsv", "/path/to/output/objects.yaml"]

        Produces a TSV file of scalar results and a YAML file of complex
        (file/image/object) results.

        Args:
            output_dir: Override the output_dir set during PipestatManager creation.

        Returns:
            list[str]: File paths of the generated stats and objects files.
        """
        if output_dir:
            self.cfg[OUTPUT_DIR] = output_dir

        self.check_multi_results()
        pipeline_name = self.cfg[PIPELINE_NAME]
        table_path_list = _create_stats_objs_summaries(self, pipeline_name)

        return table_path_list

    # File extensions for auto-wrapping when validate_results=False
    _IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp"}
    _FILE_EXTENSIONS = {".csv", ".tsv", ".json", ".pdf", ".txt", ".html", ".bed", ".bam"}

    def _infer_and_wrap(self, key: str, value: Any) -> Any:
        """When validate_results=False, auto-wrap file paths as file/image objects.

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

    def _get_extended_data(self, record_identifier: str) -> dict | None:
        """Get existing _extended_data for a record (DB backend only).

        Args:
            record_identifier (str): Record to get extended data for.

        Returns:
            dict | None: Existing extended data or None.
        """
        if self.file:
            return None
        try:
            result = self.backend.select_records(
                columns=[EXTENDED_DATA],
                filter_conditions=[
                    {"key": "record_identifier", "operator": "eq", "value": record_identifier}
                ],
            )
            if result["records"]:
                return result["records"][0].get(EXTENDED_DATA)
        except Exception:
            pass
        return None

    def _resolve_record_identifier(self, record_identifier: str | None) -> str | None:
        """Resolve record_identifier, auto-defaulting for project level.

        Args:
            record_identifier: Explicitly provided record identifier, or None.

        Returns:
            Resolved record identifier string, or None if not resolvable.

        Raises:
            ValueError: If record_identifier is an empty string.
        """
        if record_identifier is not None and not record_identifier:
            raise ValueError("record_identifier cannot be empty")
        if record_identifier is not None:
            return record_identifier
        r_id = self.cfg.get(RECORD_IDENTIFIER)
        if r_id is not None:
            return r_id
        # Auto-default for project level (covers level="project" on sample-level managers)
        if self.cfg.get(PIPELINE_TYPE) == "project":
            return self.cfg.get(PROJECT_NAME) or "project"
        return None

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
    def validate_results(self) -> bool:
        """Whether schema validation is enabled.

        Returns:
            bool: True if results are validated against schema.
        """
        return self.cfg.get("validate_results", True)

    @property
    def additional_properties(self) -> bool:
        """Whether additional properties (not in schema) are allowed for current level.

        Returns the effective value: override if set, else schema's setting for current level.

        Returns:
            bool: True if results not defined in schema are allowed.
        """
        if self._additional_properties_override is not None:
            return self._additional_properties_override
        if self.cfg.get(SCHEMA_KEY):
            return self.cfg[SCHEMA_KEY].additional_properties_for_level(self.cfg[PIPELINE_TYPE])
        return True

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
    def record_identifier(self) -> str | None:
        """Record identifier. Defaults to project_name for project-level pipelines.

        Returns:
            str | None: Record identifier.
        """
        return self._resolve_record_identifier(None)

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
                Empty dict if no schema.
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
                Empty dicts if no schema.
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
    """Convenience wrapper that creates a PipestatManager with pipeline_type="sample".

    Equivalent to PipestatManager(pipeline_type="sample", **kwargs).
    All arguments are forwarded to PipestatManager.__init__.
    """

    def __init__(self, **kwargs) -> None:
        PipestatManager.__init__(self, pipeline_type="sample", **kwargs)


class ProjectPipestatManager(PipestatManager):
    """Convenience wrapper that creates a PipestatManager with pipeline_type="project".

    Equivalent to PipestatManager(pipeline_type="project", **kwargs).
    All arguments are forwarded to PipestatManager.__init__.
    """

    def __init__(self, **kwargs) -> None:
        PipestatManager.__init__(self, pipeline_type="project", **kwargs)


class PipestatDualManager:
    """Holds both a SamplePipestatManager and a ProjectPipestatManager.

    Use this when your pipeline reports results at both the sample and project level
    and you want a single object to manage both. Access the sub-managers via the
    .sample and .project attributes.

        dual = PipestatDualManager(schema_path="schema.yaml", results_file_path="results.yaml")
        dual.sample.report(record_identifier="s1", values={"reads": 1000})
        dual.project.report(values={"total_reads": 5000})

    Args:
        **kwargs: All arguments are forwarded to both sub-managers' __init__.
    """

    def __init__(self, **kwargs) -> None:
        _LOGGER.debug("Initialize PipestatDualManager")
        self.sample = SamplePipestatManager(**kwargs)
        self.project = ProjectPipestatManager(**kwargs)
