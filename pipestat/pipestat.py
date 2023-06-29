from logging import getLogger
from copy import deepcopy

from pipestat.backends.filebackend import FileBackend
from pipestat.backends.dbbackend import DBBackend

from jsonschema import validate

from yacman import YAMLConfigManager, select_config

from .helpers import *
from .parsed_schema import ParsedSchema

_LOGGER = getLogger(PKG_NAME)


def require_backend(func):
    """Decorator to ensure a backend exists for functions that require one"""

    def inner(self, *args, **kwargs):
        if not self.backend:
            raise NoBackendSpecifiedError
        return func(self, *args, **kwargs)

    return inner


class PipestatManager(dict):
    """
    Pipestat standardizes reporting of pipeline results and
    pipeline status management. It formalizes a way for pipeline developers
    and downstream tools developers to communicate -- results produced by a
    pipeline can easily and reliably become an input for downstream analyses.
    A PipestatManager object exposes an API for interacting with the results and
    pipeline status and can be backed by either a YAML-formatted file
    or a database.
    """

    def __init__(
        self,
        sample_name: Optional[str] = None,
        schema_path: Optional[str] = None,
        results_file_path: Optional[str] = None,
        database_only: Optional[bool] = True,
        config_file: Optional[str] = None,
        config_dict: Optional[dict] = None,
        flag_file_dir: Optional[str] = None,
        show_db_logs: bool = False,
        pipeline_type: Optional[str] = None,
        pipeline_name: Optional[str] = DEFAULT_PIPELINE_NAME,
        result_formatter: staticmethod = default_formatter,
        multi_pipelines: bool = False,
    ):
        """
        Initialize the PipestatManager object

        :param str sample_name: record identifier to report for. This
            creates a weak bound to the record, which can be overridden in
            this object method calls
        :param str schema_path: path to the output schema that formalizes
            the results structure
        :param str results_file_path: YAML file to report into, if file is
            used as the object back-end
        :param bool database_only: whether the reported data should not be
            stored in the memory, but only in the database
        :param str | dict config: path to the configuration file or a mapping
            with the config file content
        :param str flag_file_dir: path to directory containing flag files
        :param bool show_db_logs: Defaults to False, toggles showing database logs
        :param str pipeline_type: "sample" or "project"
        :param str result_formatter: function for formatting result
        :param bool multi_pipelines: allows for running multiple pipelines for one file backend
        """

        super(PipestatManager, self).__init__()

        # Load and validate database configuration
        # If results_file_path is truthy, backend is a file
        # Otherwise, backend is a database.
        self._config_path = select_config(config_file, ENV_VARS["config"])
        _LOGGER.info(f"Config: {self._config_path}.")
        self[CONFIG_KEY] = YAMLConfigManager(entries=config_dict, filepath=self._config_path)
        _, cfg_schema = read_yaml_data(CFG_SCHEMA, "config schema")
        validate(self[CONFIG_KEY].exp, cfg_schema)

        self[SCHEMA_PATH] = schema_path if schema_path is not None else None
        self.process_schema(schema_path)

        # self[SCHEMA_PATH] = schema_path

        self[PIPELINE_NAME] = (
            self.schema.pipeline_name if self.schema is not None else pipeline_name
        )

        self[PROJECT_NAME] = self[CONFIG_KEY].priority_get(
            "project_name", env_var=ENV_VARS["project_name"]
        )

        self[SAMPLE_NAME_ID_KEY] = self[CONFIG_KEY].priority_get(
            "sample_name", env_var=ENV_VARS["sample_name"], override=sample_name
        )
        self[DB_ONLY_KEY] = database_only
        self[PIPELINE_TYPE] = self[CONFIG_KEY].priority_get(
            "pipeline_type", default="sample", override=pipeline_type
        )

        self[FILE_KEY] = mk_abs_via_cfg(
            self[CONFIG_KEY].priority_get(
                "results_file_path", env_var=ENV_VARS["results_file"], override=results_file_path
            ),
            self._config_path,
        )

        self[RESULT_FORMATTER] = result_formatter

        self[MULTI_PIPELINE] = multi_pipelines

        if self[FILE_KEY]:  # file backend
            _LOGGER.debug(f"Determined file as backend: {results_file_path}")
            if self[DB_ONLY_KEY]:
                _LOGGER.debug(
                    "Running in database only mode does not make sense with a YAML file as a backend. "
                    "Changing back to using memory."
                )
                self[DB_ONLY_KEY] = False

            flag_file_dir = self[CONFIG_KEY].priority_get(
                "flag_file_dir", override=flag_file_dir, default=os.path.dirname(self.file)
            )
            self[STATUS_FILE_DIR] = mk_abs_via_cfg(flag_file_dir, self.config_path)
            self.backend = FileBackend(
                self[FILE_KEY],
                sample_name,
                self[PIPELINE_NAME],
                self[PIPELINE_TYPE],
                self[SCHEMA_KEY],
                self[STATUS_SCHEMA_KEY],
                self[STATUS_FILE_DIR],
                self[RESULT_FORMATTER],
                self[MULTI_PIPELINE],
            )

        else:  # database backend
            _LOGGER.debug("Determined database as backend")
            if self[SCHEMA_KEY] is None:
                raise SchemaNotFoundError("Output schema must be supplied for DB backends.")
            if CFG_DATABASE_KEY not in self[CONFIG_KEY]:
                raise NoBackendSpecifiedError()
            try:
                dbconf = self[CONFIG_KEY][CFG_DATABASE_KEY]
                self[DB_URL] = construct_db_url(dbconf)
            except KeyError:
                raise PipestatDatabaseError(
                    f"No database section ('{CFG_DATABASE_KEY}') in config"
                )
            self._show_db_logs = show_db_logs

            self.backend = DBBackend(
                sample_name,
                self[PROJECT_NAME],
                self[PIPELINE_NAME],
                show_db_logs,
                self[PIPELINE_TYPE],
                self[SCHEMA_KEY],
                self[STATUS_SCHEMA_KEY],
                self[DB_URL],
                self[STATUS_SCHEMA_SOURCE_KEY],
                self[RESULT_FORMATTER],
            )

    def __str__(self):
        """
        Generate string representation of the object

        :return str: string representation of the object
        """
        res = f"{self.__class__.__name__} ({self[PIPELINE_NAME]})"
        res += "\nBackend: {}".format(
            f"File\n - results: {self.file}\n - status: {self[STATUS_FILE_DIR]}"
            if self.file
            else f"Database (dialect: {self.backend.db_engine_key})"
        )
        if self.file:
            res += f"\nMultiple Pipelines Allowed: {self[MULTI_PIPELINE]}"
        else:
            res += f"\nProject Name: {self[PROJECT_NAME]}"
            res += f"\nDatabase URL: {self[DB_URL]}"
            res += f"\nConfig File: {self.config_path}"

        res += f"\nPipeline name: {self[PIPELINE_NAME]}"
        res += f"\nPipeline type: {self[PIPELINE_TYPE]}"
        if self[SCHEMA_PATH] is not None:
            res += f"\nProject Level Data:"
            for k, v in self[SCHEMA_KEY].project_level_data.items():
                res += f"\n {k} : {v}"
            res += f"\nSample Level Data:"
            for k, v in self[SCHEMA_KEY].sample_level_data.items():
                res += f"\n {k} : {v}"
        res += f"\nStatus Schema key: {self[STATUS_SCHEMA_KEY]}"
        res += f"\nResults formatter: {str(self[RESULT_FORMATTER].__name__)}"
        res += f"\nResults schema source: {self[SCHEMA_PATH]}"
        res += f"\nStatus schema source: {self.status_schema_source}"
        res += f"\nRecords count: {self.record_count}"
        if self[SCHEMA_PATH] is not None:
            high_res = self.highlighted_results
        else:
            high_res = None
        if high_res:
            res += f"\nHighlighted results: {', '.join(high_res)}"
        return res

    @require_backend
    def clear_status(
        self, sample_name: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        """
        Remove status flags

        :param str sample_name: name of the record to remove flags for
        :param Iterable[str] flag_names: Names of flags to remove, optional; if
            unspecified, all schema-defined flag names will be used.
        :return List[str]: Collection of names of flags removed
        """
        r_id = self._record_identifier(sample_name)
        return self.backend.clear_status(sample_name=r_id, flag_names=flag_names)

    @require_backend
    def count_records(self, pipeline_type: Optional[str] = None) -> int:
        """
        Count records
        :param str pipeline_type: "sample" or "project"
        :return int: number of records
        """
        pipeline_type = pipeline_type or self[PIPELINE_TYPE]
        return self.backend.count_records(pipeline_type)

    @require_backend
    def get_status(
        self, sample_name: str = None, pipeline_type: Optional[str] = None
    ) -> Optional[str]:
        """
        Get the current pipeline status
        :param str sample_name: name of the record
        :param str pipeline_type: "sample" or "project"
        :return str: status identifier, like 'running'
        """
        r_id = self._record_identifier(sample_name)
        pipeline_type = pipeline_type or self[PIPELINE_TYPE]
        return self.backend.get_status(sample_name=r_id, pipeline_type=pipeline_type)

    def process_schema(self, schema_path):
        # Load pipestat schema in two parts: 1) main and 2) status
        self._schema_path = self[CONFIG_KEY].priority_get(
            "schema_path", env_var=ENV_VARS["schema"], override=schema_path
        )

        if self._schema_path is None:
            # print('DEBUG')
            _LOGGER.warning("No schema supplied.")
            self[SCHEMA_KEY] = None
            self[STATUS_SCHEMA_KEY] = None
            self[STATUS_SCHEMA_SOURCE_KEY] = None
            # return None
            # raise SchemaNotFoundError("PipestatManager creation failed; no schema")
        else:
            # Main schema
            schema_to_read = mk_abs_via_cfg(self._schema_path, self.config_path)
            parsed_schema = ParsedSchema(schema_to_read)
            self[SCHEMA_KEY] = parsed_schema

            # Status schema
            self[STATUS_SCHEMA_KEY] = parsed_schema.status_data
            if not self[STATUS_SCHEMA_KEY]:
                self[STATUS_SCHEMA_SOURCE_KEY], self[STATUS_SCHEMA_KEY] = read_yaml_data(
                    path=STATUS_SCHEMA, what="default status schema"
                )
            else:
                self[STATUS_SCHEMA_SOURCE_KEY] = schema_to_read

    def validate_schema(self) -> None:
        """
        Check schema for any possible issues

        :raises SchemaError: if any schema format issue is detected
        """

        def _recursively_replace_custom_types(s: dict) -> Dict:
            """
            Replace the custom types in pipestat schema with canonical types

            :param dict s: schema to replace types in
            :return dict: schema with types replaced
            """
            for k, v in s.items():
                missing_req_keys = [
                    req for req in [SCHEMA_TYPE_KEY, SCHEMA_DESC_KEY] if req not in v
                ]
                if missing_req_keys:
                    raise SchemaError(
                        f"Result '{k}' is missing required key(s): {', '.join(missing_req_keys)}"
                    )
                curr_type_name = v[SCHEMA_TYPE_KEY]
                if curr_type_name == "object" and SCHEMA_PROP_KEY in s[k]:
                    _recursively_replace_custom_types(s[k][SCHEMA_PROP_KEY])
                try:
                    curr_type_spec = CANONICAL_TYPES[curr_type_name]
                except KeyError:
                    continue
                s.setdefault(k, {})
                s[k].setdefault(SCHEMA_PROP_KEY, {})
                s[k][SCHEMA_PROP_KEY].update(curr_type_spec[SCHEMA_PROP_KEY])
                s[k].setdefault("required", []).extend(curr_type_spec["required"])
                s[k][SCHEMA_TYPE_KEY] = curr_type_spec[SCHEMA_TYPE_KEY]
            return s

    @require_backend
    def remove(
        self,
        sample_name: str = None,
        result_identifier: str = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str sample_name: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :param str pipeline_type: "sample" or "project"
        :return bool: whether the result has been removed
        """

        pipeline_type = pipeline_type or self[PIPELINE_TYPE]

        r_id = self._record_identifier(sample_name)
        return self.backend.remove(
            sample_name=r_id, result_identifier=result_identifier, pipeline_type=pipeline_type
        )

    @require_backend
    def report(
        self,
        values: Dict[str, Any],
        sample_name: str = None,
        force_overwrite: bool = False,
        strict_type: bool = True,
        return_id: bool = False,
        pipeline_type: Optional[str] = None,
        result_formatter: staticmethod = default_formatter,
    ) -> Union[List[str], bool]:
        """
        Report a result.

        :param Dict[str, any] values: dictionary of result-value pairs
        :param str sample_name: unique identifier of the record, value
            in 'sample_name' column to look for to determine if the record
            already exists
        :param bool force_overwrite: whether to overwrite the existing record
        :param bool strict_type: whether the type of the reported values should
            remain as is. Pipestat would attempt to convert to the
            schema-defined one otherwise
        :param str pipeline_type: whether what's being reported pertains to project-level,
            rather than sample-level, attribute(s)
        :param str result_formatter: function for formatting result
        :return str reported_results: return list of formatted string
        """

        pipeline_type = pipeline_type or self[PIPELINE_TYPE]
        result_formatter = result_formatter or self[RESULT_FORMATTER]
        values = deepcopy(values)

        sample_name = self._record_identifier(sample_name)
        if return_id and self[FILE_KEY] is not None:
            raise NotImplementedError(
                "There is no way to return the updated object ID while using "
                "results file as the object backend"
            )
        result_identifiers = list(values.keys())
        if self.schema is not None:
            for r in result_identifiers:
                validate_type(
                    value=values[r], schema=self.result_schemas[r], strict_type=strict_type
                )

        reported_results = self.backend.report(
            values, sample_name, pipeline_type, force_overwrite, result_formatter
        )

        return reported_results

    @require_backend
    def retrieve(
        self,
        sample_name: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> Union[Any, Dict[str, Any]]:
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str sample_name: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :param str pipeline_type: "sample" or "project"
        :return any | Dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """

        pipeline_type = pipeline_type or self[PIPELINE_TYPE]
        sample_name = sample_name or self.sample_name
        return self.backend.retrieve(sample_name, result_identifier, pipeline_type)

    @require_backend
    def set_status(
        self,
        status_identifier: str,
        sample_name: str = None,
        pipeline_type: Optional[str] = None,
    ) -> None:
        """
        Set pipeline run status.

        The status identifier needs to match one of identifiers specified in
        the status schema. A basic, ready to use, status schema is shipped with
        this package.

        :param str status_identifier: status to set, one of statuses defined
            in the status schema
        :param str sample_name: record identifier to set the
            pipeline status for
        :param str pipeline_type: "sample" or "project"
        """
        pipeline_type = pipeline_type or self[PIPELINE_TYPE]
        self.backend.set_status(status_identifier, sample_name, pipeline_type)

    def _get_attr(self, attr: str) -> Any:
        """
        Safely get the name of the selected attribute of this object

        :param str attr: attr to select
        :return:
        """
        return self.get(attr)

    def _record_identifier(self, override: str = None) -> str:
        """
        Get record identifier from the outer source or stored with this object

        :param str override: return this value
        :return str: self.sample_name
        """
        if override is not None:
            return override
        if self.sample_name is not None:
            return self.sample_name
        raise PipestatError(
            f"You must provide the record identifier you want to perform "
            f"the action on. Either in the {self.__class__.__name__} "
            f"constructor or as an argument to the method."
        )

    @property
    def config_path(self) -> str:
        """
        Config path. None if the config was not provided or if provided
        as a mapping of the config contents

        :return str: path to the provided config
        """
        return getattr(self, "_config_path", None)

    @property
    def data(self) -> YAMLConfigManager:
        """
        Data object

        :return yacman.YAMLConfigManager: the object that stores the reported data
        """
        return self.get(DATA_KEY)

    @property
    def db_url(self) -> str:
        """
        Database URL, generated based on config credentials

        :return str: database URL
        :raise PipestatDatabaseError: if the object is not backed by a database
        """
        return self.get(DB_URL)

    @property
    def file(self) -> str:
        """
        File path that the object is reporting the results into

        :return str: file path that the object is reporting the results into
        """
        return self.get(FILE_KEY)

    @property
    def highlighted_results(self) -> List[str]:
        """
        Highlighted results

        :return List[str]: a collection of highlighted results
        """
        return [k for k, v in self.result_schemas.items() if v.get("highlight") is True]

    @property
    def pipeline_name(self) -> str:
        """
        Pipeline name

        :return str: Pipeline name
        """
        return self.get(PIPELINE_NAME)

    @property
    def project_name(self) -> str:
        """
        Project name the object writes the results to

        :return str: project name the object writes the results to
        """
        return self.get(PROJECT_NAME)

    @property
    def pipeline_type(self) -> str:
        """
        Pipeline type: "sample" or "project"

        :return str: pipeline type
        """
        return self.get(PIPELINE_TYPE)

    @property
    def record_count(self) -> int:
        """
        Number of records reported

        :return int: number of records reported
        """
        return self.count_records()

    @property
    def sample_name(self) -> str:
        """
        Unique identifier of the record

        :return str: unique identifier of the record
        """
        return self.get(SAMPLE_NAME_ID_KEY)

    @property
    def result_schemas(self) -> Dict[str, Any]:
        """
        Result schema mappings

        :return dict: schemas that formalize the structure of each result
            in a canonical jsonschema way
        """
        return {**self.schema.project_level_data, **self.schema.sample_level_data}

    @property
    def schema(self) -> Dict:
        """
        Schema mapping

        :return dict: schema that formalizes the results structure
        """
        return self.get(SCHEMA_KEY)

    @property
    def schema_path(self) -> str:
        """
        Schema path

        :return str: path to the provided schema
        """
        return self.get(SCHEMA_PATH)

    @property
    def status_schema(self) -> Dict:
        """
        Status schema mapping

        :return dict: schema that formalizes the pipeline status structure
        """
        return self.get(STATUS_SCHEMA_KEY)

    @property
    def status_schema_source(self) -> Dict:
        """
        Status schema source

        :return dict: source of the schema that formalizes
            the pipeline status structure
        """
        return self.get(STATUS_SCHEMA_SOURCE_KEY)
