import csv
import datetime
import time
from logging import getLogger
from copy import deepcopy
from typing import List

from abc import ABC
from collections.abc import MutableMapping

from pipestat.backends.filebackend import FileBackend
from pipestat.backends.dbbackend import DBBackend

from jsonschema import validate

from yacman import YAMLConfigManager, select_config

from .helpers import *
from .parsed_schema import ParsedSchema

from .reports import HTMLReportBuilder, get_file_for_table, _create_stats_objs_summaries

_LOGGER = getLogger(PKG_NAME)


def require_backend(func):
    """Decorator to ensure a backend exists for functions that require one"""

    def inner(self, *args, **kwargs):
        if not self.backend:
            raise NoBackendSpecifiedError
        return func(self, *args, **kwargs)

    return inner


class PipestatManager(MutableMapping):
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
        project_name: Optional[str] = None,
        record_identifier: Optional[str] = None,
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
        output_dir: Optional[str] = None,
    ):
        """
        Initialize the PipestatManager object

        :param str record_identifier: record identifier to report for. This
            creates a weak bound to the record, which can be overridden in
            this object method calls
        :param str schema_path: path to the output schema that formalizes
            the results structure
        :param str results_file_path: YAML file to report into, if file is
            used as the object back-end
        :param bool database_only: whether the reported data should not be
            stored in the memory, but only in the database
        :param str config_file: path to the configuration file
        :param dict config_dict:  a mapping with the config file content
        :param str flag_file_dir: path to directory containing flag files
        :param bool show_db_logs: Defaults to False, toggles showing database logs
        :param str pipeline_type: "sample" or "project"
        :param str pipeline_name: name of the current pipeline, defaults to
        :param str result_formatter: function for formatting result
        :param bool multi_pipelines: allows for running multiple pipelines for one file backend
        :param str output_dir: target directory for report generation via summarize and table generation via table.
        """

        super(PipestatManager, self).__init__()
        self.store = dict()

        # Load and validate database configuration
        # If results_file_path is truthy, backend is a file
        # Otherwise, backend is a database.
        self._config_path = select_config(config_file, ENV_VARS["config"])
        _LOGGER.info(f"Config: {self._config_path}.")
        self[CONFIG_KEY] = YAMLConfigManager(entries=config_dict, filepath=self._config_path)
        _, cfg_schema = read_yaml_data(CFG_SCHEMA, "config schema")
        validate(self[CONFIG_KEY].exp, cfg_schema)

        self[SCHEMA_PATH] = self[CONFIG_KEY].priority_get(
            "schema_path", env_var=ENV_VARS["schema"], override=schema_path
        )  # schema_path if schema_path is not None else None
        self.process_schema(schema_path)

        self[RECORD_IDENTIFIER] = record_identifier

        self[PIPELINE_NAME] = (
            self.schema.pipeline_name if self.schema is not None else pipeline_name
        )

        self[PROJECT_NAME] = self[CONFIG_KEY].priority_get(
            "project_name", env_var=ENV_VARS["project_name"], override=project_name
        )

        self[SAMPLE_NAME_ID_KEY] = self[CONFIG_KEY].priority_get(
            "record_identifier", env_var=ENV_VARS["sample_name"], override=record_identifier
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

        self[OUTPUT_DIR] = self[CONFIG_KEY].priority_get("output_dir", override=output_dir)

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
            self[STATUS_FILE_DIR] = mk_abs_via_cfg(flag_file_dir, self.config_path or self.file)
            self.backend = FileBackend(
                self[FILE_KEY],
                record_identifier,
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
                dbconf = self[CONFIG_KEY].exp[
                    CFG_DATABASE_KEY
                ]  # the .exp expands the paths before url construction
                self[DB_URL] = construct_db_url(dbconf)
            except KeyError:
                raise PipestatDatabaseError(
                    f"No database section ('{CFG_DATABASE_KEY}') in config"
                )
            self._show_db_logs = show_db_logs

            self.backend = DBBackend(
                record_identifier,
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

    def __getitem__(self, key):
        return self.store[self._keytransform(key)]

    def __setitem__(self, key, value):
        self.store[self._keytransform(key)] = value

    def __delitem__(self, key):
        del self.store[self._keytransform(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def _keytransform(self, key):
        return key

    @require_backend
    def clear_status(
        self,
        record_identifier: str = None,
        flag_names: List[str] = None,
    ) -> List[Union[str, None]]:
        """
        Remove status flags

        :param str record_identifier: name of the sample_level record to remove flags for
        :param Iterable[str] flag_names: Names of flags to remove, optional; if
            unspecified, all schema-defined flag names will be used.
        :return List[Union[str,None]]: Collection of names of flags removed
        """

        r_id = record_identifier or self.record_identifier
        return self.backend.clear_status(record_identifier=r_id, flag_names=flag_names)

    @require_backend
    def count_records(self) -> int:
        """
        Count records
        :return int: number of records
        """
        return self.backend.count_records()

    @require_backend
    def get_records(
        self,
        limit: Optional[int] = 1000,
        offset: Optional[int] = 0,
    ) -> Optional[dict]:
        """
        Returns list of records
        :param int limit: limit number of records to this amount
        :param int offset: offset records by this amount
        :return dict: dictionary of records
        """

        return self.backend.get_records(limit=limit, offset=offset)

    @require_backend
    def get_status(
        self,
        record_identifier: str = None,
    ) -> Optional[str]:
        """
        Get the current pipeline status
        :param str record_identifier: name of the sample_level record
        :return str: status identifier, e.g. 'running'
        """

        r_id = record_identifier or self.record_identifier
        return self.backend.get_status(record_identifier=r_id)

    @require_backend
    def list_recent_results(
        self,
        limit: Optional[int] = 1000,
        start: Optional[datetime.datetime] = datetime.datetime.now(),
        end: Optional[datetime.datetime] = None,
        type: Optional[str] = "modified",
    ) -> List[str]:
        """
        :param int  limit: limit number of results returned
        :param datetime.datetime start: most recent result  to filter on, e.g. 2023-10-16 13:03:04.680400
        :param datetime.datetime end: oldest result to filter on, e.g. 1970-10-16 13:03:04.680400
        :param type: created or modified
        :return list[str]: status identifier, e.g. 'running'
        """
        # date_format = '%Y-%m-%d %H:%M:%S'
        # # start = time.strptime(start, date_format)
        # # if end is None:
        # #     end = time.strptime("1900-01-01 00:00:00", date_format)
        #
        # results = self.backend.list_recent_results(limit=limit, start=start,end=end, type=type)

        pass

    def process_schema(self, schema_path):
        # Load pipestat schema in two parts: 1) main and 2) status
        self._schema_path = self[CONFIG_KEY].priority_get(
            "schema_path", env_var=ENV_VARS["schema"], override=schema_path
        )

        if self._schema_path is None:
            _LOGGER.warning("No schema supplied.")
            self[SCHEMA_KEY] = None
            self[STATUS_SCHEMA_KEY] = None
            self[STATUS_SCHEMA_SOURCE_KEY] = None
            # return None
            # raise SchemaNotFoundError("PipestatManager creation failed; no schema")
        else:
            # Main schema
            schema_to_read = mk_abs_via_cfg(self._schema_path, self.config_path)
            self._schema_path = schema_to_read
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
        record_identifier: str = None,
        result_identifier: str = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str record_identifier: name of the sample_level record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :return bool: whether the result has been removed
        """

        r_id = record_identifier or self.record_identifier
        return self.backend.remove(
            record_identifier=r_id,
            result_identifier=result_identifier,
        )

    @require_backend
    def report(
        self,
        values: Dict[str, Any],
        record_identifier: Optional[str] = None,
        force_overwrite: bool = False,
        result_formatter: Optional[staticmethod] = None,
        strict_type: bool = True,
    ) -> Union[List[str], bool]:
        """
        Report a result.

        :param Dict[str, any] values: dictionary of result-value pairs
        :param str record_identifier: unique identifier of the record, value
            in 'record_identifier' column to look for to determine if the record
            already exists
        :param bool force_overwrite: whether to overwrite the existing record
        :param str result_formatter: function for formatting result
        :param bool strict_type: whether the type of the reported values should
            remain as is. Pipestat would attempt to convert to the
            schema-defined one otherwise
        :return str reported_results: return list of formatted string
        """

        result_formatter = result_formatter or self[RESULT_FORMATTER]
        values = deepcopy(values)
        r_id = record_identifier or self.record_identifier
        if r_id is None:
            raise NotImplementedError("You must supply a record identifier to report results")

        result_identifiers = list(values.keys())
        if self.schema is not None:
            for r in result_identifiers:
                validate_type(
                    value=values[r], schema=self.result_schemas[r], strict_type=strict_type
                )

        reported_results = self.backend.report(
            values=values,
            record_identifier=r_id,
            force_overwrite=force_overwrite,
            result_formatter=result_formatter,
        )

        return reported_results

    @require_backend
    def retrieve_distinct(
        self,
        columns: Optional[List[str]] = None,
    ) -> List[Any]:
        """
        Retrieves unique results for a list of attributes.

        :param List[str] columns: columns to include in the result
        :return list[any] result: this is a list of distinct results
        """
        if self.file:
            # Not implemented yet
            result = self.backend.retrieve_distinct()
        else:
            result = self.backend.select_distinct(columns=columns)
        return result

    @require_backend
    def retrieve(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
    ) -> Union[Any, Dict[str, Any]]:
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: name of the sample_level record
        :param str result_identifier: name of the result to be retrieved
        :return any | Dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """

        r_id = record_identifier or self.record_identifier
        return self.backend.retrieve(r_id, result_identifier)

    @require_backend
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
        :param str record_identifier: sample_level record identifier to set the
            pipeline status for
        """
        r_id = record_identifier or self.record_identifier
        self.backend.set_status(status_identifier, r_id)

    @require_backend
    def link(self, link_dir) -> str:
        """
        This function creates a link structure such that results are organized by type.
        :param str link_dir: path to desired symlink output directory
        :return str linked_results_path: path to symlink directory
        """

        linked_results_path = self.backend.link(link_dir=link_dir)

        return linked_results_path

    @require_backend
    def summarize(
        self,
        amendment: Optional[str] = None,
    ) -> None:
        """
        Builds a browsable html report for reported results.
        :param Iterable[str] amendment: name indicating amendment to use, optional
        :return str: report_path

        """

        html_report_builder = HTMLReportBuilder(prj=self)
        report_path = html_report_builder(pipeline_name=self.pipeline_name, amendment=amendment)
        return report_path

    @require_backend
    def table(
        self,
    ) -> List[str]:
        """
        Generates stats (.tsv) and object (.yaml) files.
        :return list[str] table_path_list: list containing output file paths of stats and objects

        """

        pipeline_name = self.pipeline_name
        table_path_list = _create_stats_objs_summaries(self, pipeline_name)

        return table_path_list

    def _get_attr(self, attr: str) -> Any:
        """
        Safely get the name of the selected attribute of this object

        :param str attr: attr to select
        :return:
        """
        return self.get(attr)

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
    def output_dir(self) -> str:
        """
        Output directory for report and stats generation

        :return str: path to output_dir
        """
        return self.get(OUTPUT_DIR)

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
    def record_identifier(self) -> str:
        """
        Pipeline type: "sample" or "project"

        :return str: pipeline type
        """
        return self.get(RECORD_IDENTIFIER)

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
    def schema(self) -> ParsedSchema:
        """
        Schema mapping

        :return ParsedSchema: schema object that formalizes the results structure
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


class SamplePipestatManager(PipestatManager):
    def __init__(self, **kwargs):
        PipestatManager.__init__(self, pipeline_type="sample", **kwargs)
        _LOGGER.warning("Initialize PipestatMgrSample")


class ProjectPipestatManager(PipestatManager):
    def __init__(self, **kwargs):
        PipestatManager.__init__(self, pipeline_type="project", **kwargs)
        _LOGGER.warning("Initialize PipestatMgrProject")


class PipestatBoss(ABC):
    """
    PipestatBoss simply holds Sample or Project Managers that are child classes of PipestatManager.
        :param list[str] pipeline_list: list that holds pipeline types, e.g. ['sample','project']
        :param str record_identifier: record identifier to report for. This
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
        :param str output_dir: target directory for report generation via summarize and table generation via table.
    """

    def __init__(self, pipeline_list: Optional[list] = None, **kwargs):
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

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)
