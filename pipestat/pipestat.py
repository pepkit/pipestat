from contextlib import contextmanager
from glob import glob
from logging import getLogger
import os
from copy import deepcopy
from typing import *

import sqlalchemy.orm

from sqlmodel import Session, SQLModel, create_engine, select as sql_select

from jsonschema import validate

from ubiquerg import create_lock, remove_lock, expandpath
from yacman import YAMLConfigManager, select_config

from .const import *
from .exceptions import *
from .helpers import *
from .parsed_schema import ParsedSchema

_LOGGER = getLogger(PKG_NAME)

from .backend import FileBackend, DBBackend


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
        record_identifier: Optional[str] = None,
        schema_path: Optional[str] = None,
        results_file_path: Optional[str] = None,
        database_only: Optional[bool] = True,
        config_file: Optional[str] = None,
        config_dict: Optional[dict] = None,
        flag_file_dir: Optional[str] = None,
        show_db_logs: bool = False,
        pipeline_type: Optional[str] = None,
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
        :param str | dict config: path to the configuration file or a mapping
            with the config file content
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

        self.process_schema(schema_path)

        self[RECORD_ID_KEY] = self[CONFIG_KEY].priority_get(
            "record_identifier", env_var=ENV_VARS["record_identifier"], override=record_identifier
        )
        self[DB_ONLY_KEY] = database_only
        self.pipeline_type = self[CONFIG_KEY].priority_get(
            "pipeline_type", default="sample", override=pipeline_type
        )

        self[FILE_KEY] = mk_abs_via_cfg(
            self[CONFIG_KEY].priority_get(
                "results_file_path", env_var=ENV_VARS["results_file"], override=results_file_path
            ),
            self._config_path,
        )

        if self[FILE_KEY]:  # file backend
            _LOGGER.debug(f"Determined file as backend: {results_file_path}")
            if self[DB_ONLY_KEY]:
                _LOGGER.debug(
                    "Running in database only mode does not make sense with a YAML file as a backend. "
                    "Changing back to using memory."
                )
                self[DB_ONLY_KEY] = False

            if not os.path.exists(self.file):
                _LOGGER.debug(f"Results file doesn't yet exist. Initializing: {self.file}")
                self._init_results_file()
            else:
                _LOGGER.debug(f"Loading results file: {self.file}")
                self._load_results_file()

            flag_file_dir = self[CONFIG_KEY].priority_get(
                "flag_file_dir", override=flag_file_dir, default=os.path.dirname(self.file)
            )
            self[STATUS_FILE_DIR] = mk_abs_via_cfg(flag_file_dir, self.config_path)
            self.backend = FileBackend(
                self.file,
                record_identifier,
                schema_path,
                self.namespace,
                self.pipeline_type,
                self[SCHEMA_KEY],
                self[STATUS_SCHEMA_KEY],
                self[STATUS_FILE_DIR],
            )
        else:  # database backend
            _LOGGER.debug("Determined database as backend")
            if CFG_DATABASE_KEY not in self[CONFIG_KEY]:
                raise NoBackendSpecifiedError()
            try:
                dbconf = self[CONFIG_KEY][CFG_DATABASE_KEY]
                self._db_url = construct_db_url(dbconf)
            except KeyError:
                raise PipestatDatabaseError(
                    f"No database section ('{CFG_DATABASE_KEY}') in config"
                )
            self[DATA_KEY] = YAMLConfigManager()
            self._show_db_logs = show_db_logs
            self[DB_ORMS_KEY] = self._create_orms()
            SQLModel.metadata.create_all(self._engine)

            self.backend = DBBackend(
                record_identifier,
                schema_path,
                self.namespace,
                config_file,
                config_dict,
                show_db_logs,
                pipeline_type,
                self[SCHEMA_KEY],
                self[STATUS_SCHEMA_KEY],
                self[DB_ORMS_KEY],
                self._engine
                # self.session,
            )
            print("debug")

    def __str__(self):
        """
        Generate string representation of the object

        :return str: string representation of the object
        """
        res = f"{self.__class__.__name__} ({self.namespace})"
        res += "\nBackend: {}".format(
            f"File\n - results: {self.file}\n - status: {self[STATUS_FILE_DIR]}"
            if self.file
            else f"Database (dialect: {self[DB_ENGINE_KEY].dialect.name})"
        )
        res += f"\nResults schema source: {self.schema_path}"
        res += f"\nStatus schema source: {self.status_schema_source}"
        res += f"\nRecords count: {self.record_count}"
        high_res = self.highlighted_results
        if high_res:
            res += f"\nHighlighted results: {', '.join(high_res)}"
        return res

    def process_schema(self, schema_path):
        # Load pipestat schema in two parts: 1) main and 2) status
        self._schema_path = self[CONFIG_KEY].priority_get(
            "schema_path", env_var=ENV_VARS["schema"], override=schema_path
        )

        if self._schema_path is None:
            raise SchemaNotFoundError("PipestatManager creation failed; no schema")

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

    @property
    def record_count(self) -> int:
        """
        Number of records reported

        :return int: number of records reported
        """
        # records = self.backend.count_record()
        # return records
        return len(self.data[self.namespace]) if self.file else self._count_rows(self.namespace)
        # return len(self.backend.DATA_KEY[self.namespace]) if self.file else self._count_rows(self.namespace)

    @property
    def highlighted_results(self) -> List[str]:
        """
        Highlighted results

        :return List[str]: a collection of highlighted results
        """
        return [k for k, v in self.result_schemas.items() if v.get("highlight") is True]

    @property
    def db_column_kwargs_by_result(self) -> Dict[str, Any]:
        """
        Database column key word arguments for every result,
        sourced from the results schema in the `db_column` section

        :return Dict[str, Any]: key word arguments for every result
        """
        return {
            result_id: self.schema[result_id][DB_COLUMN_KEY]
            for result_id in (self.schema or {}).keys()
            if DB_COLUMN_KEY in self.schema[result_id]
        }

    @property
    def namespace(self) -> str:
        """
        Namespace the object writes the results to

        :return str: namespace the object writes the results to
        """
        return self.schema.pipeline_id

    @property
    def record_identifier(self) -> str:
        """
        Unique identifier of the record

        :return str: unique identifier of the record
        """
        return self.get(RECORD_ID_KEY)

    @property
    def schema(self) -> Dict:
        """
        Schema mapping

        :return dict: schema that formalizes the results structure
        """
        return self.get(SCHEMA_KEY)

    @property
    def status_schema(self) -> Dict:
        """
        Status schema mapping

        :return dict: schema that formalizes the pipeline status structure
        """
        return self[STATUS_SCHEMA_KEY]

    @property
    def status_schema_source(self) -> Dict:
        """
        Status schema source

        :return dict: source of the schema that formalizes
            the pipeline status structure
        """
        return self.get(STATUS_SCHEMA_SOURCE_KEY)

    @property
    def schema_path(self) -> str:
        """
        Schema path

        :return str: path to the provided schema
        """
        return self._schema_path

    @property
    def config_path(self) -> str:
        """
        Config path. None if the config was not provided or if provided
        as a mapping of the config contents

        :return str: path to the provided config
        """
        return getattr(self, "_config_path", None)

    @property
    def result_schemas(self) -> Dict[str, Any]:
        """
        Result schema mappings

        :return dict: schemas that formalize the structure of each result
            in a canonical jsonschema way
        """
        return {**self.schema.project_level_data, **self.schema.sample_level_data}

    @property
    def file(self) -> str:
        """
        File path that the object is reporting the results into

        :return str: file path that the object is reporting the results into
        """
        return self.get(FILE_KEY)

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
        return self._db_url

    @property
    @contextmanager
    def session(self):
        """
        Provide a transactional scope around a series of query
        operations.
        """
        session = Session(self._engine)
        _LOGGER.debug("Created session")
        try:
            yield session
        except:
            _LOGGER.info("session.rollback")
            session.rollback()
            raise
        finally:
            _LOGGER.info("session.close")
            session.close()
        _LOGGER.debug("Ending session")

    def _get_flag_file(self, record_identifier: str = None) -> Union[str, List[str], None]:
        """
        Get path to the status flag file for the specified record

        :param str record_identifier: unique record identifier
        :return str | list[str] | None: path to the status flag file
        """

        r_id = self._strict_record_id(record_identifier)
        if self.file is None:
            return
        if self.file is not None:
            regex = os.path.join(self[STATUS_FILE_DIR], f"{self.namespace}_{r_id}_*.flag")
            file_list = glob(regex)
            if len(file_list) > 1:
                _LOGGER.warning("Multiple flag files found")
                return file_list
            elif len(file_list) == 1:
                return file_list[0]
            else:
                _LOGGER.debug("No flag files found")
                return None

    def _strict_record_id(self, forced_value: str = None) -> str:
        """
        Get record identifier from the outer source or stored with this object

        :param str forced_value: return this value
        :return str: record identifier
        """
        if forced_value is not None:
            return forced_value
        if self.record_identifier is not None:
            return self.record_identifier
        raise PipestatError(
            f"You must provide the record identifier you want to perform "
            f"the action on. Either in the {self.__class__.__name__} "
            f"constructor or as an argument to the method."
        )

    def _create_orms(self):
        """Create ORMs."""
        _LOGGER.debug(f"Creating models for '{self.namespace}' table in '{PKG_NAME}' database")
        project_mod = self.schema.build_project_model()
        samples_mod = self.schema.build_sample_model()
        if project_mod and samples_mod:
            return {
                self.schema.sample_table_name: samples_mod,
                self.schema.project_table_name: project_mod,
            }
        elif samples_mod:
            return {self.namespace: samples_mod}
        elif project_mod:
            return {self.namespace: project_mod}
        else:
            raise SchemaError(
                f"Neither project nor samples model could be built from schema source: {self.status_schema_source}"
            )

    def _get_table_name(self, pipeline_type: Optional[str] = None):
        pipeline_type = pipeline_type or self.pipeline_type

        mods = self[DB_ORMS_KEY]
        if len(mods) == 1:
            return list(mods.keys())[0]
        elif len(mods) == 2:
            if pipeline_type is None:
                raise Exception(
                    f"Cannot determine table suffix with 2 models present and no project-level flag"
                )
            prelim = (
                self.schema.project_table_name
                if pipeline_type == "project"
                else self.schema.sample_table_name
            )
            if prelim in mods:
                return prelim
            raise Exception(
                f"Determined table name '{prelim}', which is not stored among these: {', '.join(mods.keys())}"
            )
        raise Exception(f"Cannot determine table suffix with {len(mods)} model(s) present.")

    def set_status(
        self,
        status_identifier: str,
        record_identifier: str = None,
        pipeline_type: Optional[str] = None,
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
        :param str pipeline_type: whether status is being set for a project-level pipeline, or sample-level
        """
        pipeline_type = pipeline_type or self.pipeline_type
        r_id = self._strict_record_id(record_identifier)
        known_status_identifiers = self.status_schema.keys()
        if status_identifier not in known_status_identifiers:
            raise PipestatError(
                f"'{status_identifier}' is not a defined status identifier. "
                f"These are allowed: {known_status_identifiers}"
            )
        prev_status = self.get_status(r_id)
        if self.file is not None:
            # TODO: manage project-level flag here.
            self._set_status_file(
                status_identifier=status_identifier,
                record_identifier=r_id,
                prev_status=prev_status,
            )
            # TODO: support project / sample distinction for file backend?
        else:
            tn = self._get_table_name(pipeline_type=pipeline_type)
            self._set_status_db(
                status_identifier=status_identifier,
                record_identifier=r_id,
                table_name=tn,
            )
        if prev_status:
            _LOGGER.debug(f"Changed status from '{prev_status}' to '{status_identifier}'")

        if self.backend:
            self.backend.set_status(status_identifier, record_identifier, pipeline_type)

    def get_status_flag_path(self, status_identifier: str, record_identifier=None) -> str:
        """
        Get the path to the status file flag

        :param str status_identifier: one of the defined status IDs in schema
        :param str record_identifier: unique record ID, optional if
            specified in the object constructor
        :return str: absolute path to the flag file or None if object is
            backed by a DB
        """
        if self.file is None:
            # DB as the backend
            return
        r_id = self._strict_record_id(record_identifier)
        return os.path.join(
            self[STATUS_FILE_DIR], f"{self.namespace}_{r_id}_{status_identifier}.flag"
        )

    def _set_status_file(
        self,
        status_identifier: str,
        record_identifier: str,
        prev_status: Optional[str] = None,
    ) -> None:
        if prev_status is not None:
            prev_flag_path = self.get_status_flag_path(prev_status, record_identifier)
            os.remove(prev_flag_path)
        flag_path = self.get_status_flag_path(status_identifier, record_identifier)
        create_lock(flag_path)
        with open(flag_path, "w") as f:
            f.write(status_identifier)
        remove_lock(flag_path)

    def _set_status_db(
        self,
        status_identifier: str,
        record_identifier: str,
        table_name: str,
    ) -> None:
        try:
            self._report_db(
                values={STATUS: status_identifier},
                record_identifier=record_identifier,
                table_name=table_name,
            )
        except Exception as e:
            _LOGGER.error(
                f"Could not insert into the status table ('{table_name}'). Exception: {e}"
            )
            raise

    def get_status(self, record_identifier: str = None) -> Optional[str]:
        """
        Get the current pipeline status

        :return str: status identifier, like 'running'
        """
        r_id = self._strict_record_id(record_identifier)
        if self.file is None:
            return self._get_status_db(record_identifier=r_id)
        else:
            return self._get_status_file(record_identifier=r_id)

    def _get_status_file(self, record_identifier: str) -> Optional[str]:
        r_id = self._strict_record_id(record_identifier)
        flag_file = self._get_flag_file(record_identifier=record_identifier)
        if flag_file is not None:
            assert isinstance(flag_file, str), TypeError(
                "Flag file path is expected to be a str, were multiple flags found?"
            )
            with open(flag_file, "r") as f:
                status = f.read()
            return status
        _LOGGER.debug(
            f"Could not determine status for '{r_id}' record. "
            f"No flags found in: {self[STATUS_FILE_DIR]}"
        )
        return None

    def _get_status_db(self, record_identifier: str) -> Optional[str]:
        try:
            result = self._retrieve_db(
                result_identifier=STATUS,
                record_identifier=record_identifier,
            )
        except PipestatDatabaseError:
            return None
        return result[STATUS]

    def clear_status(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        """
        Remove status flags

        :param str record_identifier: name of the record to remove flags for
        :param Iterable[str] flag_names: Names of flags to remove, optional; if
            unspecified, all schema-defined flag names will be used.
        :return List[str]: Collection of names of flags removed
        """
        r_id = self._strict_record_id(record_identifier)
        if self.file is not None:
            return self._clear_status_file(record_identifier=r_id, flag_names=flag_names)
        else:
            return self._clear_status_db(record_identifier=r_id)

        if self.backend:
            self.backend.clear_status(record_identifier=r_id, flag_names=flag_names)

    def _clear_status_file(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[Union[str, None]]:
        flag_names = flag_names or list(self.status_schema.keys())
        if isinstance(flag_names, str):
            flag_names = [flag_names]
        removed = []
        for f in flag_names:
            path_flag_file = self.get_status_flag_path(
                status_identifier=f, record_identifier=record_identifier
            )
            try:
                os.remove(path_flag_file)
            except:
                pass
            else:
                _LOGGER.info(f"Removed existing flag: {path_flag_file}")
                removed.append(f)
        return removed

    def _clear_status_db(self, record_identifier: str = None) -> List[Union[str, None]]:
        removed = self.get_status(record_identifier)
        try:
            self._remove_db(record_identifier=record_identifier)
        except Exception as e:
            _LOGGER.error(f"Could not remove the status from the database. Exception: {e}")
            return []
        else:
            return [removed]

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

    def _init_results_file(self) -> None:
        """
        Initialize YAML results file if it does not exist.
        Read the data stored in the existing file into the memory otherwise.

        :return bool: whether the file has been created
        """
        _LOGGER.info(f"Initializing results file '{self.file}'")
        data = YAMLConfigManager(
            entries={self.namespace: "{}"}, filepath=self.file, create_file=True
        )
        with data as data_locked:
            data_locked.write()
        self[DATA_KEY] = data

    def _load_results_file(self) -> None:
        _LOGGER.debug(f"Reading data from '{self.file}'")
        data = YAMLConfigManager(filepath=self.file)
        namespaces_reported = [k for k in data.keys() if not k.startswith("_")]
        num_namespaces = len(namespaces_reported)
        if num_namespaces == 0:
            self[DATA_KEY] = data
        elif num_namespaces == 1:
            previous = namespaces_reported[0]
            if self.namespace != previous:
                msg = f"'{self.file}' is already used to report results for a different (not {self.namespace}) namespace: {previous}"
                raise PipestatError(msg)
            self[DATA_KEY] = data
        else:
            raise PipestatError(
                f"'{self.file}' is in use for {num_namespaces} namespaces: {', '.join(namespaces_reported)}"
            )

    @property
    def _engine(self):
        """Access the database engine backing this manager."""
        try:
            return self[DB_ENGINE_KEY]
        except KeyError:
            # Do it this way rather than .setdefault to avoid evaluating
            # the expression for the default argument (i.e., building
            # the engine) if it's not necessary.
            self[DB_ENGINE_KEY] = create_engine(self.db_url, echo=self._show_db_logs)
            return self[DB_ENGINE_KEY]

    def _table_to_dict(self) -> None:
        """
        Create a dictionary from the database table data
        """
        with self.session as s:
            records = s.query(self.get_orm(table_name=self.namespace)).all()
        _LOGGER.debug(f"Reading data from database for '{self.namespace}' namespace")
        for record in records:
            record_id = getattr(record, RECORD_ID)
            for column in record.__table__.columns:
                val = getattr(record, column.name, None)
                if val is not None:
                    self._report_data_element(
                        record_identifier=record_id, values={column.name: val}
                    )

    def _get_attr(self, attr: str) -> Any:
        """
        Safely get the name of the selected attribute of this object

        :param str attr: attr to select
        :return:
        """
        return self.get(attr)

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if the specified table exists

        :param str table_name: table name to be checked
        :return bool: whether the specified table exists
        """
        from sqlalchemy import inspect

        with self.session as s:
            return inspect(s.bind).has_table(table_name=table_name)

    def _get_model(self, table_name: str, strict: bool):
        orms = self[DB_ORMS_KEY]

        table_name = self._get_table_name()

        mod = orms.get(table_name)

        if strict and mod is None:
            raise PipestatDatabaseError(
                f"No object relational mapper class defined for table '{table_name}'. "
                f"{len(orms)} defined: {', '.join(orms.keys())}"
            )
        return mod

    def _count_rows(self, table_name: str) -> int:
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: number of rows in the selected table
        """
        mod = self._get_model(table_name=table_name, strict=True)
        with self.session as s:
            stmt = sql_select(mod)
            records = s.exec(stmt).all()
            return len(records)

    def get_orm(self, table_name: str) -> Any:
        """
        Get an object relational mapper class

        :param str table_name: table name to get a class for
        :return Any: Object relational mapper class
        """
        if DB_ORMS_KEY not in self:
            raise PipestatDatabaseError("Object relational mapper classes not defined")
        mod = self._get_model(table_name=table_name, strict=True)
        return mod

    def check_record_exists(
        self,
        record_identifier: str,
        table_name: str,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the specified record exists in the table

        :param str record_identifier: record to check for
        :param str table_name: table name to check
        :return bool: whether the record exists in the table
        """
        pipeline_type = pipeline_type or self.pipeline_type
        if self.file is None:
            query_hit = self.get_one_record(rid=record_identifier, table_name=table_name)
            return query_hit is not None
        else:
            return (
                self.namespace in self.data
                and record_identifier in self.data[table_name][pipeline_type]
            )

    def check_which_results_exist(
        self,
        results: List[str],
        rid: Optional[str] = None,
        table_name: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> List[str]:
        """
        Check which results have been reported

        :param List[str] results: names of the results to check
        :param str rid: unique identifier of the record
        :param str table_name: name of the table for which to check results
        :return List[str]: names of results which exist
        """

        pipeline_type = pipeline_type or self.pipeline_type

        rid = self._strict_record_id(rid)
        if self.file is None:
            return self._check_which_results_exist_db(
                results=results, rid=rid, table_name=table_name
            )
        if self.namespace not in self.data:
            return []
        return [
            r
            for r in results
            if rid in self.data[self.namespace][pipeline_type]
            and r in self.data[self.namespace][pipeline_type][rid]
        ]

    def _check_which_results_exist_db(
        self, results: List[str], rid: str = None, table_name: str = None
    ) -> List[str]:
        """
        Check if the specified results exist in the table

        :param List[str] results: results identifiers to check for
        :param str rid: record to check for
        :param str table_name: name of the table to search for results in
        :return List[str]: results identifiers that exist
        """
        # table_name = table_name or self.namespace
        rid = self._strict_record_id(rid)
        record = self.get_one_record(rid=rid, table_name=table_name)
        return [r for r in results if getattr(record, r, None) is not None] if record else []

    def get_one_record(self, table_name: str, rid: Optional[str] = None):
        models = (
            [self.get_orm(table_name=table_name)]
            if table_name
            else list(self[DB_ORMS_KEY].values())
        )
        with self.session as s:
            for mod in models:
                # record = sql_select(mod).where(mod.record_identifier == rid).first()
                # record = s.query(mod).where(mod.record_identifier == rid).first()
                stmt = sql_select(mod).where(mod.record_identifier == rid)
                # stmt = sql_select(mod)
                record = s.exec(stmt).first()
                # record = (
                #     s.query(mod)
                #     .filter_by(record_identifier=rid)
                #     .first()
                # )
                if record:
                    return record

    def check_result_exists(
        self,
        result_identifier: str,
        record_identifier: str = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Check if the result has been reported

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to check
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        record_identifier = self._strict_record_id(record_identifier)
        return (
            len(
                self.check_which_results_exist(
                    results=[result_identifier],
                    rid=record_identifier,
                    pipeline_type=pipeline_type,
                )
            )
            > 0
        )

    def select(
        self,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
        json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        pipeline_type: Optional[str] = None,
    ) -> List[Any]:
        """
        Perform a `SELECT` on the table

        :param str table_name: name of the table to SELECT from
        :param List[str] columns: columns to include in the result
        :param [(key,operator,value)] filter_conditions: e.g. [("id", "eq", 1)], operator list:
            - eq for ==
            - lt for <
            - ge for >=
            - in for in_
            - like for like
        :param [(col,key,value)] json_filter_conditions: conditions for JSONB column to
            query that include JSON column name, key withing the JSON object in that
            column and the value to check the identity against. Therefore only '==' is
            supported in non-nested checks, e.g. [("other", "genome", "hg38")]
        :param int offset: skip this number of rows
        :param int limit: include this number of rows
        """
        pipeline_type = pipeline_type or self.pipeline_type

        if self.backend:
            result = self.backend.select(
                table_name,
                columns,
                filter_conditions,
                json_filter_conditions,
                offset,
                limit,
                pipeline_type,
            )

        return result

    def select_distinct(self, table_name, columns) -> List[Any]:
        """
        Perform a `SELECT DISTINCT` on given table and column

        :param str table_name: name of the table to SELECT from
        :param List[str] columns: columns to include in the result
        """

        ORM = self.get_orm(table_name or self.namespace)
        with self.session as s:
            query = s.query(*[getattr(ORM, column) for column in columns])
            query = query.distinct()
            result = query.all()
        return result

    def retrieve(
        self,
        record_identifier: Optional[str] = None,
        result_identifier: Optional[str] = None,
        pipeline_type: Optional[str] = None,
    ) -> Union[Any, Dict[str, Any]]:
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :return any | Dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """

        pipeline_type = pipeline_type or self.pipeline_type

        record_identifier = self._strict_record_id(record_identifier)
        # should change to simpler: record_identifier = record_identifier or self.record_identifier
        if self.file is None:
            results = self.backend.retrieve(record_identifier, result_identifier, pipeline_type)
            if result_identifier is not None:
                return results[result_identifier]
            return results
        else:
            return self.backend.retrieve(record_identifier, result_identifier, pipeline_type)

    def _retrieve_db(
        self,
        result_identifier: str = None,
        record_identifier: str = None,
        table_name: str = None,
    ) -> Dict[str, Any]:
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :param str table_name: name of the table to search for results in
        :return Dict[str, any]: a single result or a mapping with all the results
            reported for the record
        """
        table_name = table_name or self.namespace
        record_identifier = self._strict_record_id(record_identifier)
        if result_identifier is not None:
            existing = self.check_which_results_exist(
                results=[result_identifier],
                rid=record_identifier,
                table_name=table_name,
            )
            if not existing:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record " f"'{record_identifier}'"
                )

        with self.session as s:
            record = (
                s.query(self.get_orm(table_name=table_name))
                .filter_by(record_identifier=record_identifier)
                .first()
            )

        if record is not None:
            if result_identifier is not None:
                return {result_identifier: getattr(record, result_identifier)}
            return {
                column: getattr(record, column)
                for column in [c.name for c in record.__table__.columns]
                if getattr(record, column, None) is not None
            }
        raise PipestatDatabaseError(f"Record '{record_identifier}' not found")

    def select_txt(
        self,
        columns: Optional[List[str]] = None,
        filter_templ: Optional[str] = "",
        filter_params: Optional[Dict[str, Any]] = {},
        table_name: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Any]:
        """
        Execute a query with a textual filter. Returns all results.

        To retrieve all table contents, leave the filter arguments out.
        Table name defaults to the namespace

        :param str filter_templ: filter template with value placeholders,
             formatted as follows `id<:value and name=:name`
        :param Dict[str, Any] filter_params: a mapping keys specified in the `filter_templ`
            to parameters that are supposed to replace the placeholders
        :param str table_name: name of the table to query
        :param int offset: skip this number of rows
        :param int limit: include this number of rows
        :return List[Any]: a list of matched records
        """
        if self.file:
            raise PipestatDatabaseError(
                f"The {self.__class__.__name__} object is not backed by a database. "
                f"This operation is not supported for file backend."
            )
        ORM = self.get_orm(table_name=table_name or self.namespace)
        with self.session as s:
            if columns is not None:
                q = (
                    s.query(*[getattr(ORM, column) for column in columns])
                    .filter(text(filter_templ))
                    .params(**filter_params)
                )
            else:
                q = s.query(ORM).filter(text(filter_templ)).params(**filter_params)
            if isinstance(offset, int):
                q = q.offset(offset)
            if isinstance(limit, int):
                q = q.limit(limit)
            results = q.all()
        return results

    def assert_results_defined(self, results: List[str], pipeline_type: str) -> None:
        """
        Assert provided list of results is defined in the schema

        :param List[str] results: list of results to
            check for existence in the schema
        :raises SchemaError: if any of the results is not defined in the schema
        """

        # take project level input and look for keys in the specific schema.
        # warn if you are trying to report a sample to a project level and vice versa.

        if pipeline_type == "sample":
            known_results = self["_schema"].sample_level_data.keys()
        if pipeline_type == "project":
            known_results = self["_schema"].project_level_data.keys()

        # known_results = self.result_schemas.keys()

        for r in results:
            assert r in known_results, SchemaError(
                f"'{r}' is not a known result. Results defined in the "
                f"schema are: {list(known_results)}."
            )

    def report(
        self,
        values: Dict[str, Any],
        record_identifier: str = None,
        force_overwrite: bool = False,
        strict_type: bool = True,
        return_id: bool = False,
        pipeline_type: Optional[str] = None,
    ) -> Union[bool, int]:
        """
        Report a result.

        :param Dict[str, any] values: dictionary of result-value pairs
        :param str record_identifier: unique identifier of the record, value
            in 'record_identifier' column to look for to determine if the record
            already exists
        :param bool force_overwrite: whether to overwrite the existing record
        :param bool strict_type: whether the type of the reported values should
            remain as is. Pipestat would attempt to convert to the
            schema-defined one otherwise
        :param bool return_id: PostgreSQL IDs of the records that have been
            updated. Not available with results file as backend
        :param pipeline_type: whether what's being reported pertains to project-level,
            rather than sample-level, attribute(s)
        :return bool | int: whether the result has been reported or the ID of
            the updated record in the table, if requested
        """

        pipeline_type = pipeline_type or self.pipeline_type
        values = deepcopy(values)

        record_identifier = self._strict_record_id(record_identifier)
        if return_id and self.file is not None:
            raise NotImplementedError(
                "There is no way to return the updated object ID while using "
                "results file as the object backend"
            )
        if self.schema is None:
            raise SchemaNotFoundError("report results")
        updated_ids = False
        result_identifiers = list(values.keys())
        for r in result_identifiers:
            validate_type(value=values[r], schema=self.result_schemas[r], strict_type=strict_type)

        _LOGGER.warning("Writing to locked data...")

        if self.backend:
            self.backend.report(values, record_identifier, pipeline_type)

        nl = "\n"
        _LOGGER.warning("TEST HERE")
        rep_strs = [f"{k}: {v}" for k, v in values.items()]
        _LOGGER.info(
            f"Reported records for '{record_identifier}' in '{self.namespace}' "
            f"namespace:{nl} - {(nl + ' - ').join(rep_strs)}"
        )
        _LOGGER.info(record_identifier, values)
        return True if not return_id else updated_ids

    def _report_db(self, values: Dict[str, Any], record_identifier: str, table_name: str) -> int:
        """
        Report a result to a database.

        :param Dict[str, Any] values: values to report
        :param str record_identifier: record to report the result for
        :param str table_name: name of the table to report the result in
        :return int: updated/inserted row
        """
        record_identifier = self._strict_record_id(record_identifier)
        ORMClass = self.get_orm(table_name=table_name)
        values.update({RECORD_ID: record_identifier})
        values.update({"namespace": self["_config"]["namespace"]})

        if not self.check_record_exists(
            record_identifier=record_identifier, table_name=table_name
        ):
            new_record = ORMClass(**values)
            with self.session as s:
                s.add(new_record)
                s.commit()
                returned_id = new_record.id
        else:
            with self.session as s:
                record_to_update = (
                    s.query(ORMClass)
                    .filter(getattr(ORMClass, RECORD_ID) == record_identifier)
                    .first()
                )
                for result_id, result_value in values.items():
                    setattr(record_to_update, result_id, result_value)
                s.commit()
                returned_id = record_to_update.id
        return returned_id

    def _report_data_element(
        self,
        record_identifier: str,
        values: Dict[str, Any],
        pipeline_type: Optional[str] = None,
        table_name: Optional[bool] = None,
    ) -> None:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        :param str table_name: name of the table to report the result in
        """

        pipeline_type = pipeline_type or self.pipeline_type

        # TODO: update to disambiguate sample- / project-level
        self[DATA_KEY].setdefault(self.namespace, {})
        # self[DATA_KEY][self.namespace].setdefault(record_identifier, {})
        self[DATA_KEY][self.namespace].setdefault(pipeline_type, {})
        self[DATA_KEY][self.namespace][pipeline_type].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self[DATA_KEY][self.namespace][pipeline_type][record_identifier][res_id] = val
            # self[DATA_KEY][self.namespace][record_identifier][res_id] = val

    def remove(
        self,
        record_identifier: str = None,
        result_identifier: str = None,
        pipeline_type: Optional[str] = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :return bool: whether the result has been removed
        """

        pipeline_type = pipeline_type or self.pipeline_type

        r_id = self._strict_record_id(record_identifier)
        if self.backend:
            return self.backend.remove(record_identifier, result_identifier, pipeline_type)
        else:
            return True

    def _remove_db(
        self,
        record_identifier: str = None,
        result_identifier: str = None,
        table_name: str = None,
    ) -> bool:
        """
        Remove a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :param str table_name: name of the table to report the result in
        :return bool: whether the result has been removed
        :raise PipestatDatabaseError: if either record or result specified are not found
        """
        table_name = table_name or self.namespace
        record_identifier = self._strict_record_id(record_identifier)
        ORMClass = self.get_orm(table_name=table_name)
        if self.check_record_exists(record_identifier=record_identifier, table_name=table_name):
            with self.session as s:
                records = s.query(ORMClass).filter(
                    getattr(ORMClass, RECORD_ID) == record_identifier
                )
                if result_identifier is None:
                    # delete row
                    records.delete()
                else:
                    # set the value to None
                    if not self.check_result_exists(
                        record_identifier=record_identifier,
                        result_identifier=result_identifier,
                    ):
                        raise PipestatDatabaseError(
                            f"Result '{result_identifier}' not found for record "
                            f"'{record_identifier}'"
                        )
                    setattr(records.first(), result_identifier, None)
                s.commit()
        else:
            raise PipestatDatabaseError(f"Record '{record_identifier}' not found")
