from contextlib import contextmanager
from logging import getLogger
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import sqlalchemy.orm
from sqlalchemy import Column, ForeignKey, text
from sqlalchemy.orm import (
    DeclarativeMeta,
    relationship,
)

from sqlmodel import Field, Session, SQLModel, create_engine, select as sql_select

from jsonschema import validate

from ubiquerg import create_lock, remove_lock
from yacman import YAMLConfigManager
from .exceptions import *
from .helpers import *
from .parsed_schema import ParsedSchema

_LOGGER = getLogger(PKG_NAME)


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
        config: Optional[Union[str, dict]] = None,
        flag_file_dir: Optional[str] = None,
        show_db_logs: bool = False,
    ):
        """
        Initialize the object

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

        def _mk_abs_via_cfg(
            path: Optional[str],
            cfg_path: Optional[str],
        ) -> Optional[str]:
            if path is None:
                return path
            assert isinstance(path, str), TypeError("Path is expected to be a str")
            if os.path.isabs(path):
                return path
            if cfg_path is None:
                rel_to_cwd = os.path.join(os.getcwd(), path)
                if os.path.exists(rel_to_cwd) or os.access(
                    os.path.dirname(rel_to_cwd), os.W_OK
                ):
                    return rel_to_cwd
                else:
                    raise OSError(f"File not found: {path}")
            joined = os.path.join(os.path.dirname(cfg_path), path)
            if os.path.isabs(joined):
                return joined
            raise OSError(f"Could not make this path absolute: {path}")

        def _select_value(
            arg_name: str,
            cfg: dict,
            strict: bool = True,
            env_var: str = None,
        ) -> Any:
            if cfg.get(arg_name) is None:
                if env_var is not None:
                    arg = os.getenv(env_var, None)
                    if arg is not None:
                        _LOGGER.debug(f"Value '{arg}' sourced from '{env_var}' env var")
                        return expandpath(arg)
                message = f"Value for the required '{arg_name}' argument could not be determined."
                if strict:
                    raise PipestatError(message)
                _LOGGER.warning(message)
                return
            return cfg[arg_name]

        super(PipestatManager, self).__init__()
        # read config or config data
        config = config or os.getenv(ENV_VARS["config"])
        if config is not None:
            if isinstance(config, str):
                config = os.path.abspath(expandpath(config))
                self._config_path = config
            elif isinstance(config, dict):
                self._config_path = None
            else:
                raise TypeError(
                    "database_config has to be either path to the "
                    "file to read or a dict"
                )
            self[CONFIG_KEY] = YAMLConfigManager(filepath=config)
            # validate config
            cfg = self[CONFIG_KEY].exp
            _, cfg_schema = read_yaml_data(CFG_SCHEMA, "config schema")
            validate(cfg, cfg_schema)
        else:
            self[CONFIG_KEY] = YAMLConfigManager()

        # Finalize results file.
        results_file_path = _mk_abs_via_cfg(
            _select_value(
                "results_file_path",
                self[CONFIG_KEY],
                False,
                ENV_VARS["results_file"],
            )
            if results_file_path is None
            else results_file_path,
            self.config_path,
        )

        # Validation of presence of backend and database info, if applicable
        # Backend is implied as file if results_file_path is truthy.
        # Otherwise, assume database as backend and validate accordingly.
        if not results_file_path:
            if CFG_DATABASE_KEY not in self[CONFIG_KEY]:
                raise NoBackendSpecifiedError()
            try:
                dbconf = self[CONFIG_KEY][CFG_DATABASE_KEY]
            except KeyError:
                raise PipestatDatabaseError(
                    f"No database section ('{CFG_DATABASE_KEY}') in config"
                )
            try:
                creds = dict(
                    name=dbconf[CFG_NAME_KEY],
                    user=dbconf[CFG_USER_KEY],
                    passwd=dbconf[CFG_PASSWORD_KEY],
                    host=dbconf[CFG_HOST_KEY],
                    port=dbconf[CFG_PORT_KEY],
                    dialect=dbconf[CFG_DIALECT_KEY],
                    driver=dbconf[CFG_DRIVER_KEY],
                )
            except KeyError as e:
                raise MissingConfigDataError(
                    f"Could not determine database URL. Caught error: {str(e)}"
                )
            parsed_creds = {k: quote_plus(str(v)) for k, v in creds.items()}
            self._db_url = (
                "{dialect}+{driver}://{user}:{passwd}@{host}:{port}/{name}".format(
                    **parsed_creds
                )
            )

        self[RECORD_ID_KEY] = record_identifier or _select_value(
            "record_identifier",
            self[CONFIG_KEY],
            False,
            ENV_VARS["record_identifier"],
        )
        self[DB_ONLY_KEY] = database_only

        # read schema
        self._schema_path = (
            _select_value(
                "schema_path",
                self[CONFIG_KEY],
                False,
                env_var=ENV_VARS["schema"],
            )
            if schema_path is None
            else schema_path
        )
        if self._schema_path is None:
            raise PipestatError("No schema path could be found.")
        schema_to_read = _mk_abs_via_cfg(self._schema_path, self.config_path)
        self[SCHEMA_KEY] = ParsedSchema(schema_to_read)
        self.validate_schema()
        # TODO: if no status schema nested in main one, check env var and default.
        # TODO: set status schema source key's value
        # TODO: validate presence of status schema if no results schema (neither project nor samples)

        if results_file_path:
            _LOGGER.debug(f"Determined file as backend: {results_file_path}")
            if self[DB_ONLY_KEY]:
                _LOGGER.debug(
                    "Running in database only mode does not make sense with a YAML file as a backend. "
                    "Changing back to using memory."
                )
                self[DB_ONLY_KEY] = False
            self[FILE_KEY] = results_file_path
            if not os.path.exists(self.file):
                self._init_results_file()
            else:
                self._load_results_file()
            flag_file_dir = (
                _select_value("flag_file_dir", self[CONFIG_KEY], False)
                if flag_file_dir is None
                else flag_file_dir
            ) or os.path.dirname(self.file)
            self[STATUS_FILE_DIR] = _mk_abs_via_cfg(flag_file_dir, self.config_path)
        else:
            _LOGGER.debug("Determined database as backend")
            self[DATA_KEY] = YAMLConfigManager()
            self._show_db_logs = show_db_logs
            self[DB_ORMS_KEY] = self._create_orms()
            SQLModel.metadata.create_all(self._engine)

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
        high_res = self.highlighted_results
        if high_res:
            res += f"\nHighlighted results: {', '.join(high_res)}"
        return res

    @property
    def highlighted_results(self) -> List[str]:
        """
        Highlighted results

        :return List[str]: a collection of highlighted results
        """
        return [k for k, v in self.result_schemas if v.get("highlight", False)]

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
    def db_column_relationships_by_result(self) -> Dict[str, str]:
        """
        Database column relationships for every result,
        sourced from the results schema in the `relationship` section

        *Note: this is an experimental feature*

        :return Dict[str, Dict[str, str]]: relationships for every result
        """
        if self.schema is None:
            return {}

        def _validate_rel_section(result_id):
            if not all(
                [
                    k in self.schema[result_id][DB_RELATIONSHIP_KEY].keys()
                    for k in DB_RELATIONSHIP_ELEMENTS
                ]
            ):
                PipestatDatabaseError(
                    f"Not all required {DB_RELATIONSHIP_KEY} settings ({DB_RELATIONSHIP_ELEMENTS}) were "
                    f"provided for result: {result_id}"
                )
            return True

        return {
            result_id: self.schema[result_id][DB_RELATIONSHIP_KEY]
            for result_id in self.schema.keys()
            if DB_RELATIONSHIP_KEY in self.schema[result_id]
            and _validate_rel_section(result_id)
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
        return self.schema.status_data

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
    def result_schemas(self) -> Dict:
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

    def _get_flag_file(
        self, record_identifier: str = None
    ) -> Union[str, List[str], None]:
        """
        Get path to the status flag file for the specified record

        :param str record_identifier: unique record identifier
        :return str | list[str] | None: path to the status flag file
        """
        from glob import glob

        r_id = self._strict_record_id(record_identifier)
        if self.file is None:
            return
        if self.file is not None:
            regex = os.path.join(
                self[STATUS_FILE_DIR], f"{self.namespace}_{r_id}_*.flag"
            )
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
        _LOGGER.debug(
            f"Creating models for '{self.namespace}' table in '{PKG_NAME}' database"
        )
        models = {}
        schema = self.schema
        #models[schema.project_table_name] = schema.build_project_model()
        models[self.namespace] = schema.build_project_model()
        return models

    def set_status(self, status_identifier: str, record_identifier: str = None) -> None:
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
        r_id = self._strict_record_id(record_identifier)
        known_status_identifiers = self.status_schema.keys()
        if status_identifier not in known_status_identifiers:
            raise PipestatError(
                f"'{status_identifier}' is not a defined status identifier. "
                f"These are allowed: {known_status_identifiers}"
            )
        prev_status = self.get_status(r_id)
        if self.file is not None:
            self._set_status_file(
                status_identifier=status_identifier,
                record_identifier=r_id,
                prev_status=prev_status,
            )
        else:
            self._set_status_db(
                status_identifier=status_identifier,
                record_identifier=r_id,
            )
        if prev_status:
            _LOGGER.debug(
                f"Changed status from '{prev_status}' to '{status_identifier}'"
            )

    def get_status_flag_path(
        self, status_identifier: str, record_identifier=None
    ) -> str:
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
    ) -> None:
        try:
            self._report_db(
                values={STATUS: status_identifier},
                record_identifier=record_identifier,
                table_name=f"{self.namespace}_{STATUS}",
            )
        except Exception as e:
            _LOGGER.error(f"Could not insert into the status table. Exception: {e}")
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
                table_name=f"{self.namespace}_{STATUS}",
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
            return self._clear_status_file(
                record_identifier=r_id, flag_names=flag_names
            )
        else:
            return self._clear_status_db(record_identifier=r_id)

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
            self._remove_db(
                record_identifier=record_identifier,
                table_name=f"{self.namespace}_{STATUS}",
            )
        except Exception as e:
            _LOGGER.error(
                f"Could not remove the status from the database. Exception: {e}"
            )
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

        reserved_keywords_used = self.schema.reserved_keywords_used
        if reserved_keywords_used:
            raise SchemaError(
                f"{len(reserved_keywords_used)} reserved keyword(s) used: {', '.join(reserved_keywords_used)}"
            )
        # TODO: pare down to what really needs dealt with here.
        project_sample_overlap = set(self.schema.project_level_data) & set(
            self.schema.sample_level_data
        )
        if project_sample_overlap:
            raise SchemaError(
                f"Overlap between project- and sample-level keys: {', '.join(project_sample_overlap)}"
            )

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
        raise PipestatError(f"'{self.file}' is in use for {num_namespaces} namespaces: {', '.join(namespaces_reported)}")

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
            records = s.query(self.get_orm(self.namespace)).all()
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

    def _count_rows(self, table_name: str) -> int:
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: number of rows in the selected table
        """
        mod = self[DB_ORMS_KEY][table_name]
        with self.session as s:
            return s.select(mod).count()

    def get_orm(self, table_name: Optional[str] = None) -> Any:
        """
        Get an object relational mapper class

        :param str table_name: table name to get a class for
        :return Any: Object relational mapper class
        """
        if DB_ORMS_KEY not in self:
            raise PipestatDatabaseError("Object relational mapper classes not defined")
        tn = table_name or self.namespace
        orms = self[DB_ORMS_KEY]
        mod = orms.get(tn)
        if mod is None:
            raise PipestatDatabaseError(
                f"No object relational mapper class defined for table '{tn}'. "
                f"{len(orms)} defined: {', '.join(orms.keys())}"
            )
        if not isinstance(mod, DeclarativeMeta):
            raise PipestatDatabaseError(
                f"Object relational mapper class for table '{tn}' is invalid"
            )
        return mod

    def check_record_exists(
        self, record_identifier: str, table_name: str = None
    ) -> bool:
        """
        Check if the specified record exists in the table

        :param str record_identifier: record to check for
        :param str table_name: table name to check
        :return bool: whether the record exists in the table
        """
        if self.file is None:
            query_hit = self.get_one_record(rid=record_identifier, table_name=table_name)
            return query_hit is not None
        else:
            return self.namespace in self.data and record_identifier in self.data[table_name]

    def check_which_results_exist(
        self,
        results: List[str],
        rid: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[str]:
        """
        Check which results have been reported

        :param List[str] results: names of the results to check
        :param str rid: unique identifier of the record
        :param str table_name: name of the table for which to check results
        :return List[str]: names of results which exist
        """
        rid = self._strict_record_id(rid)
        if self.file is None:
            return self._check_which_results_exist_db(
                results=results, rid=rid, table_name=table_name
            )
        if self.namespace not in self.data:
            return []
        return [r for r in results if rid in self.data[self.namespace] and r in self.data[self.namespace][rid]]

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
        #table_name = table_name or self.namespace
        rid = self._strict_record_id(rid)
        models = [self.get_orm(table_name)] if table_name else list(self[DB_ORMS_KEY].values())
        # DEBUG
        print("MODELS")
        print(models)
        with self.session as s:
            record = self.get_one_record(rid=rid, table_name=table_name)

        return [r for r in results if getattr(record, r, None) is not None] if record else []

    def get_one_record(self, rid: Optional[str] = None, table_name: Optional[str] = None):
        models = [self.get_orm(table_name)] if table_name else list(self[DB_ORMS_KEY].values())
        # DEBUG
        print("MODELS")
        print(models)
        with self.session as s:
            for mod in models:
                # DEBUG
                print("DIR(mod)")
                print(dir(mod))
                # print("OUTPUT_FILE")
                # print(getattr(mod, "output_file"))
                print("SCHEMA")
                print(mod.schema_json())
                # record = sql_select(mod).where(mod.record_identifier == rid).first()
                # record = s.query(mod).where(mod.record_identifier == rid).first()
                stmt = sql_select(mod).where(mod.record_identifier == rid)
                #stmt = sql_select(mod)
                print("STATEMENT")
                print(stmt)
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
                )
            )
            > 0
        )

    def select(
        self,
        table_name: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filter_conditions: Optional[
            List[Tuple[str, str, Union[str, List[str]]]]
        ] = None,
        json_filter_conditions: Optional[List[Tuple[str, str, str]]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
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

        ORM = self.get_orm(table_name or self.namespace)
        with self.session as s:
            if columns is not None:
                query = s.query(*[getattr(ORM, column) for column in columns])
            else:
                query = s.query(ORM)
            query = dynamic_filter(
                ORM=ORM,
                query=query,
                filter_conditions=filter_conditions,
                json_filter_conditions=json_filter_conditions,
            )
            if isinstance(offset, int):
                query = query.offset(offset)
            if isinstance(limit, int):
                query = query.limit(limit)
            result = query.all()
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
        r_id = self._strict_record_id(record_identifier)
        if self.file is None:
            results = self._retrieve_db(
                result_identifier=result_identifier, record_identifier=r_id
            )
            if result_identifier is not None:
                return results[result_identifier]
            return results
        else:
            if r_id not in self.data[self.namespace]:
                raise PipestatDatabaseError(f"Record '{r_id}' not found")
            if result_identifier is None:
                return self.data.exp[self.namespace][r_id]
            if result_identifier not in self.data[self.namespace][r_id]:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record '{r_id}'"
                )
            return self.data[self.namespace][r_id][result_identifier]

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
                    f"Result '{result_identifier}' not found for record "
                    f"'{record_identifier}'"
                )

        with self.session as s:
            record = (
                s.query(self.get_orm(table_name))
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
        ORM = self.get_orm(table_name or self.namespace)
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

    def assert_results_defined(self, results: List[str]) -> None:
        """
        Assert provided list of results is defined in the schema

        :param List[str] results: list of results to
            check for existence in the schema
        :raises SchemaError: if any of the results is not defined in the schema
        """
        known_results = self.result_schemas.keys()
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
        :return bool | int: whether the result has been reported or the ID of
            the updated record in the table, if requested
        """
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
        self.assert_results_defined(results=result_identifiers)
        existing = self.check_which_results_exist(
            rid=record_identifier, results=result_identifiers
        )
        if existing:
            existing_str = ", ".join(existing)
            _LOGGER.warning(
                f"These results exist for '{record_identifier}': {existing_str}"
            )
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing_str}")
        for r in result_identifiers:
            validate_type(
                value=values[r], schema=self.result_schemas[r], strict_type=strict_type
            )

        # if self.file is not None:
        # self.data.make_writable()

        _LOGGER.warning("Writing to locked data...")

        if not self[DB_ONLY_KEY]:
            self._report_data_element(
                record_identifier=record_identifier, values=values
            )
        if self.file is not None:
            with self.data as locked_data:
                locked_data.write()
        else:
            _LOGGER.warning("ELSE...")
            try:
                updated_ids = self._report_db(
                    record_identifier=record_identifier, values=values
                )
            except Exception as e:
                _LOGGER.error(
                    f"Could not insert the result into the database. Exception: {e}"
                )
                if not self[DB_ONLY_KEY]:
                    for r in result_identifiers:
                        del self[DATA_KEY][self.namespace][record_identifier][r]
                raise
        nl = "\n"
        _LOGGER.warning("TEST HERE")
        rep_strs = [f"{k}: {v}" for k, v in values.items()]
        _LOGGER.info(
            f"Reported records for '{record_identifier}' in '{self.namespace}' "
            f"namespace:{nl} - {(nl + ' - ').join(rep_strs)}"
        )
        _LOGGER.warning(self.data)
        _LOGGER.warning(updated_ids)
        _LOGGER.info(record_identifier, values)
        return True if not return_id else updated_ids

    def _report_db(
        self, values: Dict[str, Any], record_identifier: str, table_name: str = None
    ) -> int:
        """
        Report a result to a database

        :param Dict[str, Any] values: values to report
        :param str record_identifier: record to report the result for
        :param str table_name: name of the table to report the result in
        :return int: updated/inserted row
        """
        record_identifier = self._strict_record_id(record_identifier)
        ORMClass = self.get_orm(table_name)
        values.update({RECORD_ID: record_identifier})
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
        self, record_identifier: str, values: Dict[str, Any]
    ) -> None:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param Dict[str, Any] values: dict of results identifiers and values
            to be reported
        """
        self[DATA_KEY].setdefault(self.namespace, {})
        self[DATA_KEY][self.namespace].setdefault(record_identifier, {})
        for res_id, val in values.items():
            self[DATA_KEY][self.namespace][record_identifier][res_id] = val

    def remove(
        self,
        record_identifier: str = None,
        result_identifier: str = None,
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
        r_id = self._strict_record_id(record_identifier)
        rm_record = True if result_identifier is None else False
        if not self.check_record_exists(
            record_identifier=r_id, table_name=self.namespace
        ):
            _LOGGER.error(f"Record '{r_id}' not found")
            return False
        if result_identifier and not self.check_result_exists(result_identifier, r_id):
            _LOGGER.error(f"'{result_identifier}' has not been reported for '{r_id}'")
            return False

        if not self[DB_ONLY_KEY]:
            if rm_record:
                _LOGGER.info(f"Removing '{r_id}' record")
                del self[DATA_KEY][self.namespace][r_id]
            else:
                val_backup = self[DATA_KEY][self.namespace][r_id][result_identifier]
                del self[DATA_KEY][self.namespace][r_id][result_identifier]
                _LOGGER.info(
                    f"Removed result '{result_identifier}' for record "
                    f"'{r_id}' from '{self.namespace}' namespace"
                )
                if not self[DATA_KEY][self.namespace][r_id]:
                    _LOGGER.info(
                        f"Last result removed for '{r_id}'. " f"Removing the record"
                    )
                    del self[DATA_KEY][self.namespace][r_id]
                    rm_record = True

            if self.file:
                with self.data as locked_data:
                    locked_data.write()

        if self.file is None:
            try:
                self._remove_db(
                    record_identifier=r_id,
                    result_identifier=None if rm_record else result_identifier,
                )
            except Exception as e:
                _LOGGER.error(
                    f"Could not remove the result from the database. Exception: {e}"
                )
                if not self[DB_ONLY_KEY] and not rm_record:
                    self[DATA_KEY][self.namespace][r_id][result_identifier] = val_backup
                raise
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
        if self.check_record_exists(
            record_identifier=record_identifier, table_name=table_name
        ):
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
