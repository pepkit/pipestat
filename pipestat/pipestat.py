from contextlib import contextmanager
from copy import deepcopy
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import sqlalchemy.orm
from attmap import PathExAttMap as PXAM
from jsonschema import validate
from sqlalchemy import Column, Float, ForeignKey, Integer, String, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeMeta, relationship, sessionmaker
from ubiquerg import create_lock, remove_lock
from yacman import YacAttMap

from .const import *
from .exceptions import *
from .helpers import *

_LOGGER = getLogger(PKG_NAME)


class PipestatManager(dict):
    """
    Pipestat standardizes reporting of pipeline results and
    pipeline status management. It formalizes a way for pipeline developers
    and downstream tools developers to communicate -- results produced by a
    pipeline can easily and reliably become an input for downstream analyses.
    The object exposes API for interacting with the results and
    pipeline status and can be backed by either a YAML-formatted file
    or a database.
    """

    def __init__(
        self,
        namespace: str = None,
        record_identifier: str = None,
        schema_path: str = None,
        results_file_path: str = None,
        database_only: bool = False,
        config: Union[str, dict] = None,
        status_schema_path: str = None,
        flag_file_dir: str = None,
    ):
        """
        Initialize the object

        :param str namespace: namespace to report into. This will be the DB
        table name if using DB as the object back-end
        :param str record_identifier: record identifier to report for. This
            creates a weak bound to the record, which can be overriden in
            this object method calls
        :param str schema_path: path to the output schema that formalizes
            the results structure
        :param str results_file_path: YAML file to report into, if file is
            used as the object back-end
        :param bool database_only: whether the reported data should not be
            stored in the memory, but only in the database
        :param str | dict config: path to the configuration file or a mapping
            with the config file content
        :param str status_schema_path: path to the status schema that formalizes
            the status flags structure
        """

        def _check_cfg_key(cfg: dict, key: str) -> bool:
            if key not in cfg:
                _LOGGER.warning(f"Key '{key}' not found in config")
                return False
            return True

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
                raise OSError(f"Could not make this path absolute: {path}")
            joined = os.path.join(os.path.dirname(cfg_path), path)
            if os.path.isabs(joined):
                return joined
            raise OSError(f"Could not make this path absolute: {path}")

        def _select_value(
            arg_name: str,
            arg_value: Any,
            cfg: dict,
            strict: bool = True,
            env_var: str = None,
        ) -> Any:
            if arg_value is not None:
                return arg_value
            if arg_name not in cfg or cfg[arg_name] is None:
                if env_var is not None:
                    arg = os.getenv(env_var, None)
                    if arg is not None:
                        _LOGGER.debug(f"Value '{arg}' sourced from '{env_var}' env var")
                        return expandpath(arg)
                if strict:
                    raise PipestatError(
                        f"Value for the required '{arg_name}' argument could not be"
                        f" determined. Provide it in the config or pass to the "
                        f"object constructor."
                    )
                return
            return cfg[arg_name]

        super(PipestatManager, self).__init__()
        self[CONFIG_KEY] = YacAttMap()
        # read config or config data
        config = config or os.getenv(ENV_VARS["config"])
        if config is not None:
            if isinstance(config, str):
                config = os.path.abspath(expandpath(config))
                self[CONFIG_KEY] = YacAttMap(filepath=config)
                self._config_path = config
            elif isinstance(config, dict):
                self[CONFIG_KEY] = YacAttMap(entries=config)
                self._config_path = None
            else:
                raise TypeError(
                    "database_config has to be either path to the "
                    "file to read or a dict"
                )
            # validate config
            cfg = self[CONFIG_KEY].to_dict(expand=True)
            _, cfg_schema = read_yaml_data(CFG_SCHEMA, "config schema")
            validate(cfg, cfg_schema)

        self[NAME_KEY] = _select_value(
            "namespace", namespace, self[CONFIG_KEY], env_var=ENV_VARS["namespace"]
        )
        self[RECORD_ID_KEY] = _select_value(
            "record_identifier",
            record_identifier,
            self[CONFIG_KEY],
            False,
            ENV_VARS["record_identifier"],
        )
        self[DB_ONLY_KEY] = database_only
        # read results schema
        self._schema_path = _select_value(
            "schema_path",
            schema_path,
            self[CONFIG_KEY],
            False,
            env_var=ENV_VARS["schema"],
        )
        if self._schema_path is not None:
            _, self[SCHEMA_KEY] = read_yaml_data(
                _mk_abs_via_cfg(self._schema_path, self.config_path), "schema"
            )
            self.validate_schema()
            # determine the highlighted results
            self[HIGHLIGHTED_KEY] = [
                k
                for k, v in self.schema.items()
                if "highlight" in v and v["highlight"] is True
            ]
            if self[HIGHLIGHTED_KEY]:
                assert isinstance(self[HIGHLIGHTED_KEY], list), TypeError(
                    f"highlighted results specification "
                    f"({self[HIGHLIGHTED_KEY]}) has to be a list"
                )
        # read status schema
        status_schema_path = (
            _mk_abs_via_cfg(
                _select_value(
                    "status_schema_path",
                    status_schema_path,
                    self[CONFIG_KEY],
                    False,
                    env_var=ENV_VARS["status_schema"],
                ),
                self.config_path,
            )
            or STATUS_SCHEMA
        )
        self[STATUS_SCHEMA_SOURCE_KEY], self[STATUS_SCHEMA_KEY] = read_yaml_data(
            status_schema_path, "status schema"
        )
        # determine results file
        results_file_path = _mk_abs_via_cfg(
            _select_value(
                "results_file_path",
                results_file_path,
                self[CONFIG_KEY],
                False,
                ENV_VARS["results_file"],
            ),
            self.config_path,
        )
        if results_file_path:
            if self[DB_ONLY_KEY]:
                raise ValueError(
                    "Running in database only mode does not make "
                    "sense with a YAML file as a backend."
                )
            self[FILE_KEY] = results_file_path
            self._init_results_file()
            flag_file_dir = _select_value(
                "flag_file_dir", flag_file_dir, self[CONFIG_KEY], False
            ) or os.path.dirname(self.file)
            self[STATUS_FILE_DIR] = _mk_abs_via_cfg(flag_file_dir, self.config_path)
        elif CFG_DATABASE_KEY in self[CONFIG_KEY]:
            if not all(
                [
                    _check_cfg_key(self[CONFIG_KEY][CFG_DATABASE_KEY], key)
                    for key in DB_CREDENTIALS
                ]
            ):
                raise MissingConfigDataError(
                    "Must specify all database login " "credentials or result_file_path"
                )
            self[DB_ORMS_KEY] = {}
            self[DB_BASE_KEY] = declarative_base()
            self[DATA_KEY] = YacAttMap()
            self._init_db_table()
            self._init_status_table()
        else:
            raise MissingConfigDataError(
                "Must specify either database login " "credentials or a YAML file path"
            )

    def __str__(self):
        """
        Generate string representation of the object

        :return str: string representation of the object
        """
        res = f"{self.__class__.__name__} ({self.namespace})"
        res += "\nBackend: {}".format(
            f"file ({self.file})" if self.file else "PostgreSQL"
        )
        res += f"\nResults schema source: {self.schema_path}"
        res += f"\nStatus schema source: {self.status_schema_source}"
        res += f"\nRecords count: {self.record_count}"
        if self.highlighted_results:
            res += f"\nHighlighted results: {', '.join(self.highlighted_results)}"
        return res

    @property
    def record_count(self) -> int:
        """
        Number of records reported

        :return int: number of records reported
        """
        return (
            len(self.data[self.namespace])
            if self.file
            else self._count_rows(self.namespace)
        )

    @property
    def highlighted_results(self) -> List[str]:
        """
        Highlighted results

        :return List[str]: a collection of highlighted results
        """
        return self._get_attr(HIGHLIGHTED_KEY) or []

    @property
    def namespace(self) -> str:
        """
        Namespace the object writes the results to

        :return str: namespace the object writes the results to
        """
        return self._get_attr(NAME_KEY)

    @property
    def record_identifier(self) -> str:
        """
        Unique identifier of the record

        :return str: unique identifier of the record
        """
        return self._get_attr(RECORD_ID_KEY)

    @property
    def schema(self) -> Dict:
        """
        Schema mapping

        :return dict: schema that formalizes the results structure
        """
        return self._get_attr(SCHEMA_KEY)

    @property
    def status_schema(self) -> Dict:
        """
        Status schema mapping

        :return dict: schema that formalizes the pipeline status structure
        """
        return self._get_attr(STATUS_SCHEMA_KEY)

    @property
    def status_schema_source(self) -> Dict:
        """
        Status schema source

        :return dict: source of the schema that formalizes
            the pipeline status structure
        """
        return self._get_attr(STATUS_SCHEMA_SOURCE_KEY)

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
        return self._get_attr(RES_SCHEMAS_KEY)

    @property
    def file(self) -> str:
        """
        File path that the object is reporting the results into

        :return str: file path that the object is reporting the results into
        """
        return self._get_attr(FILE_KEY)

    @property
    def data(self) -> YacAttMap:
        """
        Data object

        :return yacman.YacAttMap: the object that stores the reported data
        """
        return self._get_attr(DATA_KEY)

    @property
    def db_url(self) -> str:
        """
        Database URL, generated based on config credentials

        :return str: database URL
        :raise PipestatDatabaseError: if the object is not backed by a database
        """
        if self.file is not None:
            raise PipestatDatabaseError(
                "Can't determine database URL if the object is backed by a file"
            )
        try:
            creds = dict(
                name=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_NAME_KEY],
                user=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_USER_KEY],
                passwd=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_PASSWORD_KEY],
                host=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY],
                port=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_PORT_KEY],
                dialect=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_DIALECT_KEY],
            )
        except (KeyError, AttributeError) as e:
            raise PipestatDatabaseError(
                f"Could not determine database URL. Caught error: {str(e)}"
            )
        parsed_creds = {k: quote_plus(str(v)) for k, v in creds.items()}
        return "{dialect}://{user}:{passwd}@{host}:{port}/{name}".format(**parsed_creds)

    @property
    @contextmanager
    def session(self):
        """
        Provide a transactional scope around a series of query
        operations, no commit afterwards.
        """
        if not self.is_db_connected():
            self.establish_db_connection()
        with self[DB_SESSION_KEY]() as session:
            _LOGGER.debug("Created session")
            yield session
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

    def _create_table_orm(self, table_name: str, schema: Dict[str, Any]):
        """
        Create a table

        :param str table_name: name of the table to create
        :param Dict[str, Any] schema: schema to base table creation on
        """

        def _auto_repr(x: Any) -> str:
            """
            Auto-generated __repr__ fun

            :param Any x: object to generate __repr__ method for
            :return str: string object representation
            """
            attr_strs = [
                f"{k}={str(v)}" for k, v in x.__dict__.items() if not k.startswith("_")
            ]
            return "<{}: {}>".format(x.__class__.__name__, ", ".join(attr_strs))

        tn = table_name or self.namespace
        attr_dict = dict(
            __tablename__=tn,
            id=Column(Integer, primary_key=True),
            record_identifier=Column(String, unique=True),
        )
        for result_id, result_metadata in schema.items():
            col_type = SQL_CLASSES_BY_TYPE[result_metadata[SCHEMA_TYPE_KEY]]
            _LOGGER.debug(f"Adding object: {result_id} of type: {str(col_type)}")
            attr_dict.update({result_id: Column(col_type)})
        attr_dict.update({"__repr__": _auto_repr})
        _LOGGER.debug(f"Creating '{tn}' ORM with args: {attr_dict}")
        self[DB_ORMS_KEY][tn] = type(tn.capitalize(), (self[DB_BASE_KEY],), attr_dict)
        self[DB_BASE_KEY].metadata.create_all(bind=self[DB_ENGINE_KEY])

    def establish_db_connection(self) -> bool:
        """
        Establish DB connection using the config data

        :return bool: whether the connection has been established successfully
        """
        if self.is_db_connected():
            raise PipestatDatabaseError("Connection is already established")

        self[DB_ENGINE_KEY] = create_engine(self.db_url, echo=True)
        self[DB_SESSION_KEY] = sessionmaker(bind=self[DB_ENGINE_KEY])
        return True

    def is_db_connected(self) -> bool:
        """
        Check whether a DB connection has been established

        :return bool: whether the connection has been established
        """
        if self.file is not None:
            raise PipestatDatabaseError(
                f"The {self.__class__.__name__} object is not backed by a database"
            )
        if DB_SESSION_KEY in self and isinstance(self[DB_SESSION_KEY], sessionmaker):
            return True
        return False

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
                assert SCHEMA_TYPE_KEY in v, SchemaError(
                    f"Result '{k}' is missing '{SCHEMA_TYPE_KEY}' key"
                )
                if v[SCHEMA_TYPE_KEY] == "object" and SCHEMA_PROP_KEY in s[k]:
                    _recursively_replace_custom_types(s[k][SCHEMA_PROP_KEY])
                if v[SCHEMA_TYPE_KEY] in CANONICAL_TYPES.keys():
                    s.setdefault(k, {})
                    s[k].setdefault(SCHEMA_PROP_KEY, {})
                    s[k][SCHEMA_PROP_KEY].update(
                        CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]][SCHEMA_PROP_KEY]
                    )
                    s[k].setdefault("required", [])
                    s[k]["required"].extend(
                        CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]]["required"]
                    )
                    s[k][SCHEMA_TYPE_KEY] = CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]][
                        SCHEMA_TYPE_KEY
                    ]
            return s

        schema = deepcopy(self.schema)
        _LOGGER.debug(f"Validating input schema")
        assert isinstance(schema, dict), SchemaError(
            f"The schema has to be a {dict().__class__.__name__}"
        )
        for col_name in RESERVED_COLNAMES:
            assert col_name not in schema.keys(), PipestatError(
                f"'{col_name}' is an identifier reserved by pipestat"
            )
        self[RES_SCHEMAS_KEY] = {}
        schema = _recursively_replace_custom_types(schema)
        self[RES_SCHEMAS_KEY] = schema

    def _init_results_file(self) -> bool:
        """
        Initialize YAML results file if it does not exist.
        Read the data stored in the existing file into the memory otherwise.

        :return bool: whether the file has been created
        """
        if not os.path.exists(self.file):
            _LOGGER.info(f"Initializing results file '{self.file}'")
            data = YacAttMap(entries={self.namespace: "{}"})
            data.write(filepath=self.file)
            data.make_readonly()
            self[DATA_KEY] = data
            return True
        _LOGGER.debug(f"Reading data from '{self.file}'")
        data = YacAttMap(filepath=self.file)
        filtered = list(filter(lambda x: not x.startswith("_"), data.keys()))
        if filtered and self.namespace not in filtered:
            raise PipestatDatabaseError(
                f"'{self.file}' is already used to report results for "
                f"other namespace: {filtered[0]}"
            )
        self[DATA_KEY] = data
        return False

    def _init_db_table(self) -> bool:
        """
        Initialize a database table based on the provided schema,
        if it does not exist. Read the data stored in the database into the
        memory otherwise.

        :return bool: whether the table has been created
        """
        if self.schema is None:
            raise SchemaNotFoundError("initialize the database table")
        if not self.is_db_connected():
            self.establish_db_connection()
        _LOGGER.info(f"Initializing '{self.namespace}' table in '{PKG_NAME}' database")
        self._create_table_orm(table_name=self.namespace, schema=self.result_schemas)
        if not self[DB_ONLY_KEY]:
            self._table_to_dict()
        return True

    def _table_to_dict(self) -> None:
        """
        Create a dictionary from the database table data
        """
        with self.session as s:
            records = s.query(self._get_orm(self.namespace)).all()
        _LOGGER.debug(f"Reading data from database for '{self.namespace}' namespace")
        for record in records:
            record_id = getattr(record, RECORD_ID)
            for column in record.__table__.columns:
                val = getattr(record, column.name, None)
                if val is not None:
                    self._report_data_element(
                        record_identifier=record_id, values={column.name: val}
                    )

    def _init_status_table(self):
        status_table_name = f"{self.namespace}_{STATUS}"
        if not self.is_db_connected():
            self.establish_db_connection()
        # if not self._check_table_exists(table_name=status_table_name):
        _LOGGER.debug(
            f"Initializing '{status_table_name}' table in " f"'{PKG_NAME}' database"
        )
        self._create_table_orm(
            table_name=status_table_name,
            schema=get_status_table_schema(status_schema=self.status_schema),
        )

    def _get_attr(self, attr: str) -> Any:
        """
        Safely get the name of the selected attribute of this object

        :param str attr: attr to select
        :return:
        """
        return self[attr] if attr in self else None

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
        with self.session as s:
            return s.query(self[DB_ORMS_KEY][table_name].id).count()

    def _get_orm(self, table_name: str = None) -> Any:
        """
        Get an object relational mapper class

        :param str table_name: table name to get a class for
        :return Any: Object relational mapper class
        """
        if DB_ORMS_KEY not in self:
            raise PipestatDatabaseError("Object relational mapper classes not defined")
        tn = f"{table_name or self.namespace}"
        if tn not in self[DB_ORMS_KEY]:
            raise PipestatDatabaseError(
                f"No object relational mapper class defined for table: {tn}"
            )
        if not isinstance(self[DB_ORMS_KEY][tn], DeclarativeMeta):
            raise PipestatDatabaseError(
                f"Object relational mapper class for table '{tn}' is invalid"
            )
        return self[DB_ORMS_KEY][tn]

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
            with self.session as s:
                return (
                    s.query(self._get_orm(table_name).id)
                    .filter_by(record_identifier=record_identifier)
                    .first()
                    is not None
                )
        else:
            if (
                self.namespace in self.data
                and record_identifier in self.data[table_name]
            ):
                return True
            return False

    def check_which_results_exist(
        self, results: List[str], rid: str = None
    ) -> List[str]:
        """
        Check which results have been reported

        :param str rid: unique identifier of the record
        :param List[str] results: names of the results to check
        :return List[str]: whether the specified result has been reported for the
            indicated record in current namespace
        """
        rid = self._strict_record_id(rid)
        if self.file is None:
            existing = self._check_which_results_exist_db(results=results, rid=rid)
        else:
            existing = []
            for r in results:
                if (
                    self.namespace in self.data
                    and rid in self.data[self.namespace]
                    and r in self.data[self.namespace][rid]
                ):
                    existing.append(r)
        return existing

    def _check_which_results_exist_db(
        self, results: List[str], rid: str = None, table_name: str = None
    ) -> List[str]:
        """
        Check if the specified results exist in the table

        :param str rid: record to check for
        :param List[str] results: results identifiers to check for
        :param str table_name: name of the table to search for results in
        :return List[str]: results identifiers that exist
        """
        table_name = table_name or self.namespace
        rid = self._strict_record_id(rid)
        with self.session as s:
            record = (
                s.query(self._get_orm(table_name))
                .filter_by(record_identifier=rid)
                .first()
            )
        return [r for r in results if getattr(record, r, None) is not None]

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
        filter_condition: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Any]:
        """
        Perform a SELECT on the table, filtering limited to a single condition

        :param str table_name: name of the table to SELECT from
        :param List[str] columns: columns to include in the result
        :param [(key,operator,value)] filter_condition: e.g. [("id", "eq", 1)] operator list
            - eq for ==
            - lt for <
            - ge for >=
            - in for in_
            - like for like
        :param int offset: skip this number of rows
        :param int limit: include this number of rows
        """

        def _dynamic_filter(
            ORM: sqlalchemy.orm.DeclarativeMeta,
            query: sqlalchemy.orm.Query,
            filter_condition: List[Tuple[str, str, Union[str, List[str]]]],
        ):
            """
            Return filtered query based on condition.

            :param sqlalchemy.orm.DeclarativeMeta ORM:
            :param sqlalchemy.orm.Query query: takes query
            :param [(key,operator,value)] filter_condition: e.g. [("id", "eq", 1)] operator list
                - eq for ==
                - lt for <
                - ge for >=
                - in for in_
                - like for like
            :return: query
            """
            for raw in filter_condition:
                try:
                    key, op, value = raw
                except ValueError:
                    raise Exception("Invalid filter: %s" % raw)
                column = getattr(ORM, key, None)
                if column is None:
                    raise Exception("Invalid filter column: %s" % key)
                if op == "in":
                    if isinstance(value, list):
                        filt = column.in_(value)
                    else:
                        filt = column.in_(value.split(","))
                else:
                    try:
                        attr = (
                            list(
                                filter(
                                    lambda e: hasattr(column, e % op),
                                    ["%s", "%s_", "__%s__"],
                                )
                            )[0]
                            % op
                        )
                    except IndexError:
                        raise Exception(f"Invalid filter operator: {op}")
                    if value == "null":
                        value = None
                    filt = getattr(column, attr)(value)
                query = query.filter(filt)
            return query

        ORM = self._get_orm(table_name or self.namespace)
        with self.session as s:
            if columns is not None:
                query = s.query(*[getattr(ORM, column) for column in columns])
            else:
                query = s.query(ORM)
            if filter_condition is not None:
                query = _dynamic_filter(
                    ORM=ORM, query=query, filter_condition=filter_condition
                )
            if isinstance(offset, int):
                query = query.offset(offset)
            if isinstance(limit, int):
                query = query.limit(limit)
            result = query.all()
        return result

    def retrieve(
        self, record_identifier: str = None, result_identifier: str = None
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
                return self.data[self.namespace][r_id].to_dict()
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
            )
            if not existing:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record "
                    f"'{record_identifier}'"
                )

        with self.session as s:
            record = (
                s.query(self._get_orm(table_name))
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
            _LOGGER.warning(
                f"These results exist for '{record_identifier}': {existing}"
            )
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing}")
        for r in result_identifiers:
            validate_type(
                value=values[r], schema=self.result_schemas[r], strict_type=strict_type
            )
        if self.file is not None:
            self.data.make_writable()
        if not self[DB_ONLY_KEY]:
            self._report_data_element(
                record_identifier=record_identifier, values=values
            )
        if self.file is not None:
            self.data.write()
            self.data.make_readonly()
        else:
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
        rep_strs = [f"{k}: {v}" for k, v in values.items()]
        _LOGGER.info(
            f"Reported records for '{record_identifier}' in '{self.namespace}' "
            f"namespace:{nl} - {(nl + ' - ').join(rep_strs)}"
        )
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
        ORMClass = self._get_orm(table_name)
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
        self[DATA_KEY].setdefault(self.namespace, PXAM())
        self[DATA_KEY][self.namespace].setdefault(record_identifier, PXAM())
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
        if self.file:
            self.data.make_writable()
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
            self.data.write()
            self.data.make_readonly()
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
        ORMClass = self._get_orm(table_name=table_name)
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
