from contextlib import contextmanager
from copy import deepcopy
from logging import getLogger
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote_plus

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


class PipestatManagerORM(dict):
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

        super(PipestatManagerORM, self).__init__()
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
    @contextmanager
    def session(self):
        """
        Provide a transactional scope around a series of query
        operations, no commit afterwards.
        """
        if not self.is_db_connected():
            self.establish_db_connection_orm()
        with self[DB_SESSION_KEY]() as session:
            _LOGGER.debug("Created session")
            yield session
            _LOGGER.debug("Ending session")

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

    def establish_db_connection_orm(self) -> bool:
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

    @property
    def db_url(self) -> str:
        """
        Database URL, generated based on config credentials

        :return str: database URL
        """
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
            self.establish_db_connection_orm()
        # if self._check_table_exists(table_name=self.namespace):
        #     _LOGGER.debug(f"Table '{self.namespace}' already exists in the database")
        #     if not self[DB_ONLY_KEY]:
        #         self._table_to_dict()
        #     # return False
        _LOGGER.info(f"Initializing '{self.namespace}' table in '{PKG_NAME}' database")
        self._create_table_orm(table_name=self.namespace, schema=self.result_schemas)
        return True

    def _init_status_table(self):
        status_table_name = f"{self.namespace}_{STATUS}"
        if not self.is_db_connected():
            self.establish_db_connection_orm()
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

        :param ste record_identifier: record to check for
        :param str table_name: table name to check
        :return bool: whether the record exists in the table
        """
        with self.session as s:
            return (
                s.query(self._get_orm(table_name).id)
                .filter_by(record_identifier=record_identifier)
                .first()
                is not None
            )

    def _report(
        self, value: Dict[str, Any], record_identifier: str, table_name: str = None
    ) -> int:
        """


        :param value:
        :param record_identifier:
        :param table_name:
        :return:
        """
        ORMClass = self._get_orm(table_name)
        value.update({RECORD_ID: record_identifier})
        if not self.check_record_exists(
            record_identifier=record_identifier, table_name=table_name
        ):
            x = ORMClass(**value)
            with self.session as s:
                s.add(x)
                s.commit()
        else:
            with self.session as s:
                s.query(ORMClass).filter(
                    getattr(ORMClass, RECORD_ID) == record_identifier
                ).update(value)
                s.commit()
