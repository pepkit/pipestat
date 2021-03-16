from contextlib import contextmanager
from copy import deepcopy
from logging import getLogger
from typing import Any, Dict, List, Optional, Union

import psycopg2
from attmap import PathExAttMap as PXAM
from jsonschema import validate
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor, Json
from ubiquerg import create_lock, remove_lock
from yacman import YacAttMap

from .const import *
from .exceptions import *
from .helpers import *

_LOGGER = getLogger(PKG_NAME)


class LoggingCursor(psycopg2.extras.DictCursor):
    """
    Logging db cursor
    """

    def execute(self, query, vars=None):
        """
        Execute a database operation (query or command) and issue a debug
        and info level log messages

        :param query:
        :param vars:
        :return:
        """
        _LOGGER.debug(f"Executing query: {self.mogrify(query, vars)}")
        super(LoggingCursor, self).execute(query=query, vars=vars)
        _LOGGER.debug(f"Executed query: {self.query}")


class PipestatManager(dict):
    """
    Pipestat standardizes reporting of pipeline results and
    pipeline status management. It formalizes a way for pipeline developers
    and downstream tools developers to communicate -- results produced by a
    pipeline can easily and reliably become an input for downstream analyses.
    The object exposes API for interacting with the results and
    pipeline status and can be backed by either a YAML-formatted file
    or a PostgreSQL database.
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
            self[DATA_KEY] = YacAttMap()
            self._init_postgres_table()
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
    def highlighted_results(self) -> List[str]:
        """
        Highlighted results

        :return List[str]: a collection of highlighted results
        """
        return self._get_attr(HIGHLIGHTED_KEY) or []

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
    def db_cursor(self):
        """
        Establish connection and get a PostgreSQL database cursor,
        commit and close the connection afterwards

        :return LoggingCursor: Database cursor object
        """
        try:
            if not self.check_connection():
                self.establish_postgres_connection()
            with self[DB_CONNECTION_KEY] as c, c.cursor(
                cursor_factory=LoggingCursor
            ) as cur:
                yield cur
        except Exception:
            raise
        finally:
            self.close_postgres_connection()

    def get_status(self, record_identifier: str = None) -> Optional[str]:
        """
        Get the current pipeline status

        :return str: status identifier, like 'running'
        """
        r_id = self._strict_record_id(record_identifier)
        if self.file is None:
            with self.db_cursor as cur:
                query = sql.SQL(
                    f"SELECT {STATUS} "
                    f"FROM {f'{self.namespace}_{STATUS}'} "
                    f"WHERE {RECORD_ID}=%s"
                )
                cur.execute(query, (r_id,))
                result = cur.fetchone()
            return result[0] if result is not None else None
        else:
            flag_file = self._get_flag_file(record_identifier=r_id)
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

    def _get_attr(self, attr: str) -> Any:
        """
        Safely get the name of the selected attribute of this object

        :param str attr: attr to select
        :return:
        """
        return self[attr] if attr in self else None

    def _table_to_dict(self) -> None:
        """
        Create a dictionary from the database table data
        """
        with self.db_cursor as cur:
            cur.execute(f"SELECT * FROM {self.namespace}")
            data = cur.fetchall()
        _LOGGER.debug(f"Reading data from database for '{self.namespace}' namespace")
        for record in data:
            record_id = record[RECORD_ID]
            for res_id, val in record.items():
                if val is not None:
                    self._report_data_element(
                        record_identifier=record_id, values={res_id: val}
                    )

    def _init_postgres_table(self) -> bool:
        """
        Initialize a PostgreSQL table based on the provided schema,
        if it does not exist. Read the data stored in the database into the
        memory otherwise.

        :return bool: whether the table has been created
        """
        if self.schema is None:
            raise SchemaNotFoundError("initialize the database table")
        if self._check_table_exists(table_name=self.namespace):
            _LOGGER.debug(f"Table '{self.namespace}' already exists in the database")
            if not self[DB_ONLY_KEY]:
                self._table_to_dict()
            return False
        _LOGGER.info(f"Initializing '{self.namespace}' table in '{PKG_NAME}' database")
        columns = FIXED_COLUMNS + schema_to_columns(schema=self.schema)
        self._create_table(table_name=self.namespace, columns=columns)
        return True

    # def _create_status_type(self):
    #     with self.db_cursor as cur:
    #         s = sql.SQL(f"SELECT exists (SELECT 1 FROM pg_type WHERE typname = '{STATUS}');")
    #         cur.execute(s)
    #         if cur.fetchone()[0]:
    #             return
    #     with self.db_cursor as cur:
    #         status_strs = [f"'{st_id}'" for st_id in self.status_schema.keys()]
    #         status_str = ", ".join(status_strs)
    #         s = sql.SQL(f"CREATE TYPE {STATUS} as enum({status_str});")
    #         cur.execute(s)

    def _init_status_table(self):
        status_table_name = f"{self.namespace}_{STATUS}"
        # self._create_status_type()
        if not self._check_table_exists(table_name=status_table_name):
            _LOGGER.info(
                f"Initializing '{status_table_name}' table in " f"'{PKG_NAME}' database"
            )
            self._create_table(status_table_name, STATUS_TABLE_COLUMNS)

    def _create_table(self, table_name: str, columns: List[str]):
        """
        Create a table

        :param str table_name: name of the table to create
        :param str | List[str] columns: columns definition list,
            for instance: ['name VARCHAR(50) NOT NULL']
        """
        columns = mk_list_of_str(columns)
        with self.db_cursor as cur:
            s = sql.SQL(f"CREATE TABLE {table_name} ({','.join(columns)})")
            cur.execute(s)

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

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if the specified table exists

        :param str table_name: table name to be checked
        :return bool: whether the specified table exists
        """
        with self.db_cursor as cur:
            cur.execute(
                "SELECT EXISTS(SELECT * FROM information_schema.tables "
                "WHERE table_name=%s)",
                (table_name,),
            )
            return cur.fetchone()[0]

    def _check_record(
        self, condition_col: str, condition_val: str, table_name: str
    ) -> bool:
        """
        Check if the record matching the condition is in the table

        :param str condition_col: column to base the check on
        :param str condition_val: value in the selected column
        :param str table_name: name of the table ot check the record in
        :return bool: whether any record matches the provided condition
        """
        with self.db_cursor as cur:
            statement = (
                f"SELECT EXISTS(SELECT 1 from {table_name} "
                f"WHERE {condition_col}=%s)"
            )
            cur.execute(statement, (condition_val,))
            return cur.fetchone()[0]

    def _count_rows(self, table_name: str) -> int:
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: number of rows in the selected table
        """
        with self.db_cursor as cur:
            statement = sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            )
            cur.execute(statement)
            return cur.fetchall()[0][0]

    def _report_postgres(
        self, value: Dict[str, Any], record_identifier: str, table_name: str = None
    ) -> int:
        """
        Check if record with this record identifier in table, create new record
         if not (INSERT), update the record if yes (UPDATE).

        Currently supports just one column at a time.

        :param str record_identifier: unique identifier of the record, value to
            in 'record_identifier' column to look for to determine if the record
            already exists in the table
        :param dict value: a mapping of pair of table column names and
            respective values to be inserted to the database
        :return int: id of the row just inserted
        """
        table_name = table_name or self.namespace
        if not self._check_record(
            condition_col=RECORD_ID,
            condition_val=record_identifier,
            table_name=table_name,
        ):
            with self.db_cursor as cur:
                cur.execute(
                    f"INSERT INTO {table_name} ({RECORD_ID}) VALUES (%s)",
                    (record_identifier,),
                )
        # prep a list of SQL objects with column-named value placeholders
        columns = sql.SQL(",").join(
            [
                sql.SQL("{}=%({})s").format(sql.Identifier(k), sql.SQL(k))
                for k in list(value.keys())
            ]
        )
        # construct the query template to execute
        query = sql.SQL("UPDATE {n} SET {c} WHERE {id}=%({id})s RETURNING id").format(
            n=sql.Identifier(table_name), c=columns, id=sql.SQL(RECORD_ID)
        )
        # preprocess the values, dict -> Json
        values = {k: Json(v) if isinstance(v, dict) else v for k, v in value.items()}
        # add record_identifier column, which is specified outside of values
        values.update({RECORD_ID: record_identifier})
        with self.db_cursor as cur:
            cur.execute(query, values)
            return cur.fetchone()[0]

    def clear_status(
        self, record_identifier: str = None, flag_names: List[str] = None
    ) -> List[str]:
        """
        Remove status flags

        :param str record_identifier: name of the record to remove flags for
        :param Iterable[str] flag_names: Names of flags to remove, optional; if
            unspecified, all schema-defined flag names will be used.
        :return List[str]: Collection of names of flags removed
        """
        r_id = self._strict_record_id(record_identifier)
        if self.file is not None:
            flag_names = flag_names or list(self.status_schema.keys())
            if isinstance(flag_names, str):
                flag_names = [flag_names]
            removed = []
            for f in flag_names:
                path_flag_file = self.get_status_flag_path(
                    status_identifier=f, record_identifier=r_id
                )
                try:
                    os.remove(path_flag_file)
                except:
                    pass
                else:
                    _LOGGER.info(f"Removed existing flag: {path_flag_file}")
                    removed.append(f)
            return removed
        else:
            removed = self.get_status(r_id)
            status_table_name = f"{self.namespace}_{STATUS}"
            with self.db_cursor as cur:
                try:
                    cur.execute(
                        f"DELETE FROM {status_table_name} WHERE "
                        f"{RECORD_ID}='{r_id}'"
                    )
                except Exception as e:
                    _LOGGER.error(
                        f"Could not remove the status from the "
                        f"database. Exception: {e}"
                    )
                    return []
                else:
                    return [removed]

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
            if prev_status:
                prev_flag_path = self.get_status_flag_path(prev_status, r_id)
                os.remove(prev_flag_path)
            flag_path = self.get_status_flag_path(status_identifier, r_id)
            create_lock(flag_path)
            with open(flag_path, "w") as f:
                f.write(status_identifier)
            remove_lock(flag_path)
        else:
            try:
                self._report_postgres(
                    value={STATUS: status_identifier},
                    record_identifier=r_id,
                    table_name=f"{self.namespace}_{STATUS}",
                )
            except Exception as e:
                _LOGGER.error(
                    f"Could not insert into the status table. " f"Exception: {e}"
                )
                raise
        if prev_status:
            _LOGGER.debug(
                f"Changed status from '{prev_status}' to '{status_identifier}'"
            )

    def check_result_exists(self, result_identifier, record_identifier=None):
        """
        Check if the result has been reported

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to check
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        record_identifier = self._strict_record_id(record_identifier)
        return self._check_which_results_exist(
            results=[result_identifier], rid=record_identifier
        )

    def _check_which_results_exist(
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
        existing = []
        for r in results:
            if not self[DB_ONLY_KEY]:
                if (
                    self.namespace in self.data
                    and rid in self.data[self.namespace]
                    and r in self.data[self.namespace][rid]
                ):
                    existing.append(r)
            else:
                with self.db_cursor as cur:
                    try:
                        cur.execute(
                            f"SELECT {r} FROM {self.namespace} WHERE {RECORD_ID}=%s",
                            (rid,),
                        )
                    except Exception:
                        continue
                    else:
                        res = cur.fetchone()
                        if res is not None and res[0] is not None:
                            existing.append(r)
        return existing

    def check_record_exists(self, record_identifier: str = None) -> bool:
        """
        Check if the record exists

        :param str record_identifier: unique identifier of the record
        :return bool: whether the record exists
        """
        record_identifier = self._strict_record_id(record_identifier)
        if self[DB_ONLY_KEY]:
            with self.db_cursor as cur:
                cur.execute(
                    f"SELECT exists(SELECT 1 from {self.namespace} "
                    f"WHERE {RECORD_ID}=%s)",
                    (record_identifier,),
                )
                return cur.fetchone()
        if (
            self.namespace in self.data
            and record_identifier in self.data[self.namespace]
        ):
            return True
        return False

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
        updated_ids = False
        if self.schema is None:
            raise SchemaNotFoundError("report results")
        result_identifiers = list(values.keys())
        self.assert_results_defined(results=result_identifiers)
        existing = self._check_which_results_exist(
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
                updated_ids = self._report_postgres(
                    record_identifier=record_identifier, value=values
                )
            except Exception as e:
                _LOGGER.error(
                    f"Could not insert the result into the database. " f"Exception: {e}"
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

    def _report_data_element(
        self, record_identifier: str, values: Dict[str, Any]
    ) -> None:
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param Dict[str,Any] values: dict of results identifiers and values
            to be reported
        """
        self[DATA_KEY].setdefault(self.namespace, PXAM())
        self[DATA_KEY][self.namespace].setdefault(record_identifier, PXAM())
        for res_id, val in values.items():
            self[DATA_KEY][self.namespace][record_identifier][res_id] = val

    def select(
        self,
        columns: Union[str, List[str]] = None,
        condition: str = None,
        condition_val: str = None,
        offset: int = None,
        limit: int = None,
    ) -> List[psycopg2.extras.DictRow]:
        """
        Get all the contents from the selected table, possibly restricted by
        the provided condition.

        :param str | List[str] columns: columns to select
        :param str condition: condition to restrict the results
            with, will be appended to the end of the SELECT statement and
            safely populated with 'condition_val',
            for example: `"id=%s"`
        :param list condition_val: values to fill the placeholder
            in 'condition' with
        :param int offset: number of records to be skipped
        :param int limit: max number of records to be returned
        :return List[psycopg2.extras.DictRow]: all table contents
        """
        if self.file:
            raise NotImplementedError(
                "Selection is not supported on objects backed by results files."
                " Use 'retrieve' method instead."
            )
        condition, condition_val = preprocess_condition_pair(condition, condition_val)
        if not columns:
            columns = sql.SQL("*")
        else:
            columns = sql.SQL(",").join(
                [sql.Identifier(x) for x in mk_list_of_str(columns)]
            )
        statement = sql.SQL("SELECT {} FROM {}").format(
            columns, sql.Identifier(self.namespace)
        )
        if condition:
            statement += sql.SQL(" WHERE ")
            statement += condition
        statement = paginate_query(statement, offset, limit)
        with self.db_cursor as cur:
            cur.execute(query=statement, vars=condition_val)
            result = cur.fetchall()
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
        record_identifier = self._strict_record_id(record_identifier)
        if self[DB_ONLY_KEY]:
            if result_identifier is not None:
                existing = self._check_which_results_exist(
                    results=[result_identifier], rid=record_identifier
                )
                if not existing:
                    raise PipestatDatabaseError(
                        f"Result '{result_identifier}' not found for record "
                        f"'{record_identifier}'"
                    )
            with self.db_cursor as cur:
                query = sql.SQL(
                    f"SELECT {result_identifier or '*'} "
                    f"FROM {self.namespace} WHERE {RECORD_ID}=%s"
                )
                cur.execute(query, (record_identifier,))
                result = cur.fetchall()
            if len(result) > 0:
                if result_identifier is None:
                    return {k: v for k, v in dict(result[0]).items() if v is not None}
                return dict(result[0])[result_identifier]
            raise PipestatDatabaseError(f"Record '{record_identifier}' not found")
        else:
            if record_identifier not in self.data[self.namespace]:
                raise PipestatDatabaseError(f"Record '{record_identifier}' not found")
            if result_identifier is None:
                return self.data[self.namespace][record_identifier].to_dict()
            if result_identifier not in self.data[self.namespace][record_identifier]:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record "
                    f"'{record_identifier}'"
                )
            return self.data[self.namespace][record_identifier][result_identifier]

    def remove(
        self, record_identifier: str = None, result_identifier: str = None
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
        record_identifier = self._strict_record_id(record_identifier)
        rm_record = True if result_identifier is None else False
        if not self.check_record_exists(record_identifier):
            _LOGGER.error(f"Record '{record_identifier}' not found")
            return False
        if result_identifier and not self.check_result_exists(
            result_identifier, record_identifier
        ):
            _LOGGER.error(
                f"'{result_identifier}' has not been reported for "
                f"'{record_identifier}'"
            )
            return False
        if self.file:
            self.data.make_writable()
        if not self[DB_ONLY_KEY]:
            if rm_record:
                _LOGGER.info(f"Removing '{record_identifier}' record")
                del self[DATA_KEY][self.namespace][record_identifier]
            else:
                val_backup = self[DATA_KEY][self.namespace][record_identifier][
                    result_identifier
                ]
                del self[DATA_KEY][self.namespace][record_identifier][result_identifier]
                _LOGGER.info(
                    f"Removed result '{result_identifier}' for record "
                    f"'{record_identifier}' from '{self.namespace}' namespace"
                )
                if not self[DATA_KEY][self.namespace][record_identifier]:
                    _LOGGER.info(
                        f"Last result removed for '{record_identifier}'. "
                        f"Removing the record"
                    )
                    del self[DATA_KEY][self.namespace][record_identifier]
                    rm_record = True
        if self.file:
            self.data.write()
            self.data.make_readonly()
        if self.file is None:
            if rm_record:
                try:
                    with self.db_cursor as cur:
                        cur.execute(
                            f"DELETE FROM {self.namespace} WHERE "
                            f"{RECORD_ID}='{record_identifier}'"
                        )
                except Exception as e:
                    _LOGGER.error(
                        f"Could not remove the result from the "
                        f"database. Exception: {e}"
                    )
                    self[DATA_KEY][self.namespace].setdefault(record_identifier, PXAM())
                    raise
                return True
            try:
                with self.db_cursor as cur:
                    cur.execute(
                        f"UPDATE {self.namespace} SET {result_identifier}=null "
                        f"WHERE {RECORD_ID}='{record_identifier}'"
                    )
            except Exception as e:
                _LOGGER.error(
                    f"Could not remove the result from the database. " f"Exception: {e}"
                )
                if not self[DB_ONLY_KEY]:
                    self[DATA_KEY][self.namespace][record_identifier][
                        result_identifier
                    ] = val_backup
                raise
        return True

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

    def check_connection(self) -> bool:
        """
        Check whether a PostgreSQL connection has been established

        :return bool: whether the connection has been established
        """
        if self.file is not None:
            raise PipestatDatabaseError(
                f"The {self.__class__.__name__} object " f"is not backed by a database"
            )
        if DB_CONNECTION_KEY in self and isinstance(
            self[DB_CONNECTION_KEY], psycopg2.extensions.connection
        ):
            return True
        return False

    def establish_postgres_connection(self, suppress: bool = False) -> bool:
        """
        Establish PostgreSQL connection using the config data

        :param bool suppress: whether to suppress any connection errors
        :return bool: whether the connection has been established successfully
        """
        if self.check_connection():
            raise PipestatDatabaseError(
                f"Connection is already established: "
                f"{self[DB_CONNECTION_KEY].info.host}"
            )
        try:
            self[DB_CONNECTION_KEY] = psycopg2.connect(
                dbname=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_NAME_KEY],
                user=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_USER_KEY],
                password=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_PASSWORD_KEY],
                host=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY],
                port=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_PORT_KEY],
            )
        except psycopg2.Error as e:
            _LOGGER.error(
                f"Could not connect to: "
                f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}"
            )
            _LOGGER.info(f"Caught error: {e}")
            if suppress:
                return False
            raise
        else:
            _LOGGER.debug(
                f"Established connection with PostgreSQL: "
                f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}"
            )
            return True

    def close_postgres_connection(self) -> None:
        """
        Close connection and remove client bound
        """
        if not self.check_connection():
            raise PipestatDatabaseError(
                f"The connection has not been established: "
                f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}"
            )
        self[DB_CONNECTION_KEY].close()
        del self[DB_CONNECTION_KEY]
        _LOGGER.debug(
            f"Closed connection with PostgreSQL: "
            f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}"
        )

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
