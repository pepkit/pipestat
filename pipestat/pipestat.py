import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, Json
from psycopg2.extensions import connection
from logging import getLogger
from contextlib import contextmanager
from copy import deepcopy

import sys
import logmuse
from attmap import PathExAttMap as PXAM
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
    Pipestat standardizes reporting of pipeline results. It formalizes a way
    for pipeline developers and downstream tools developers to communicate --
    results produced by a pipeline can easily and reliably become an input for
    downstream analyses. The object exposes API for interacting with the results
    can be backed by either a YAML-formatted file or a PostgreSQL database.
    """
    def __init__(self, namespace=None, record_identifier=None, schema_path=None,
                 results_file_path=None, database_only=False, config=None):
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
        """
        def _check_cfg_key(cfg, key):
            if key not in cfg:
                _LOGGER.warning(f"Key '{key}' not found in config")
                return False
            return True

        def _mk_abs_via_cfg(path, cfg_path):
            if path is None:
                return
            path = expandpath(path)
            if os.path.isabs(path):
                return path
            if cfg_path is None:
                raise OSError(f"Could not make this path absolute: {path}")
            joined = os.path.join(os.path.dirname(cfg_path), path)
            if os.path.isabs(joined):
                return joined
            raise OSError(f"Could not make this path absolute: {path}")

        def _select_value(arg_name, arg_value, cfg, strict=True):
            if arg_value is not None:
                return arg_value
            if arg_name not in cfg or cfg[arg_name] is None:
                if strict:
                    raise PipestatError(
                        f"Value for the required '{arg_name}' argument could not be"
                        f" determined. Provide it in the config or pass to the "
                        f"object constructor.")
                return
            return cfg[arg_name]

        super(PipestatManager, self).__init__()
        self[CONFIG_KEY] = YacAttMap()
        if config is not None:
            if isinstance(config, str):
                config = os.path.abspath(expandpath(config))
                self[CONFIG_KEY] = YacAttMap(filepath=config)
            elif isinstance(config, dict):
                self[CONFIG_KEY] = YacAttMap(entries=config)
            else:
                raise TypeError("database_config has to be either path to the "
                                "file to read or a dict")

        self[NAME_KEY] = _select_value("name", namespace, self[CONFIG_KEY])
        self[RECORD_ID_KEY] = _select_value(
            "record_identifier", record_identifier, self[CONFIG_KEY], False)
        self[DB_ONLY_KEY] = database_only
        schema_path = _mk_abs_via_cfg(_select_value(
            "schema_path", schema_path, self[CONFIG_KEY]), config)
        _, self[SCHEMA_KEY] = read_yaml_data(schema_path, "schema")
        self.validate_schema()
        self._schema_path = schema_path
        results_file_path = _mk_abs_via_cfg(_select_value(
                "results_file_path", results_file_path, self[CONFIG_KEY], False), config)
        if results_file_path:
            if self[DB_ONLY_KEY]:
                raise ValueError("Running in database only mode does not make "
                                 "sense with a YAML file as a backend.")
            self[FILE_KEY] = results_file_path
            self._init_results_file()
        elif CFG_DATABASE_KEY in self[CONFIG_KEY]:
            if not all([_check_cfg_key(self[CONFIG_KEY][CFG_DATABASE_KEY], key)
                        for key in DB_CREDENTIALS]):
                raise MissingConfigDataError("Must specify all database login "
                                             "credentials or result_file_path")
            self[DATA_KEY] = YacAttMap()
            self._init_postgres_table()
        else:
            raise MissingConfigDataError("Must specify either database login "
                                         "credentials or a YAML file path")

    def __str__(self):
        """
        Generate string representation of the object

        :return str: string representation of the object
        """
        res = f"{self.__class__.__name__} ({self.namespace})"
        res += "\nBackend: {}".format(
            f"file ({self.file})" if self.file else "PostgreSQL")
        res += "\nSchema source: {}".format(self.schema_path)
        res += f"\nRecords count: {self.record_count}"
        return res

    @property
    def record_count(self):
        """
        Number of records reported

        :return int: number of records reported
        """
        return len(self.data[self.namespace]) if self.file \
            else self._count_rows(self.namespace)

    @property
    def namespace(self):
        """
        Namespace the object writes the results to

        :return str: namespace the object writes the results to
        """
        return self._get_attr(NAME_KEY)

    @property
    def record_identifier(self):
        """
        Unique identifier of the record

        :return str: unique identifier of the record
        """
        return self._get_attr(RECORD_ID_KEY)

    @property
    def schema(self):
        """
        Schema mapping

        :return dict: schema that formalizes the results structure
        """
        return self._get_attr(SCHEMA_KEY)

    @property
    def schema_path(self):
        """
        Schema path

        :return str: path to the provided schema
        """
        return self._schema_path

    @property
    def result_schemas(self):
        """
        Result schema mappings

        :return dict: schemas that formalize the structure of each result
            in a canonical jsonschema way
        """
        return self._get_attr(RES_SCHEMAS_KEY)

    @property
    def file(self):
        """
        File path that the object is reporting the results into

        :return str: file path that the object is reporting the results into
        """
        return self._get_attr(FILE_KEY)

    @property
    def data(self):
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
            with self[DB_CONNECTION_KEY] as c, \
                    c.cursor(cursor_factory=LoggingCursor) as cur:
                yield cur
        except Exception:
            raise
        finally:
            self.close_postgres_connection()

    def _get_attr(self, attr):
        """
        Safely get the name of the selected attribute of this object

        :param str attr: attr to select
        :return:
        """
        return self[attr] if attr in self else None

    def _table_to_dict(self):
        """
        Create a dictionary from the database table data

        :return dict: database table data in a dict form
        """
        with self.db_cursor as cur:
            cur.execute(f"SELECT * FROM {self.namespace}")
            data = cur.fetchall()
        _LOGGER.info(f"Reading data from database for '{self.namespace}' namespace")
        for record in data:
            record_id = record[RECORD_ID]
            for res_id, val in record.items():
                if val is not None:
                    self._report_data_element(
                        record_identifier=record_id,
                        values={res_id: val}
                    )

    def _init_postgres_table(self):
        """
        Initialize postgreSQL table based on the provided schema,
        if it does not exist. Read the data stored in the database into the
        memory otherwise.

        :return bool: whether the table has been created
        """
        if self.schema is None:
            raise SchemaNotFoundError("initialize the database table")
        if self._check_table_exists(table_name=self.namespace):
            _LOGGER.debug(
                f"Table '{self.namespace}' already exists in the database")
            if not self[DB_ONLY_KEY]:
                self._table_to_dict()
            return False
        _LOGGER.info(
            f"Initializing '{self.namespace}' table in '{PKG_NAME}' database")
        columns = FIXED_COLUMNS + schema_to_columns(schema=self.schema)
        self._create_table(table_name=self.namespace, columns=columns)
        return True

    def _create_table(self, table_name, columns):
        """
        Create a table

        :param str table_name: name of the table to create
        :param str | list[str] columns: columns definition list,
            for instance: ['name VARCHAR(50) NOT NULL']
        """
        columns = mk_list_of_str(columns)
        with self.db_cursor as cur:
            s = sql.SQL(f"CREATE TABLE {table_name} ({','.join(columns)})")
            cur.execute(s)

    def _init_results_file(self):
        """
        Initialize YAML results file if it does not exist.
        Read the data stored in the existing file into the memory otherwise.

        :return bool: whether the file has been created
        """
        if not os.path.exists(self.file):
            _LOGGER.info(f"Initializing results file '{self.file}'")
            data = YacAttMap(entries={self.namespace: '{}'})
            data.write(filepath=self.file)
            data.make_readonly()
            self[DATA_KEY] = data
            return True
        _LOGGER.info(f"Reading data from '{self.file}'")
        data = YacAttMap(filepath=self.file)
        filtered = list(filter(lambda x: not x.startswith("_"), data.keys()))
        if filtered and self.namespace not in filtered:
            raise PipestatDatabaseError(
                f"'{self.file}' is already used to report results for "
                f"other namespace: {filtered[0]}")
        self[DATA_KEY] = data
        return False

    def _check_table_exists(self, table_name):
        """
        Check if the specified table exists

        :param str table_name: table name to be checked
        :return bool: whether the specified table exists
        """
        with self.db_cursor as cur:
            cur.execute(
                "SELECT EXISTS(SELECT * FROM information_schema.tables "
                "WHERE table_name=%s)",
                (table_name, )
            )
            return cur.fetchone()[0]

    def _check_record(self, condition_col, condition_val):
        """
        Check if the record matching the condition is in the table

        :param str condition_col: column to base the check on
        :param str condition_val: value in the selected column
        :return bool: whether any record matches the provided condition
        """
        with self.db_cursor as cur:
            statement = f"SELECT EXISTS(SELECT 1 from {self.namespace} " \
                        f"WHERE {condition_col}=%s)"
            cur.execute(statement, (condition_val, ))
            return cur.fetchone()[0]

    def _count_rows(self, table_name):
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: number of rows in the selected table
        """
        with self.db_cursor as cur:
            statement = sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name))
            cur.execute(statement)
            return cur.fetchall()[0][0]

    def _report_postgres(self, value, record_identifier):
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
        if not self._check_record(condition_col=RECORD_ID,
                                  condition_val=record_identifier):
            with self.db_cursor as cur:
                cur.execute(
                    f"INSERT INTO {self.namespace} ({RECORD_ID}) VALUES (%s)",
                    (record_identifier, )
                )
        # prep a list of SQL objects with column-named value placeholders
        columns = sql.SQL(",").join([sql.SQL("{}=%({})s").format(
            sql.Identifier(k), sql.SQL(k)) for k in list(value.keys())])
        # construct the query template to execute
        query = sql.SQL("UPDATE {n} SET {c} WHERE {id}=%({id})s RETURNING id").\
            format(
            n=sql.Identifier(self.namespace),
            c=columns,
            id=sql.SQL(RECORD_ID)
        )
        # preprocess the values, dict -> Json
        values = {k: Json(v) if isinstance(v, dict) else v
                  for k, v in value.items()}
        # add record_identifier column, which is specified outside of values
        values.update({RECORD_ID: record_identifier})
        with self.db_cursor as cur:
            cur.execute(query, values)
            return cur.fetchone()[0]

    def check_result_exists(self, result_identifier,  record_identifier=None):
        """
        Check if the result has been reported

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to check
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        record_identifier = self._strict_record_id(record_identifier)
        return self._check_which_results_exist(
            results=[result_identifier], rid=record_identifier)

    def _check_which_results_exist(self, results, rid=None):
        """
        Check which results have been reported

        :param str rid: unique identifier of the record
        :param list[str] results: names of the results to check
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        rid = self._strict_record_id(rid)
        existing = []
        for r in results:
            if not self[DB_ONLY_KEY]:
                if self.namespace in self.data and rid in self.data[self.namespace] \
                        and r in self.data[self.namespace][rid]:
                    existing.append(r)
            else:
                with self.db_cursor as cur:
                    try:
                        cur.execute(
                            f"SELECT {r} FROM {self.namespace} WHERE {RECORD_ID}=%s",
                            (rid, )
                        )
                    except Exception:
                        continue
                    else:
                        res = cur.fetchone()
                        if res is not None and res[0] is not None:
                            existing.append(r)
        return existing

    def check_record_exists(self, record_identifier=None):
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
                    (record_identifier, )
                )
                return cur.fetchone()
        if self.namespace in self.data and record_identifier in self.data[self.namespace]:
            return True
        return False

    def report(self, values, record_identifier=None, force_overwrite=False,
               strict_type=True, return_id=False):
        """
        Report a result.

        :param dict[str, any] values: dictionary of result-value pairs
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
                "results file as the object backend")
        if self.schema is None:
            raise SchemaNotFoundError("report results")
        known_results = self.result_schemas.keys()
        result_identifiers = list(values.keys())
        for r in result_identifiers:
            if r not in known_results:
                raise SchemaError(
                    f"'{r}' is not a known result. Results defined in the "
                    f"schema are: {list(known_results)}.")
        existing = self._check_which_results_exist(
            rid=record_identifier, results=result_identifiers)
        if existing:
            _LOGGER.warning(
                f"These results exist for '{record_identifier}': {existing}")
            if not force_overwrite:
                return False
            _LOGGER.info(f"Overwriting existing results: {existing}")
        for r in result_identifiers:
            validate_type(value=values[r], schema=self.result_schemas[r],
                          strict_type=strict_type)
        if self.file is not None:
            self.data.make_writable()
        if not self[DB_ONLY_KEY]:
            self._report_data_element(
                record_identifier=record_identifier,
                values=values
            )
        if self.file is not None:
            self.data.write()
            self.data.make_readonly()
        else:
            try:
                updated_ids = self._report_postgres(
                    record_identifier=record_identifier,
                    value=values
                )
            except Exception as e:
                _LOGGER.error(f"Could not insert the result into the database. "
                              f"Exception: {e}")
                if not self[DB_ONLY_KEY]:
                    for r in result_identifiers:
                        del self[DATA_KEY][self.namespace][record_identifier][r]
                raise
        nl = "\n"
        rep_strs = [f'{k}: {v}' for k, v in values.items()]
        _LOGGER.info(
            f"Reported records for '{record_identifier}' in '{self.namespace}' "
            f"namespace:{nl} - {(nl + ' - ').join(rep_strs)}")
        return True if not return_id else updated_ids

    def _report_data_element(self, record_identifier, values):
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param any values: dict of results identifiers and values to be reported
        """
        self[DATA_KEY].setdefault(self.namespace, PXAM())
        self[DATA_KEY][self.namespace].setdefault(record_identifier, PXAM())
        for res_id, val in values.items():
            self[DATA_KEY][self.namespace][record_identifier][res_id] = val

    def select(self, columns=None, condition=None, condition_val=None):
        """
        Get all the contents from the selected table, possibly restricted by
        the provided condition.

        :param str | list[str] columns: columns to select
        :param str condition: condition to restrict the results
            with, will be appended to the end of the SELECT statement and
            safely populated with 'condition_val',
            for example: `"id=%s"`
        :param list condition_val: values to fill the placeholder
            in 'condition' with
        :return list[psycopg2.extras.DictRow]: all table contents
        """
        if self.file:
            raise NotImplementedError(
                "Selection is not supported on objects backed by results files."
                " Use 'retrieve' method instead."
            )
        condition, condition_val = \
            preprocess_condition_pair(condition, condition_val)
        if not columns:
            columns = sql.SQL("*")
        else:
            columns = sql.SQL(',').join(
                [sql.Identifier(x) for x in mk_list_of_str(columns)])
        statement = sql.SQL("SELECT {} FROM {}").format(
            columns, sql.Identifier(self.namespace))
        if condition:
            statement += sql.SQL(" WHERE ")
            statement += condition
        with self.db_cursor as cur:
            cur.execute(query=statement, vars=condition_val)
            result = cur.fetchall()
        return result

    def retrieve(self, record_identifier=None, result_identifier=None):
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :return any | dict[str, any]: a single result or a mapping with all the
            results reported for the record
        """
        record_identifier = self._strict_record_id(record_identifier)
        if self[DB_ONLY_KEY]:
            existing = self._check_which_results_exist(
                results=[result_identifier],
                rid=record_identifier
            )
            if not existing:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record "
                    f"'{record_identifier}'")
            with self.db_cursor as cur:
                cur.execute(f"SELECT {result_identifier} FROM {self.namespace} "
                            f"WHERE {RECORD_ID}=%s", (record_identifier, ))
                return cur.fetchone()[0]
        else:
            if record_identifier not in self.data[self.namespace]:
                raise PipestatDatabaseError(
                    f"Record '{record_identifier}' not found")
            if result_identifier is None:
                return self.data[self.namespace][record_identifier]
            if result_identifier not in self.data[self.namespace][record_identifier]:
                raise PipestatDatabaseError(
                    f"Result '{result_identifier}' not found for record "
                    f"'{record_identifier}'")
            return self.data[self.namespace][record_identifier][result_identifier]

    def remove(self, record_identifier=None, result_identifier=None):
        """
        Report a result.

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
                result_identifier, record_identifier):
            _LOGGER.error(f"'{result_identifier}' has not been reported for "
                          f"'{record_identifier}'")
            return False
        if self.file:
            self.data.make_writable()
        if not self[DB_ONLY_KEY]:
            if rm_record:
                _LOGGER.info(f"Removing '{record_identifier}' record")
                del self[DATA_KEY][self.namespace][record_identifier]
            else:
                val_backup = \
                    self[DATA_KEY][self.namespace][record_identifier][result_identifier]
                del self[DATA_KEY][self.namespace][record_identifier][result_identifier]
                _LOGGER.info(f"Removed result '{result_identifier}' for record "
                             f"'{record_identifier}' from '{self.namespace}' namespace")
                if not self[DATA_KEY][self.namespace][record_identifier]:
                    _LOGGER.info(f"Last result removed for '{record_identifier}'. "
                                 f"Removing the record")
                    del self[DATA_KEY][self.namespace][record_identifier]
                    rm_record = True
        if self.file:
            self.data.write()
            self.data.make_readonly()
        if self.file is None:
            if rm_record:
                try:
                    with self.db_cursor as cur:
                        cur.execute(f"DELETE FROM {self.namespace} WHERE "
                                    f"{RECORD_ID}='{record_identifier}'")
                except Exception as e:
                    _LOGGER.error(f"Could not remove the result from the "
                                  f"database. Exception: {e}")
                    self[DATA_KEY][self.namespace].setdefault(
                        record_identifier, PXAM())
                    raise
                return True
            try:
                with self.db_cursor as cur:
                    cur.execute(
                        f"UPDATE {self.namespace} SET {result_identifier}=null "
                        f"WHERE {RECORD_ID}='{record_identifier}'"
                    )
            except Exception as e:
                _LOGGER.error(f"Could not remove the result from the database. "
                              f"Exception: {e}")
                if not self[DB_ONLY_KEY]:
                    self[DATA_KEY][self.namespace][record_identifier][result_identifier] = val_backup
                raise
        return True

    def validate_schema(self):
        """
        Check schema for any possible issues

        :raises SchemaError: if any schema format issue is detected
        """
        def _recursively_replace_custom_types(s):
            """
            Replace the custom types in pipestat schema with canonical types

            :param dict s: schema to replace types in
            :return dict: schema with types replaced
            """
            for k, v in s.items():
                assert SCHEMA_TYPE_KEY in v, \
                    SchemaError(f"Result '{k}' is missing '{SCHEMA_TYPE_KEY}' key")
                if v[SCHEMA_TYPE_KEY] == "object" and SCHEMA_PROP_KEY in s[k]:
                    _recursively_replace_custom_types(s[k][SCHEMA_PROP_KEY])
                if v[SCHEMA_TYPE_KEY] in CANONICAL_TYPES.keys():
                    s.setdefault(k, {})
                    s[k].setdefault(SCHEMA_PROP_KEY, {})
                    s[k][SCHEMA_PROP_KEY].update(
                        CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]][SCHEMA_PROP_KEY])
                    s[k].setdefault("required", [])
                    s[k]["required"].extend(
                        CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]]["required"])
                    s[k][SCHEMA_TYPE_KEY] = \
                        CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]][SCHEMA_TYPE_KEY]
            return s

        schema = deepcopy(self.schema)
        _LOGGER.debug(f"Validating input schema")
        assert isinstance(schema, dict), \
            SchemaError(f"The schema has to be a {dict().__class__.__name__}")
        self[RES_SCHEMAS_KEY] = {}
        schema = _recursively_replace_custom_types(schema)
        self[RES_SCHEMAS_KEY] = schema

    def check_connection(self):
        """
        Check whether a PostgreSQL connection has been established

        :return bool: whether the connection has been established
        """
        if self.file is not None:
            raise PipestatDatabaseError(f"The {self.__class__.__name__} object "
                                        f"is not backed by a database")
        if DB_CONNECTION_KEY in self and isinstance(
                self[DB_CONNECTION_KEY], psycopg2.extensions.connection):
            return True
        return False

    def establish_postgres_connection(self, suppress=False):
        """
        Establish PostgreSQL connection using the config data

        :param bool suppress: whether to suppress any connection errors
        :return bool: whether the connection has been established successfully
        """
        if self.check_connection():
            raise PipestatDatabaseError(f"Connection is already established: "
                                        f"{self[DB_CONNECTION_KEY].info.host}")
        try:
            self[DB_CONNECTION_KEY] = psycopg2.connect(
                dbname=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_NAME_KEY],
                user=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_USER_KEY],
                password=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_PASSWORD_KEY],
                host=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY],
                port=self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_PORT_KEY]
            )
        except psycopg2.Error as e:
            _LOGGER.error(f"Could not connect to: "
                          f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}")
            _LOGGER.info(f"Caught error: {e}")
            if suppress:
                return False
            raise
        else:
            _LOGGER.debug(f"Established connection with PostgreSQL: "
                          f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}")
            return True

    def close_postgres_connection(self):
        """
        Close connection and remove client bound
        """
        if not self.check_connection():
            raise PipestatDatabaseError(
                f"The connection has not been established: "
                f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}")
        self[DB_CONNECTION_KEY].close()
        del self[DB_CONNECTION_KEY]
        _LOGGER.debug(f"Closed connection with PostgreSQL: "
                      f"{self[CONFIG_KEY][CFG_DATABASE_KEY][CFG_HOST_KEY]}")

    def _strict_record_id(self, forced_value=None):
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


def main():
    """ Primary workflow """
    from inspect import getdoc
    parser = logmuse.add_logging_options(
        build_argparser(getdoc(PipestatManager)))
    args = parser.parse_args()
    if args.command is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    global _LOGGER
    _LOGGER = logmuse.logger_via_cli(args, make_root=True)
    _LOGGER.debug("Args namespace:\n{}".format(args))
    if args.database_config and not args.schema:
        parser.error("the following arguments are required: -s/--schema")
    psm = PipestatManager(
        namespace=args.namespace,
        schema_path=args.schema,
        results_file_path=args.results_file,
        database_config=args.database_config
    )
    if args.command == REPORT_CMD:
        value = args.value
        result_metadata = psm.schema[args.result_identifier]
        if result_metadata[SCHEMA_TYPE_KEY] in ["object", "image", "file"] \
                and os.path.exists(expandpath(value)):
            from json import load
            _LOGGER.info(f"Reading JSON file with object type value: "
                         f"{expandpath(value)}")
            with open(expandpath(value), "r") as json_file:
                value = load(json_file)
        psm.report(
            record_identifier=args.record_identifier,
            values={args.result_identifier: value},
            force_overwrite=args.overwrite,
            strict_type=not args.try_convert
        )
        sys.exit(0)
    if args.command == INSPECT_CMD:
        print("\n")
        print(psm)
        if args.data and not args.database_only:
            print("\nData:")
            print(psm.data)
        sys.exit(0)
    if args.command == REMOVE_CMD:
        psm.remove(
            result_identifier=args.result_identifier,
            record_identifier=args.record_identifier
        )
        sys.exit(0)
    if args.command == RETRIEVE_CMD:
        print(psm.retrieve(
            result_identifier=args.result_identifier,
            record_identifier=args.record_identifier
        ))
        sys.exit(0)
