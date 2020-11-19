import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, Json
from psycopg2.extensions import connection
from logging import getLogger
from contextlib import contextmanager
from copy import deepcopy

import sys
import logmuse
from attmap import AttMap, PathExAttMap as PXAM, AttMapLike
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
        try:
            super(LoggingCursor, self).execute(query=query, vars=vars)
        except Exception as e:
            _LOGGER.error(f"{e.__class__.__name__}: {e}")
            raise
        else:
            _LOGGER.debug(f"Executed query: {self.query}")


class PipestatManager(dict):
    """
    pipestat standardizes reporting of pipeline results. It formalizes a way
    for pipeline developers and downstream tools developers to communicate --
    results produced by a pipeline can easily and reliably become an input for
    downstream analyses. The ovject exposes API for interacting with the results
    can be backed by either a YAML-formatted file or a PostgreSQL database.
    """
    def __init__(self, name, schema_path, results_file=None, database_config=None):
        """
        Initialize the object

        :param str name: namespace to report into. This will be the DB table
            name if using DB as the object back-end
        :param str schema_path: path to the output schema that formalizes
            the results structure
        :param str results_file: YAML file to report into, if file is used as
            the object back-end
        :param str database_config: DB login credentials to report into,
            if DB is used as the object back-end
        """
        def _check_cfg_key(cfg, key):
            if key not in cfg:
                _LOGGER.warning(f"Key '{key}' not found in config")
                return False
            return True

        super(PipestatManager, self).__init__()

        self[NAME_KEY] = str(name)
        _, self[SCHEMA_KEY] = read_yaml_data(schema_path, "schema")
        self.validate_schema()
        if results_file:
            self[FILE_KEY] = expandpath(results_file)
            self._init_results_file()
        elif database_config:
            _, self[CONFIG_KEY] = read_yaml_data(database_config, "DB config")
            if not all([_check_cfg_key(self[CONFIG_KEY][CFG_DATABASE_KEY], key)
                        for key in DB_CREDENTIALS]):
                raise MissingConfigDataError(
                    "Must specify all database login credentials")
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
        res = f"{self.__class__.__name__} ({self.name})"
        records_count = len(self[DATA_KEY]) if self.file \
            else self._count_rows(table_name=self.name)
        res += "\nBackend: {}".format(
            f"file ({self.file})" if self.file else "PostgreSQL")
        res += f"\nRecords count: {records_count}"
        return res

    @property
    def name(self):
        """
        Namespace the object writes the results to

        :return str: Namespace the object writes the results to
        """
        return self[NAME_KEY] if NAME_KEY in self else None

    @property
    def schema(self):
        """
        Schema mapping

        :return dict: schema that formalizes the results structure
        """
        return self[SCHEMA_KEY] if SCHEMA_KEY in self else None

    @property
    def result_schemas(self):
        """
        Result schema mappings

        :return dict: schemas that formalize the structure of each result
            in a canonical jsonschema way
        """
        return self[RES_SCHEMAS_KEY] if RES_SCHEMAS_KEY in self else None

    @property
    def file(self):
        """
        File path that the object is reporting the results into

        :return str: file path that the object is reporting the results into
        """
        return self[FILE_KEY] if FILE_KEY in self else None

    @property
    def data(self):
        """
        Data object

        :return yacman.YacAttMap: the object that stores the reported data
        """
        return self[DATA_KEY] if DATA_KEY in self else None

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

    def _table_to_dict(self):
        """
        Create a dictionary from the database table data

        :return dict: database table data in a dict form
        """
        with self.db_cursor as cur:
            cur.execute(f"SELECT * FROM {self.name}")
            data = cur.fetchall()
        _LOGGER.info(f"Reading data from database for '{self.name}' namespace")
        for record in data:
            for result_id in list(self.schema.keys()):
                record_id = record[RECORD_ID]
                value = record[result_id]
                if value is not None:
                    _LOGGER.debug(f"Saving result: {result_id}={value}")
                    self._report_data_element(
                        record_identifier=record_id,
                        result_identifier=result_id,
                        value=value
                    )

    def _init_postgres_table(self):
        """
        Initialize postgreSQL table based on the provided schema,
        if it does not exist. Read the data stored in the database into the
        memory otherwise.

        :return bool: whether the table has been created
        """
        if self._check_table_exists(table_name=self.name):
            _LOGGER.debug(
                f"Table '{self.name}' already exists in the database")
            self._table_to_dict()
            return False
        _LOGGER.info(
            f"Initializing '{self.name}' table in '{PKG_NAME}' database")
        columns = FIXED_COLUMNS + schema_to_columns(schema=self.schema)
        with self.db_cursor as cur:
            s = sql.SQL(f"CREATE TABLE {self.name} ({','.join(columns)})")
            cur.execute(s)
        return True

    def _init_results_file(self):
        """
        Initialize postgreSQL table based on the provided schema,
        if it does not exist. Read the data stored in the database into the
        memory otherwise.

        :return bool: whether the table has been created
        """
        if not os.path.exists(self.file):
            _LOGGER.info(f"Initializing results file '{self.file}'")
            data = YacAttMap(entries={self.name: PXAM()})
            data.write(filepath=self.file)
            self[DATA_KEY] = data
            return True
        _LOGGER.info(f"Reading data from '{self.file}'")
        data = YacAttMap(filepath=self.file)
        filtered = list(filter(lambda x: not x.startswith("_"), data.keys()))
        if filtered and self.name not in filtered:
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
            statement = f"SELECT EXISTS(SELECT 1 from {self.name} " \
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
        :param dict value: a mapping of pair of table column name and
            respective value to be inserted to the database
        :return int: id of the row just inserted
        """
        # TODO: allow multi-value insertions
        # placeholder = sql.SQL(','.join(['%s'] * len(value)))
        # TODO: allow returning updated/inserted record ID
        if not self._check_record(condition_col=RECORD_ID,
                                  condition_val=record_identifier):
            with self.db_cursor as cur:
                cur.execute(
                    f"INSERT INTO {self.name} ({RECORD_ID}) VALUES (%s)",
                    (record_identifier, )
                )
        column = list(value.keys())
        assert len(column) == 1, \
            NotImplementedError("Can't report more than one column at once")
        value = list(value.values())[0]
        query = "UPDATE {table_name} SET {column}=%s " \
                "WHERE {record_id_col}=%s"
        statement = sql.SQL(query).format(
            column=sql.Identifier(column[0]),
            table_name=sql.Identifier(self.name),
            record_id_col=sql.SQL(RECORD_ID)
        )
        # convert mappings to JSON for postgres
        values = Json(value) if isinstance(value, Mapping) else value
        with self.db_cursor as cur:
            cur.execute(statement, (values, record_identifier))

    def check_result_exists(self, record_identifier, result_identifier):
        """
        Check if the result has been reported

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to check
        :return bool: whether the specified result has been reported for the
            indicated record in current namespace
        """
        if self.name in self.data and \
                record_identifier in self.data[self.name] and \
                result_identifier in self.data[self.name][record_identifier]:
            return True
        return False

    def check_record_exists(self, record_identifier):
        """
        Check if the record exists

        :param str record_identifier: unique identifier of the record
        :return bool: whether the record exists
        """
        if self.name in self.data and record_identifier in self.data[self.name]:
            return True
        return False

    def report(self, record_identifier, result_identifier, value,
               force_overwrite=False, strict_type=True):
        """
        Report a result.

        :param str record_identifier: unique identifier of the record, value to
            in 'record_identifier' column to look for to determine if the record
            already exists
        :param any value: value to be reported
        :param str result_identifier: name of the result to be reported
        :param bool force_overwrite: whether to overwrite the existing record
        :return bool: whether the result has been reported
        """
        known_results = self.result_schemas.keys()
        if result_identifier not in known_results:
            raise SchemaError(
                f"'{result_identifier}' is not a known result. Results defined "
                f"in the schema are: {list(known_results)}.")
        if self.check_result_exists(record_identifier, result_identifier):
            _LOGGER.warning(
                f"'{result_identifier}' exists for '{record_identifier}'")
            if not force_overwrite:
                return False
        validate_type(value=value,
                      schema=self.result_schemas[result_identifier],
                      strict_type=strict_type)
        if self.file:
            self.data.make_writable()
        self._report_data_element(record_identifier, result_identifier, value)
        if self.file:
            self.data.write()
            self.data.make_readonly()
        if self.file is None:
            try:
                self._report_postgres(value={result_identifier: value},
                                      record_identifier=record_identifier)
            except Exception as e:
                _LOGGER.error(f"Could not insert the result into the database. "
                              f"Exception: {e}")
                del self[DATA_KEY][self.name][record_identifier][result_identifier]
                raise
        _LOGGER.info(
            f"Reported record for '{record_identifier}': {result_identifier}="
            f"{value} in '{self.name}' namespace")
        return True

    def _report_data_element(self, record_identifier, result_identifier, value):
        """
        Update the value of a result in a current namespace.

        This method overwrites any existing data and creates the required
         hierarchical mapping structure if needed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be reported
        :param any value: value to be reported
        """
        self[DATA_KEY].setdefault(self.name, PXAM())
        self[DATA_KEY][self.name].setdefault(record_identifier, PXAM())
        self[DATA_KEY][self.name][record_identifier][result_identifier] = value

    def retrieve(self, record_identifier, result_identifier=None):
        """
        Retrieve a result for a record.

        If no result ID specified, results for the entire record will
        be returned.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be retrieved
        :return any | dict[any]: a single result or a mapping with all the
            results reported for the record
        """
        if record_identifier not in self.data[self.name]:
            raise PipestatDatabaseError(
                f"Record '{record_identifier}' not found")
        if result_identifier is None:
            return self.data[self.name][record_identifier]
        if result_identifier not in self.data[self.name][record_identifier]:
            raise PipestatDatabaseError(
                f"Result '{result_identifier}' not found for record "
                f"'{record_identifier}'")
        return self.data[self.name][record_identifier][result_identifier]

    def remove(self, record_identifier, result_identifier=None):
        """
        Report a result.

        If no result ID specified or last result is removed, the entire record
        will be removed.

        :param str record_identifier: unique identifier of the record
        :param str result_identifier: name of the result to be removed or None
             if the record should be removed.
        :return bool: whether the result has been removed
        """
        rm_record = True if result_identifier is None else False
        if not self.check_record_exists(record_identifier):
            _LOGGER.error(f"Record '{record_identifier}' not found")
            return False
        if result_identifier and not self.check_result_exists(
                record_identifier, result_identifier):
            _LOGGER.error(f"'{result_identifier}' has not been reported for "
                          f"'{record_identifier}'")
            return False
        if self.file:
            self.data.make_writable()
        if rm_record:
            _LOGGER.info(f"Removing '{record_identifier}' record")
            del self[DATA_KEY][self.name][record_identifier]
        else:
            val_backup = \
                self[DATA_KEY][self.name][record_identifier][result_identifier]
            del self[DATA_KEY][self.name][record_identifier][result_identifier]
            if not self[DATA_KEY][self.name][record_identifier]:
                _LOGGER.info(f"Last result removed for '{record_identifier}'. "
                             f"Removing the record")
                del self[DATA_KEY][self.name][record_identifier]
                rm_record = True
        if self.file:
            self.data.write()
            self.data.make_readonly()
        if self.file is None:
            if rm_record:
                try:
                    with self.db_cursor as cur:
                        cur.execute(f"DELETE FROM {self.name} WHERE "
                                    f"{RECORD_ID}='{record_identifier}'")
                except Exception as e:
                    _LOGGER.error(f"Could not remove the result from the "
                                  f"database. Exception: {e}")
                    self[DATA_KEY][self.name].setdefault(
                        record_identifier, PXAM())
                    raise
                return True
            try:
                with self.db_cursor as cur:
                    cur.execute(
                        f"UPDATE {self.name} SET {result_identifier}=null "
                        f"WHERE {RECORD_ID}='{record_identifier}'"
                    )
            except Exception as e:
                _LOGGER.error(f"Could not remove the result from the database. "
                              f"Exception: {e}")
                self[DATA_KEY][self.name][record_identifier][result_identifier] = val_backup
                raise
        return True

    def validate_schema(self):
        """
        Check schema for any possible issues

        :raises SchemaError: if any schema format issue is detected
        """
        schema = deepcopy(self.schema)
        _LOGGER.debug(f"Validating input schema")
        assert isinstance(schema, dict), \
            SchemaError(f"The schema has to be a {dict.__class__.__name__}")
        self[RES_SCHEMAS_KEY] = {}
        for k, v in schema.items():
            assert SCHEMA_TYPE_KEY in v, \
                SchemaError(f"Result '{k}' is missing '{SCHEMA_TYPE_KEY}' key")
            if v[SCHEMA_TYPE_KEY] in CANONICAL_TYPES.keys():
                schema.setdefault(k, {})
                schema[k].update(CANONICAL_TYPES[v[SCHEMA_TYPE_KEY]])
            self[RES_SCHEMAS_KEY].setdefault(k, {})
            self[RES_SCHEMAS_KEY][k] = schema[k]

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
            cfg_db = self[CONFIG_KEY][CFG_DATABASE_KEY]
            self[DB_CONNECTION_KEY] = psycopg2.connect(
                dbname=cfg_db[CFG_NAME_KEY],
                user=cfg_db[CFG_USER_KEY],
                password=cfg_db[CFG_PASSWORD_KEY],
                host=cfg_db[CFG_HOST_KEY],
                port=cfg_db[CFG_PORT_KEY]
            )
        except psycopg2.Error as e:
            _LOGGER.error(f"Could not connect to: "
                          f"{cfg_db[CFG_HOST_KEY]}")
            _LOGGER.info(f"Caught error: {e}")
            if suppress:
                return False
            raise
        else:
            _LOGGER.debug(f"Established connection with PostgreSQL: "
                          f"{cfg_db[CFG_HOST_KEY]}")
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
    psm = PipestatManager(
        name=args.namespace,
        schema_path=args.schema,
        results_file=args.results_file,
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
            result_identifier=args.result_identifier,
            record_identifier=args.record_identifier,
            value=value,
            force_overwrite=args.overwrite,
            strict_type=not args.try_convert
        )
        sys.exit(0)
    if args.command == INSPECT_CMD:
        print(psm)
        if args.data:
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

