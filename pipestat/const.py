PKG_NAME = "pipestat"
LOCK_PREFIX = "lock."
REPORT_CMD = "report"
INSPECT_CMD = "inspect"
REMOVE_CMD = "remove"
TABLE_CMD = "table"
SUBPARSER_MSGS = {
    REPORT_CMD: "Report a result.",
    INSPECT_CMD: "Inspect a database.",
    REMOVE_CMD: "Remove a result.",
    TABLE_CMD: "Create a results table."
}

ATTRS_BY_TYPE = {
    "integer": [],
    "float": [],
    "string": [],
    "boolean": [],
    "object": [],
    "array": [],
    "file": ["path", "title"],
    "image": ["thumbnail_path", "path", "title"]
}

TABLE_COLS_BY_TYPE = {
    "integer": '{} INT',
    "float": '{} FLOAT',
    "string": "{} TEXT",
    "boolean": '{} BOOLEAN',
    "object": '{} JSONB',
    "array": '{} TEXT[]',
    "file": '{} JSONB',
    "image": '{} JSONB'
}

DOC_URL = "TBA"

# DB config keys
CFG_DB_NAME_KEY = "db_name"
CFG_DB_HOST_KEY = "db_host"
CFG_DB_PORT_KEY = "db_port"
CFG_DB_PASSWORD_KEY = "db_password"
CFG_DB_USER_KEY = "db_user"

DB_CREDENTIALS = [CFG_DB_HOST_KEY, CFG_DB_PORT_KEY, CFG_DB_PASSWORD_KEY,
                  CFG_DB_USER_KEY, CFG_DB_NAME_KEY]

# object attribute names
CONFIG_KEY = "_config"
SCHEMA_KEY = "_schema"
DATA_KEY = "_data"
NAME_KEY = "_name"
FILE_KEY = "_file"
DB_CONNECTION_KEY = "_db_connnection"

# schema keys
SCHEMA_PROP_KEY = "properties"
SCHEMA_TYPE_KEY = "type"

# DB column names
ID = "id"
RECORD_ID = "record_identifier"

FIXED_COLUMNS = [f"{ID} BIGSERIAL PRIMARY KEY",
                 f"{RECORD_ID} TEXT UNIQUE NOT NULL"]