PKG_NAME = "pipestat"
LOCK_PREFIX = "lock."
REPORT_CMD = "report"
INSPECT_CMD = "inspect"
REMOVE_CMD = "remove"
RETRIEVE_CMD = "retrieve"
SUBPARSER_MSGS = {
    REPORT_CMD: "Report a result.",
    INSPECT_CMD: "Inspect a database.",
    REMOVE_CMD: "Remove a result.",
    RETRIEVE_CMD: "Retrieve a result."
}

TABLE_COLS_BY_TYPE = {
    "integer": '{} INT',
    "number": '{} NUMERIC',
    "string": "{} TEXT",
    "boolean": '{} BOOLEAN',
    "object": '{} JSONB',
    "array": '{} TEXT[]',
    "file": '{} JSONB',
    "image": '{} JSONB'
}

DOC_URL = "http://pipestat.databio.org/en/latest/db_config/"

# DB config keys
CFG_DATABASE_KEY = "database"
CFG_NAME_KEY = "name"
CFG_HOST_KEY = "host"
CFG_PORT_KEY = "port"
CFG_PASSWORD_KEY = "password"
CFG_USER_KEY = "user"

DB_CREDENTIALS = [CFG_HOST_KEY, CFG_PORT_KEY, CFG_PASSWORD_KEY, CFG_USER_KEY,
                  CFG_NAME_KEY]

# object attribute names
DB_ONLY_KEY = "_database_only"
CONFIG_KEY = "_config"
SCHEMA_KEY = "_schema"
RES_SCHEMAS_KEY = "_result_schemas"
DATA_KEY = "_data"
NAME_KEY = "_name"
FILE_KEY = "_file"
RECORD_ID_KEY = "_record_id"
DB_CONNECTION_KEY = "_db_connnection"

# schema keys
SCHEMA_PROP_KEY = "properties"
SCHEMA_TYPE_KEY = "type"

# DB column names
ID = "id"
RECORD_ID = "record_identifier"

FIXED_COLUMNS = [f"{ID} BIGSERIAL PRIMARY KEY",
                 f"{RECORD_ID} TEXT UNIQUE NOT NULL"]

CANONICAL_TYPES = {
    "image": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "thumbnail_path": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["path", "thumbnail_path", "title"]
    },
    "file": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["path", "title"]
    }
}

CLASSES_BY_TYPE = {
    "number": float,
    "integer": int,
    "object": dict,
    "image": dict,
    "file": dict,
    "string": str,
    "array": list,
    "boolean": bool
}