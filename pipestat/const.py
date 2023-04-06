import os

from pathlib import Path
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.types import ARRAY, JSON, Boolean, Float, Integer, String

PKG_NAME = "pipestat"
LOCK_PREFIX = "lock."
REPORT_CMD = "report"
INSPECT_CMD = "inspect"
REMOVE_CMD = "remove"
RETRIEVE_CMD = "retrieve"
STATUS_CMD = "status"
SUBPARSER_MSGS = {
    REPORT_CMD: "Report a result.",
    INSPECT_CMD: "Inspect a database.",
    REMOVE_CMD: "Remove a result.",
    RETRIEVE_CMD: "Retrieve a result.",
    STATUS_CMD: "Manage pipeline status.",
}

STATUS_GET_CMD = "get"
STATUS_SET_CMD = "set"

STATUS_SUBPARSER_MESSAGES = {
    STATUS_SET_CMD: "Set status.",
    STATUS_GET_CMD: "Get status.",
}

DOC_URL = "http://pipestat.databio.org/en/latest/db_config/"

# DB config keys
CFG_DATABASE_KEY = "database"
CFG_NAME_KEY = "name"
CFG_HOST_KEY = "host"
CFG_PORT_KEY = "port"
CFG_PASSWORD_KEY = "password"
CFG_USER_KEY = "user"
CFG_DIALECT_KEY = "dialect"  # sqlite, mysql, postgresql, oracle, or mssql
CFG_DRIVER_KEY = "driver"

# object attribute names
DB_ONLY_KEY = "_database_only"
CONFIG_KEY = "_config"
SCHEMA_KEY = "_schema"
STATUS_KEY = "_status"
STATUS_SCHEMA_SOURCE_KEY = "_status_schema_source"
STATUS_FILE_DIR = "_status_file_dir"
RES_SCHEMAS_KEY = "_result_schemas"
DB_BASE_KEY = "_declarative_base"
DB_ORMS_KEY = "_orms"
DATA_KEY = "_data"
FILE_KEY = "_file"
RECORD_ID_KEY = "_record_id"
DB_SESSION_KEY = "_db_session"
DB_ENGINE_KEY = "_db_engine"
HIGHLIGHTED_KEY = "_highlighted"
DB_COLUMN_KEY = "db_column"
DB_RELATIONSHIP_KEY = "relationship"
DB_RELATIONSHIP_NAME_KEY = "name"
DB_RELATIONSHIP_TABLE_KEY = "table"
DB_RELATIONSHIP_COL_KEY = "column"
DB_RELATIONSHIP_BACKREF_KEY = "backref"
DB_RELATIONSHIP_ELEMENTS = [
    DB_RELATIONSHIP_BACKREF_KEY,
    DB_RELATIONSHIP_COL_KEY,
    DB_RELATIONSHIP_NAME_KEY,
    DB_RELATIONSHIP_TABLE_KEY,
]

# schema keys
SCHEMA_PROP_KEY = "properties"
SCHEMA_TYPE_KEY = "type"
SCHEMA_DESC_KEY = "description"
SCHEMA_PIPELINE_ID_KEY = "pipeline_id"

# DB column names
ID = "id"
RECORD_ID = "record_identifier"
STATUS = "status"

RESERVED_COLNAMES = [ID, RECORD_ID]

FILE_TYPE_NAME = "file"
IMAGE_TYPE_NAME = "image"

CANONICAL_TYPES = {
    IMAGE_TYPE_NAME: {
        "type": IMAGE_TYPE_NAME,
        "properties": {
            "path": {"type": "string"},
            "thumbnail_path": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["path", "thumbnail_path", "title"],
    },
    FILE_TYPE_NAME: {
        "type": FILE_TYPE_NAME,
        "properties": {
            "path": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["path", "title"],
    },
}

ENV_VARS = {
    "namespace": "PIPESTAT_NAMESPACE",
    "config": "PIPESTAT_CONFIG",
    "results_file": "PIPESTAT_RESULTS_FILE",
    "schema": "PIPESTAT_RESULTS_SCHEMA",
    "status_schema": "PIPESTAT_SATUS_SCHEMA",
    "record_identifier": "PIPESTAT_RECORD_ID",
}

CLASSES_BY_TYPE = {
    "number": float,
    "integer": int,
    "string": str,
    "path": Path,
    "boolean": bool,
}

SQL_CLASSES_BY_TYPE = {
    "number": Float,
    "integer": Integer,
    "object": JSONB,
    "image": JSONB,
    "file": JSONB,
    "string": String(500),
    "array": JSONB,
    "boolean": Boolean,
}

CFG_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "pipestat_config_schema.yaml"
)
STATUS_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "status_schema.yaml"
)

STATUS_TABLE_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "status_table_schema.yaml"
)
