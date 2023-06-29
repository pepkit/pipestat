"""Project-wide constants"""

import os
import sys
from pathlib import Path

# Can be removed when 3.8 is deprecated
if int(sys.version.split(".")[1]) < 9:
    from typing import List, Dict

    list_of_dicts = List[Dict]
else:
    list_of_dicts = list[dict]


__all__ = [
    "CANONICAL_TYPES",
    "CFG_SCHEMA",
    "CLASSES_BY_TYPE",
    "ENV_VARS",
    "LOCK_PREFIX",
    "PKG_NAME",
    "SAMPLE_NAME",
    "SAMPLE_NAME_ID_KEY",
    "SCHEMA_DESC_KEY",
    "SCHEMA_PROP_KEY",
    "SCHEMA_TYPE_KEY",
    "SCHEMA_ITEMS_KEY",
    "STATUS",
    "STATUS_SCHEMA",
    "CFG_DATABASE_KEY",
    "CONFIG_KEY",
    "DATA_KEY",
    "DB_COLUMN_KEY",
    "DB_ENGINE_KEY",
    "DB_ONLY_KEY",
    "DB_ORMS_KEY",
    "FILE_KEY",
    "SCHEMA_KEY",
    "STATUS_FILE_DIR",
    "STATUS_SCHEMA_SOURCE_KEY",
    "STATUS_SCHEMA_KEY",
    "PROJECT_NAME",
    "PIPELINE_NAME",
    "PIPELINE_TYPE",
    "DB_URL",
    "SCHEMA_PATH",
    "ID_KEY",
    "PIPESTAT_GENERIC_CONFIG",
    "RESULT_FORMATTER",
    "DEFAULT_PIPELINE_NAME",
    "MULTI_PIPELINE",
]

PKG_NAME = "pipestat"
LOCK_PREFIX = "lock."

# object attribute names
SAMPLE_NAME_ID_KEY = "_sample_name"

# schema keys
SCHEMA_PROP_KEY = "properties"
SCHEMA_TYPE_KEY = "type"
SCHEMA_DESC_KEY = "description"
SCHEMA_ITEMS_KEY = "items"

# DB column names
SAMPLE_NAME = "sample_name"
STATUS = "status"

CANONICAL_TYPES = {
    "image": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "thumbnail_path": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["path", "thumbnail_path", "title"],
    },
    "file": {
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "title": {"type": "string"},
        },
        "required": ["path", "title"],
    },
}

ENV_VARS = {
    "project_name": "PIPESTAT_PROJECT_NAME",
    "config": "PIPESTAT_CONFIG",
    "results_file": "PIPESTAT_RESULTS_FILE",
    "schema": "PIPESTAT_RESULTS_SCHEMA",
    "status_schema": "PIPESTAT_STATUS_SCHEMA",
    "sample_name": "PIPESTAT_SAMPLE_NAME",
}

CLASSES_BY_TYPE = {
    "object": dict,
    "number": float,
    "integer": int,
    "string": str,
    "path": Path,
    "boolean": bool,
    "file": str,
    "image": str,
    "link": str,
    "array": list_of_dicts,
}

CFG_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "pipestat_config_schema.yaml"
)
STATUS_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "status_schema.yaml"
)


CFG_DATABASE_KEY = "database"
CONFIG_KEY = "_config"
DATA_KEY = "_data"
DB_COLUMN_KEY = "db_column"
DB_ENGINE_KEY = "_db_engine"
DB_ONLY_KEY = "_database_only"
DB_ORMS_KEY = "_orms"
FILE_KEY = "_file"
SCHEMA_KEY = "_schema"
STATUS_FILE_DIR = "_status_file_dir"
STATUS_SCHEMA_KEY = "_status_schema"
STATUS_SCHEMA_SOURCE_KEY = "_status_schema_source"
PROJECT_NAME = "project_name"
PIPELINE_NAME = "_pipeline_name"
PIPELINE_TYPE = "_pipeline_type"
DB_URL = "_db_url"
SCHEMA_PATH = "_schema_path"
ID_KEY = "id"
PIPESTAT_GENERIC_CONFIG = "generic_config.yaml"
RESULT_FORMATTER = "_result_formatter"
DEFAULT_PIPELINE_NAME = "default_pipeline_name"
MULTI_PIPELINE = "_multi_pipelines"
