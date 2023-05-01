"""Project-wide constants"""

import os
from pathlib import Path

__all__ = [
    "CANONICAL_TYPES",
    "CFG_SCHEMA",
    "CLASSES_BY_TYPE",
    "ENV_VARS",
    "LOCK_PREFIX",
    "PKG_NAME",
    "RECORD_ID",
    "RECORD_ID_KEY",
    "SCHEMA_DESC_KEY",
    "SCHEMA_PROP_KEY",
    "SCHEMA_TYPE_KEY",
    "STATUS",
    "STATUS_SCHEMA",
]

PKG_NAME = "pipestat"
LOCK_PREFIX = "lock."

# object attribute names
RECORD_ID_KEY = "_record_id"

# schema keys
SCHEMA_PROP_KEY = "properties"
SCHEMA_TYPE_KEY = "type"
SCHEMA_DESC_KEY = "description"

# DB column names
RECORD_ID = "record_identifier"
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
    "namespace": "PIPESTAT_NAMESPACE",
    "config": "PIPESTAT_CONFIG",
    "results_file": "PIPESTAT_RESULTS_FILE",
    "schema": "PIPESTAT_RESULTS_SCHEMA",
    "status_schema": "PIPESTAT_SATUS_SCHEMA",
    "record_identifier": "PIPESTAT_RECORD_ID",
}

CLASSES_BY_TYPE = {
    "object": str,
    "number": float,
    "integer": int,
    "string": str,
    "path": Path,
    "boolean": bool,
    "file": str,
    "image": str,
    "link": str,
}

CFG_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "pipestat_config_schema.yaml"
)
STATUS_SCHEMA = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "schemas", "status_schema.yaml"
)
