"""Infer pipestat schema from results file."""

import os
from collections import defaultdict
from logging import getLogger
from typing import Any, Dict, Optional

import yaml

from .const import PKG_NAME
from .exceptions import SchemaError

_LOGGER = getLogger(PKG_NAME)

# Extensions for type inference
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp"}
FILE_EXTENSIONS = {".csv", ".tsv", ".json", ".pdf", ".txt", ".html", ".bed", ".bam"}


def infer_schema(
    results_file: str,
    output_file: Optional[str] = None,
    level: Optional[str] = None,
    strict: bool = False,
    pipeline_name: Optional[str] = None,
) -> dict:
    """Infer a pipestat schema from a results file.

    Args:
        results_file: Path to YAML results file.
        output_file: Path to write schema (stdout if None).
        level: "sample", "project", or None (auto-detect from data).
        strict: If True, error on type conflicts; if False, use most common type.
        pipeline_name: Override pipeline name in schema (auto-detected if None).

    Returns:
        dict: Inferred schema dictionary.

    Raises:
        SchemaError: If strict mode and type conflicts are found.
        FileNotFoundError: If results file doesn't exist.
    """
    with open(results_file, "r") as f:
        data = yaml.safe_load(f)

    if not data:
        _LOGGER.warning("Results file is empty")
        return {}

    # Auto-detect pipeline name and levels with data
    detected_pipeline_name = None
    detected_levels = set()

    for pname, levels in data.items():
        if detected_pipeline_name is None:
            detected_pipeline_name = pname
        if isinstance(levels, dict):
            for level_name, records in levels.items():
                if records and level_name in ("sample", "project"):
                    detected_levels.add(level_name)

    # Use specified level, or detected levels, or default to sample
    levels_to_process = [level] if level else list(detected_levels) or ["sample"]

    _LOGGER.info(f"Inferring schema for level(s): {levels_to_process}")

    schema = {"pipeline_name": pipeline_name or detected_pipeline_name}

    for proc_level in levels_to_process:
        # Collect types per result key for this level
        type_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for pname, levels in data.items():
            if not isinstance(levels, dict):
                continue
            if proc_level not in levels:
                continue
            records = levels[proc_level]
            if not isinstance(records, dict):
                continue
            for record_id, results in records.items():
                if not isinstance(results, dict):
                    continue
                for key, value in results.items():
                    if key in ("meta", "history"):
                        continue
                    inferred_type = _infer_type(value)
                    type_counts[key][inferred_type] += 1

        # Resolve conflicts and build schema items
        schema_items = {}
        for key, types in type_counts.items():
            if len(types) > 1:
                if strict:
                    raise SchemaError(f"Type conflict for '{key}': {dict(types)}")
                chosen_type = max(types.items(), key=lambda x: x[1])[0]
                _LOGGER.warning(
                    f"Result '{key}' has mixed types: {dict(types)}. Using '{chosen_type}'."
                )
            else:
                chosen_type = list(types.keys())[0]

            schema_items[key] = _type_to_schema(chosen_type)

        # Use correct key name for schema section
        section_key = "samples" if proc_level == "sample" else "project"
        schema[section_key] = schema_items

    if output_file:
        with open(output_file, "w") as f:
            yaml.dump(schema, f, default_flow_style=False, sort_keys=False)
        _LOGGER.info(f"Schema written to {output_file}")
    else:
        print(yaml.dump(schema, default_flow_style=False, sort_keys=False))

    return schema


def _infer_type(value: Any) -> str:
    """Infer JSON Schema type from a Python value.

    Args:
        value: Python value to infer type from.

    Returns:
        str: Inferred type name.
    """
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        if "path" in value:
            # Check extension to distinguish image vs file
            path = value.get("path", "")
            ext = os.path.splitext(path)[1].lower()
            if ext in IMAGE_EXTENSIONS:
                return "image"
            return "file"
        return "object"
    return "string"  # fallback


def _type_to_schema(type_name: str) -> dict:
    """Convert inferred type name to JSON Schema dict.

    Args:
        type_name: Inferred type name.

    Returns:
        dict: JSON Schema representation.
    """
    if type_name == "image":
        return {
            "type": "object",
            "object_type": "image",
            "properties": {
                "path": {"type": "string"},
                "thumbnail_path": {"type": "string"},
                "title": {"type": "string"},
            },
            "required": ["path", "title"],
        }
    if type_name == "file":
        return {
            "type": "object",
            "object_type": "file",
            "properties": {
                "path": {"type": "string"},
                "title": {"type": "string"},
            },
            "required": ["path", "title"],
        }
    return {"type": type_name}
