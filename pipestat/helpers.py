"""Assorted project utilities"""

import errno
import glob
import logging
import os
from json import dumps
from pathlib import Path
from shutil import make_archive
from typing import Any, Dict, List, Optional, Tuple, Union

import jsonschema
from yaml import dump

from .const import CLASSES_BY_TYPE, PIPESTAT_GENERIC_CONFIG, SCHEMA_PROP_KEY, SCHEMA_TYPE_KEY
from .exceptions import SchemaValidationErrorDuringReport

_LOGGER = logging.getLogger(__name__)


def validate_type(
    value: Any,
    schema: Dict[str, Any],
    strict_type: bool = False,
    record_identifier: Optional[str] = None,
) -> None:
    """Validate reported result against a partial schema, in case of failure try to cast the value.

    Does not support objects of objects.

    Args:
        value (any): Reported value.
        schema (dict): Partial jsonschema schema to validate against, e.g. {"type": "integer"}.
        strict_type (bool, optional): Whether the value should validate as is. Defaults to False.
        record_identifier (str, optional): Used for clarifying error messages. Defaults to None.
    """

    try:
        jsonschema.validate(value, schema)
    except jsonschema.exceptions.ValidationError as e:
        if strict_type:
            raise SchemaValidationErrorDuringReport(
                msg=str(e),
                record_identifier=record_identifier,
                result_identifier=schema,
                result=value,
            )
        _LOGGER.debug(f"{str(e)}")
        if schema[SCHEMA_TYPE_KEY] != "object":
            value = CLASSES_BY_TYPE[schema[SCHEMA_TYPE_KEY]](value)
        else:
            for prop, prop_dict in schema[SCHEMA_PROP_KEY].items():
                try:
                    cls_fun = CLASSES_BY_TYPE[prop_dict[SCHEMA_TYPE_KEY]]
                    value[prop] = cls_fun(value[prop])
                except Exception as e:
                    _LOGGER.error(f"Could not cast the result into " f"required type: {str(e)}")
                else:
                    _LOGGER.debug(
                        f"Casted the reported result into required " f"type: {str(cls_fun)}"
                    )
        jsonschema.validate(value, schema)
    else:
        _LOGGER.debug(f"Value '{value}' validated successfully against a schema")


def mk_list_of_str(x: Union[str, List[str], None]) -> Optional[List[str]]:
    """Make sure the input is a list of strings.

    Args:
        x (str | list[str] | falsy): Input to convert.

    Returns:
        list[str]: Converted input.

    Raises:
        TypeError: If the argument cannot be converted.
    """
    if not x or isinstance(x, list):
        return x
    if isinstance(x, str):
        return [x]
    raise TypeError(
        f"String or list of strings required as input. Got: " f"{x.__class__.__name__}"
    )


def make_subdirectories(path: Optional[str]) -> None:
    """Takes an absolute file path and creates subdirectories to file if they do not exist.

    Args:
        path: File path for which to create subdirectories.
    """

    if path:
        try:
            os.makedirs(os.path.dirname(path))
        except FileExistsError:
            pass


def init_generic_config() -> bool:
    """Create generic config file for DB Backend.

    Returns:
        bool: True if successful.
    """
    try:
        os.makedirs("config")
    except FileExistsError:
        pass

    # Destination one level down from CWD in config folder
    dest_file = os.path.join(os.getcwd(), "config", PIPESTAT_GENERIC_CONFIG)

    # Determine Generic Configuration File
    generic_config_dict = {
        "project_name": "generic_test_name",
        "sample_name": "sample1",
        "schema_path": "sample_output_schema.yaml",
        "database": {
            "dialect": "postgresql",
            "driver": "psycopg",
            "name": "pipestat-test",
            "user": "postgres",
            "password": "pipestat-password",
            "host": "127.0.0.1",
            "port": 5432,
        },
    }
    # Write file
    if not os.path.exists(dest_file):
        with open(dest_file, "w") as file:
            dump(generic_config_dict, file)
        print(f"Generic configuration file successfully created at: {dest_file}")
    else:
        print(f"Generic configuration file already exists `{dest_file}`. Skipping creation..")

    return True


def markdown_formatter(
    pipeline_name: str, record_identifier: str, res_id: str, value: Any
) -> str:
    """Returns Markdown formatted value as string.

    Args:
        pipeline_name: Name of the pipeline.
        record_identifier: Identifier of the record.
        res_id: Result identifier.
        value: Value to format.

    Returns:
        str: Markdown formatted result.
    """
    if not isinstance(value, dict):
        nl = "\n"
        rep_strs = [f"`{res_id}`: ```{value}```"]
        formatted_result = (
            f"\n > Reported records for `'{record_identifier}'` in `'{pipeline_name}'` {nl} "
            + f"{nl} {(nl).join(rep_strs)}"
        )
    else:
        nl = "\n"
        rep_strs = [f"`{res_id}`:\n ```\n{dumps(value, indent=2)}\n```"]
        formatted_result = (
            f"\n > Reported records for `'{record_identifier}'` in `'{pipeline_name}'` {nl} "
            + f"{nl} {(nl).join(rep_strs)}"
        )
    return formatted_result


def default_formatter(
    pipeline_name: str, record_identifier: str, res_id: str, value: Any
) -> str:
    """Returns formatted value as string.

    Args:
        pipeline_name: Name of the pipeline.
        record_identifier: Identifier of the record.
        res_id: Result identifier.
        value: Value to format.

    Returns:
        str: Formatted result.
    """
    # Assume default method desired
    nl = "\n"
    rep_strs = [f"{res_id}: {value}"]
    formatted_result = (
        f"Reported records for '{record_identifier}' in '{pipeline_name}' "
        + f":{nl} - {(nl + ' - ').join(rep_strs)}"
    )
    return formatted_result


def force_symlink(file1: str, file2: str) -> None:
    """Create a symlink between two files.

    Args:
        file1: Source file path.
        file2: Target file path for the symlink.
    """
    try:
        os.symlink(file1, file2)
    except OSError as e:
        if e.errno == errno.EEXIST:
            _LOGGER.warning(
                f"Symlink collision detected for {file1} and {file2}. Overwriting symlink."
            )
            os.remove(file2)
            os.symlink(file1, file2)


def get_all_result_files(results_file_path: str) -> List[str]:
    """Collects any yaml result files relative to the CURRENT results_file_path.

    Args:
        results_file_path (str): Path to the pipestatmanager's current result_file.

    Returns:
        list: List of yaml result file paths.
    """
    files = glob.glob(results_file_path + "**/*.yaml")

    return files


def zip_report(report_dir_name: str) -> Optional[str]:
    """Walks through files and attempts to zip them into a Zip object using default compression.

    Gracefully fails and informs user if compression library is not available.

    Args:
        report_dir_name (str): Directory name of report directory.

    Returns:
        str | None: Path to the zip file if successful, None otherwise.
    """

    zip_file_name = f"{report_dir_name}_report_portable"

    try:
        make_archive(zip_file_name, "zip", report_dir_name)
    except RuntimeError as e:
        _LOGGER.warning("Report zip file not created! \n {e}")

    if os.path.exists(zip_file_name + ".zip"):
        _LOGGER.info(f"Report zip file successfully created: {zip_file_name}.zip")
        return f"{zip_file_name}.zip"
    else:
        _LOGGER.warning("Report zip file not created.")
        return None
