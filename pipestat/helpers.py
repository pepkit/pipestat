"""Assorted project utilities"""

import logging
import glob
import os
import errno

import jsonschema
from json import dumps
from pathlib import Path
from shutil import make_archive
from typing import Any, Dict, Optional, Tuple, Union, List

from oyaml import safe_load, dump
from ubiquerg import expandpath

from zipfile import ZipFile, ZIP_DEFLATED

from .const import (
    PIPESTAT_GENERIC_CONFIG,
    SCHEMA_PROP_KEY,
    SCHEMA_TYPE_KEY,
    CLASSES_BY_TYPE,
)

_LOGGER = logging.getLogger(__name__)


def validate_type(value, schema, strict_type=False):
    """
    Validate reported result against a partial schema, in case of failure try
    to cast the value into the required class.

    Does not support objects of objects.

    :param any value: reported value
    :param dict schema: partial jsonschema schema to validate
        against, e.g. {"type": "integer"}
    :param bool strict_type: whether the value should validate as is
    """

    try:
        jsonschema.validate(value, schema)
    except jsonschema.exceptions.ValidationError as e:
        if strict_type:
            raise
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


def read_yaml_data(path: Union[str, Path], what: str) -> Tuple[str, Dict[str, Any]]:
    """
    Safely read YAML file and log message

    :param str path: YAML file to read
    :param str what: context
    :return (str, dict): absolute path to the read file and the read data
    """
    if isinstance(path, Path):
        test = lambda p: p.is_file()
    elif isinstance(path, str):
        path = expandpath(path)
        test = os.path.isfile
    else:
        raise TypeError(f"Alleged path to YAML file to read is neither path nor string: {path}")
    assert test(path), FileNotFoundError(f"File not found: {path}")
    _LOGGER.debug(f"Reading {what} from '{path}'")
    with open(path, "r") as f:
        return path, safe_load(f)


def mk_list_of_str(x):
    """
    Make sure the input is a list of strings
    :param str | list[str] | falsy x: input to covert
    :return list[str]: converted input
    :raise TypeError: if the argument cannot be converted
    """
    if not x or isinstance(x, list):
        return x
    if isinstance(x, str):
        return [x]
    raise TypeError(
        f"String or list of strings required as input. Got: " f"{x.__class__.__name__}"
    )


def mk_abs_via_cfg(
    path: Optional[str],
    cfg_path: Optional[str],
) -> Optional[str]:
    """
    Helper function to ensure a path is absolute.

    Assumes a relative path is relative to cfg_path, or to current working directory if cfg_path is None.

    : param str path: The path to make absolute.
    : param str cfg_path: Relative paths will be relative the containing folder of this pat
    """
    if path is None:
        return path
    assert isinstance(path, str), TypeError("Path is expected to be a str")
    if os.path.isabs(path):
        return path
    if cfg_path is None:
        rel_to_cwd = os.path.join(os.getcwd(), path)
        try:
            os.makedirs(os.path.dirname(rel_to_cwd))
        except FileExistsError:
            pass
        if os.path.exists(rel_to_cwd) or os.access(os.path.dirname(rel_to_cwd), os.W_OK):
            return rel_to_cwd
        else:
            raise OSError(f"File not found: {path}")
    joined = os.path.join(os.path.dirname(cfg_path), path)
    try:
        os.makedirs(os.path.dirname(joined))
    except FileExistsError:
        pass
    if os.path.isabs(joined):
        return joined
    raise OSError(f"Could not make this path absolute: {path}")


def init_generic_config():
    """
    Create generic config file for DB Backend
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


def markdown_formatter(pipeline_name, record_identifier, res_id, value) -> str:
    """
    Returns Markdown formatted value as string
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


def default_formatter(pipeline_name, record_identifier, res_id, value) -> str:
    """
    Returns formatted value as string
    """
    # Assume default method desired
    nl = "\n"
    rep_strs = [f"{res_id}: {value}"]
    formatted_result = (
        f"Reported records for '{record_identifier}' in '{pipeline_name}' "
        + f":{nl} - {(nl + ' - ').join(rep_strs)}"
    )
    return formatted_result


def force_symlink(file1, file2):
    """Create a symlink between two files."""
    try:
        os.symlink(file1, file2)
    except OSError as e:
        if e.errno == errno.EEXIST:
            _LOGGER.warning(
                f"Symlink collision detected for {file1} and {file2}. Overwriting symlink."
            )
            os.remove(file2)
            os.symlink(file1, file2)


def get_all_result_files(results_file_path: str) -> List:
    """
    Collects any yaml result files relative to the CURRENT results_file_path
    :param str results_file_path: path to the pipestamanager's current result_file
    :return: list
    """
    files = glob.glob(results_file_path + "**/*.yaml")

    return files


def zip_report(report_dir_name: str):
    """

    Walks through files and attempts to zip them into a Zip object using default compression.
    Gracefully fails and informs user if compression library is not available.

    :param report_dir_name: directory name of report directory
    :return: None
    """

    zip_file_name = f"{report_dir_name}_report_portable"

    try:
        make_archive(zip_file_name, "zip", report_dir_name)
    except RuntimeError as e:
        _LOGGER.warning("Report zip file not created! \n {e}")

    if os.path.exists(zip_file_name + ".zip"):
        _LOGGER.info(f"Report zip file successfully created: {zip_file_name}")
    else:
        _LOGGER.warning("Report zip file not created.")
