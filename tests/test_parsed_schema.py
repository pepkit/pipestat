"""Tests of the parsed schema class"""

from functools import partial
import os
from pathlib import Path
from typing import *
import pytest
import oyaml
from pipestat.exceptions import SchemaError
from pipestat.parsed_schema import ParsedSchema
from .conftest import DATA_PATH

TEMP_SCHEMA_FILENAME = "schema.tmp.yaml"


def write_yaml(data: Mapping[str, Any], path: Path) -> Path:
    with open(path, "w") as fH:
        oyaml.dump(data, fH)
    return path


# This is to mirror the signature of write_yaml, serving schema as dict.
def echo_data(data: Mapping[str, Any], path: Path) -> Mapping[str, Any]:
    return data


def read_yaml(path: Union[str, Path]) -> Dict[str, Any]:
    with open(path, "r") as fh:
        return oyaml.safe_load(fh)


@pytest.fixture(scope="function", params=[lambda p: p, read_yaml])
def prepare_schema_from_file(request):
    return request.param


@pytest.fixture(scope="function", params=[echo_data, write_yaml])
def prepare_schema_from_mapping(request, tmp_path):
    func = request.param
    path = tmp_path / TEMP_SCHEMA_FILENAME
    return partial(func, path=path)


def test_empty__fails_with_missing_pipeline_id(prepare_schema_from_mapping):
    schema = prepare_schema_from_mapping({})
    with pytest.raises(SchemaError):
        ParsedSchema(schema)


PROJECT_ATTR = "project_level_data"
SAMPLES_ATTR = "sample_level_data"
STATUS_ATTR = "status_data"
NULL_SCHEMA_DATA = {}

STATUS_DATA = {
    "running": {
        "description": "the pipeline is running",
        "color": [30, 144, 255],  # dodgerblue
    },
    "completed": {
        "description": "the pipeline has completed",
        "color": [50, 205, 50],  # limegreen
    },
    "failed": {
        "description": "the pipeline has failed",
        "color": [220, 20, 60],  # crimson
    },
    "waiting": {
        "description": "the pipeline is waiting",
        "color": [240, 230, 140],  # khaki
    },
    "partial": {
        "description": "the pipeline stopped before completion point",
        "color": [169, 169, 169],  # darkgray
    },
}

PROJECT_DATA = {
    "percentage_of_things": {"type": "number", "description": "Percentage of things"},
    "name_of_something": {"type": "string", "description": "Name of something"},
    "number_of_things": {
        "type": "integer",
        "description": "Number of things",
    },
    "switch_value": {
        "type": "boolean",
        "description": "Is the switch on of off",
    },
}


SAMPLES_DATA = {
    "smooth_bw": {
        "path": "aligned_{genome}/{sample_name}_smooth.bw",
        "type": "string",
        "description": "A smooth bigwig file",
    },
    "aligned_bam": {
        "path": "aligned_{genome}/{sample_name}_sort.bam",
        "type": "string",
        "description": "A sorted, aligned BAM file",
    },
    "peaks_bed": {
        "path": "peak_calling_{genome}/{sample_name}_peaks.bed",
        "type": "string",
        "description": "Peaks in BED format",
    },
}


EXPECTED_SUBDATA_BY_EXAMPLE_FILE = [
    (
        "sample_output_schema__with_project_without_samples_without_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, NULL_SCHEMA_DATA),
            (STATUS_ATTR, NULL_SCHEMA_DATA),
        ],
    ),
    (
        "sample_output_schema__without_project_with_samples_without_status.yaml",
        [
            (PROJECT_ATTR, NULL_SCHEMA_DATA),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, NULL_SCHEMA_DATA),
        ],
    ),
    (
        "sample_output_schema__without_project_without_samples_with_status.yaml",
        [
            (PROJECT_ATTR, NULL_SCHEMA_DATA),
            (SAMPLES_ATTR, NULL_SCHEMA_DATA),
            (STATUS_ATTR, STATUS_DATA),
        ],
    ),
    (
        "sample_output_schema__with_project_with_samples_without_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, NULL_SCHEMA_DATA),
        ],
    ),
    (
        "sample_output_schema__with_project_without_samples_with_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, NULL_SCHEMA_DATA),
            (STATUS_ATTR, STATUS_DATA),
        ],
    ),
    (
        "sample_output_schema__without_project_with_samples_with_status.yaml",
        [
            (PROJECT_ATTR, NULL_SCHEMA_DATA),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, STATUS_DATA),
        ],
    ),
    (
        "sample_output_schema__with_project_with_samples_with_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, STATUS_DATA),
        ],
    ),
]


@pytest.mark.parametrize(
    ["filename", "attr_name", "expected"],
    [(fn, attr, exp) for fn, attr_exp_pairs in EXPECTED_SUBDATA_BY_EXAMPLE_FILE for attr, exp in attr_exp_pairs],
)
def test_parsed_schema__has_correct_data(
    prepare_schema_from_file, filename, attr_name, expected
):
    data_file = get_test_data_path(filename)
    raw_schema = prepare_schema_from_file(data_file)
    schema = ParsedSchema(raw_schema)
    observed = getattr(schema, attr_name)
    assert observed == expected


def get_test_data_path(filename: str) -> str:
    return os.path.join(DATA_PATH, filename)
