"""Tests of the parsed schema class"""

from functools import partial
from pathlib import Path
from typing import *
import pytest
import oyaml
from pipestat.const import SAMPLE_NAME, STATUS, RECORD_IDENTIFIER
from pipestat.exceptions import SchemaError
from pipestat.parsed_schema import (
    NULL_MAPPING_VALUE,
    ParsedSchema,
    SCHEMA_PIPELINE_NAME_KEY,
)
from .conftest import COMMON_CUSTOM_STATUS_DATA, DEFAULT_STATUS_DATA, get_data_file_path

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


def test_empty__fails_with_missing_pipeline_name(prepare_schema_from_mapping):
    schema = prepare_schema_from_mapping({})
    with pytest.raises(SchemaError):
        ParsedSchema(schema)


PROJECT_ATTR = "project_level_data"
SAMPLES_ATTR = "sample_level_data"
STATUS_ATTR = "status_data"


PROJECT_DATA = {
    "percentage_of_things": {"type": "number", "description": "Percentage of things"},
    "name_of_something": {"type": "string", "description": "Name of something"},
    "number_of_things": {
        "type": "integer",
        "description": "Number of things",
    },
    "switch_value": {
        "type": "boolean",
        "description": "Is the switch on or off",
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
            (SAMPLES_ATTR, NULL_MAPPING_VALUE),
            (STATUS_ATTR, NULL_MAPPING_VALUE),
        ],
    ),
    (
        "sample_output_schema__without_project_with_samples_without_status.yaml",
        [
            (PROJECT_ATTR, NULL_MAPPING_VALUE),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, NULL_MAPPING_VALUE),
        ],
    ),
    (
        "sample_output_schema__with_project_with_samples_without_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, NULL_MAPPING_VALUE),
        ],
    ),
    (
        "sample_output_schema__with_project_without_samples_with_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, NULL_MAPPING_VALUE),
            (STATUS_ATTR, COMMON_CUSTOM_STATUS_DATA),
        ],
    ),
    (
        "sample_output_schema__without_project_with_samples_with_status.yaml",
        [
            (PROJECT_ATTR, NULL_MAPPING_VALUE),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, COMMON_CUSTOM_STATUS_DATA),
        ],
    ),
    (
        "sample_output_schema__with_project_with_samples_with_status.yaml",
        [
            (PROJECT_ATTR, PROJECT_DATA),
            (SAMPLES_ATTR, SAMPLES_DATA),
            (STATUS_ATTR, COMMON_CUSTOM_STATUS_DATA),
        ],
    ),
]


@pytest.mark.parametrize(
    ["filename", "attr_name", "expected"],
    [
        (fn, attr, exp)
        for fn, attr_exp_pairs in EXPECTED_SUBDATA_BY_EXAMPLE_FILE
        for attr, exp in attr_exp_pairs
    ],
)
def test_parsed_schema__has_correct_data_and_print(
    prepare_schema_from_file, filename, attr_name, expected
):
    data_file = get_data_file_path(filename)
    raw_schema = prepare_schema_from_file(data_file)
    schema = ParsedSchema(raw_schema)
    observed = getattr(schema, attr_name)
    assert observed == expected
    try:
        print(str(schema))
    except:
        assert False


SCHEMA_DATA_TUPLES_WITHOUT_PIPELINE_NAME = [
    [("samples", SAMPLES_DATA)],
    [("project", PROJECT_DATA)],
    [("samples", SAMPLES_DATA), ("project", PROJECT_DATA)],
    [("samples", SAMPLES_DATA), ("status", DEFAULT_STATUS_DATA)],
    [("project", PROJECT_DATA), ("status", DEFAULT_STATUS_DATA)],
    [
        ("samples", SAMPLES_DATA),
        ("project", PROJECT_DATA),
        ("status", DEFAULT_STATUS_DATA),
    ],
]


@pytest.mark.parametrize(
    ["schema_data", "expected_message"],
    [
        (
            {SCHEMA_PIPELINE_NAME_KEY: "test_pipe"},
            "Neither sample-level nor project-level data items are declared.",
        ),
        (
            {SCHEMA_PIPELINE_NAME_KEY: "test_pipe", STATUS: DEFAULT_STATUS_DATA},
            "Neither sample-level nor project-level data items are declared.",
        ),
        (
            {SCHEMA_PIPELINE_NAME_KEY: "test_pipe", "samples": ["s1", "s2"]},
            f"sample-level info in schema definition has invalid type: list",
        ),
        (
            {SCHEMA_PIPELINE_NAME_KEY: "test_pipe", "samples": "sample1"},
            f"sample-level info in schema definition has invalid type: str",
        ),
    ]
    + [
        (
            dict(data),
            f"Could not find valid pipeline identifier (key '{SCHEMA_PIPELINE_NAME_KEY}') in given schema data",
        )
        for data in SCHEMA_DATA_TUPLES_WITHOUT_PIPELINE_NAME
    ],
)
def test_insufficient_schema__raises_expected_error_and_message(schema_data, expected_message):
    with pytest.raises(SchemaError) as err_ctx:
        ParsedSchema(schema_data)
    observed_message = str(err_ctx.value)
    assert observed_message == expected_message


SIMPLE_ID_SECTION = [(SCHEMA_PIPELINE_NAME_KEY, "test_pipe")]
SIMPLE_SAMPLES_DATA = [("count", {"type": "integer", "description": "number of things"})]
SIMPLE_PROJECT_DATA = [("pct", {"type": "number", "description": "percentage"})]


@pytest.mark.parametrize(
    "schema_data",
    [
        dict(SIMPLE_ID_SECTION + [(section_name, dict(section_data + extra))])
        for section_name, section_data in [
            ("samples", SIMPLE_SAMPLES_DATA),
            ("project", SIMPLE_PROJECT_DATA),
        ]
        for extra in [
            [("id", {"type": "string", "description": "identifier"})],
            [(RECORD_IDENTIFIER, {"type": "string", "description": "identifier"})],
            [
                ("id", {"type": "string", "description": "identifier"}),
                (RECORD_IDENTIFIER, {"type": "string", "description": "identifier"}),
            ],
        ]
    ],
)
def test_reserved_keyword_use_in_schema__raises_expected_error_and_message(schema_data):
    with pytest.raises(SchemaError) as err_ctx:
        ParsedSchema(schema_data)
    observed_message = str(err_ctx.value)
    assert "reserved keyword(s) used" in observed_message


def test_sample_project_data_item_name_overlap__raises_expected_error_and_message():
    common_key = "shared_key"
    schema_data = {
        SCHEMA_PIPELINE_NAME_KEY: "test_pipe",
        "samples": {
            "just_in_sample": {"type": "string", "description": "placeholder"},
            common_key: {"type": "string", "description": "in samples"},
        },
        "project": {common_key: {"type": "string", "description": "in project"}},
    }
    with pytest.raises(SchemaError) as err_ctx:
        ParsedSchema(schema_data)
    obs_msg = str(err_ctx.value)
    exp_msg = f"Overlap between project- and sample-level keys: {common_key}"
    assert obs_msg == exp_msg


def test_JSON_schema_validation(output_schema_as_JSON_schema):
    schema = ParsedSchema(output_schema_as_JSON_schema)
    assert "number_of_things" in dict(schema.sample_level_data).keys()


def test_JSON_schema_resolved_original(output_schema_as_JSON_schema, output_schema_no_refs):
    # schema with defs and refs
    schema = ParsedSchema(output_schema_as_JSON_schema)
    print(schema.original_schema)
    print(schema.resolved_schema)

    # Schema without refs and defs
    schema2 = ParsedSchema(output_schema_no_refs)
    print(schema2.original_schema)
    print(schema2.resolved_schema)
    print("done")
