"""Tests for improved initialization error messages and schema-optional mode."""

import os
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
from yaml import dump

from pipestat import PipestatManager, SamplePipestatManager
from pipestat.exceptions import NoBackendSpecifiedError, SchemaError
from pipestat.parsed_schema import ParsedSchema


class TestNoBackendSpecifiedError:
    def test_no_backend_error_has_message(self):
        """NoBackendSpecifiedError raised with no args has a helpful default message."""
        with pytest.raises(NoBackendSpecifiedError, match="results_file_path"):
            raise NoBackendSpecifiedError()

    def test_no_backend_error_mentions_config(self):
        """Default message mentions config_file option."""
        with pytest.raises(NoBackendSpecifiedError, match="config_file"):
            raise NoBackendSpecifiedError()

    def test_no_backend_error_mentions_pephub(self):
        """Default message mentions pephub_path option."""
        with pytest.raises(NoBackendSpecifiedError, match="pephub_path"):
            raise NoBackendSpecifiedError()

    def test_no_backend_error_custom_message(self):
        """Custom message overrides the default."""
        with pytest.raises(NoBackendSpecifiedError, match="custom error"):
            raise NoBackendSpecifiedError("custom error")

    def test_manager_no_backend_raises_with_message(self):
        """PipestatManager() with no backend raises NoBackendSpecifiedError with message."""
        with pytest.raises(NoBackendSpecifiedError, match="results_file_path"):
            PipestatManager(pipeline_name="test", validate_results=False)


class TestPipelineIdDetection:
    def test_pipeline_id_old_style_schema(self):
        """Schema with pipeline_id (old key) gives a clear error about renaming."""
        schema_data = {
            "pipeline_id": "my_pipeline",
            "samples": {
                "my_result": {
                    "type": "integer",
                    "description": "A result",
                },
            },
        }
        with pytest.raises(SchemaError, match="did you mean 'pipeline_name'"):
            ParsedSchema(schema_data)

    def test_pipeline_id_json_schema_format(self):
        """Schema with pipeline_id in properties gives a clear error about renaming."""
        schema_data = {
            "properties": {
                "pipeline_id": "my_pipeline",
                "samples": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "my_result": {
                                "type": "integer",
                                "description": "A result",
                            },
                        },
                    },
                },
            },
        }
        with pytest.raises(SchemaError, match="did you mean 'pipeline_name'"):
            ParsedSchema(schema_data)

    def test_missing_pipeline_name_gives_helpful_error(self):
        """Schema with no pipeline_name at all gives improved error."""
        schema_data = {
            "samples": {
                "my_result": {
                    "type": "integer",
                    "description": "A result",
                },
            },
        }
        with pytest.raises(SchemaError, match="pipeline_name"):
            ParsedSchema(schema_data)


class TestResolveResultsFilePathTypeError:
    def test_non_string_path_raises_type_error(self):
        """Passing a non-string results_file_path raises TypeError (not AssertionError)."""
        with TemporaryDirectory() as tmp_dir:
            schema_content = {
                "pipeline_name": "test_pipeline",
                "samples": {
                    "my_result": {
                        "type": "integer",
                        "description": "A test result",
                    },
                },
            }
            schema_file = os.path.join(tmp_dir, "schema.yaml")
            with open(schema_file, "w") as f:
                dump(schema_content, f)

            with pytest.raises(TypeError, match="results_file_path must be a string"):
                SamplePipestatManager(
                    results_file_path=123,
                    schema_path=schema_file,
                )


class TestSchemaOptionalMode:
    def test_schema_optional_create(self):
        """PipestatManager can be created without a schema when validate_results=False."""
        with TemporaryDirectory() as tmp_dir:
            results_file = os.path.join(tmp_dir, "results.yaml")
            psm = PipestatManager(
                results_file_path=results_file,
                pipeline_name="test_pipeline",
                validate_results=False,
            )
            assert psm is not None
            assert psm.pipeline_name == "test_pipeline"

    def test_schema_optional_report_and_retrieve(self):
        """Schema-optional mode allows reporting and retrieving arbitrary results."""
        with TemporaryDirectory() as tmp_dir:
            results_file = os.path.join(tmp_dir, "results.yaml")
            psm = PipestatManager(
                results_file_path=results_file,
                pipeline_name="test_pipeline",
                validate_results=False,
            )
            psm.report(
                record_identifier="sample1",
                values={"arbitrary_key": 42, "another_key": "hello"},
            )
            result = psm.retrieve_one(record_identifier="sample1")
            assert result["arbitrary_key"] == 42
            assert result["another_key"] == "hello"
