"""Tests for schema error messages, demo schema validity, and infer-schema description field."""

import os
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest
import yaml

from pipestat.exceptions import SchemaError
from pipestat.parsed_schema import ParsedSchema


class TestSchemaErrorMessages:
    def test_missing_pipeline_name_error(self):
        """Schema without pipeline_name raises SchemaError with template."""
        schema = {
            "samples": {
                "my_result": {
                    "type": "string",
                    "description": "A result",
                },
            },
        }
        with pytest.raises(SchemaError, match="pipeline_name") as exc_info:
            ParsedSchema(schema)
        assert "Minimal working schema" in str(exc_info.value)

    def test_pipeline_id_typo_detected(self):
        """Schema with pipeline_id gives a clear error about the rename."""
        schema = {
            "pipeline_id": "my_pipeline",
            "samples": {
                "my_result": {
                    "type": "integer",
                    "description": "A result",
                },
            },
        }
        with pytest.raises(SchemaError, match="did you mean 'pipeline_name'"):
            ParsedSchema(schema)

    def test_missing_description_error(self):
        """Result missing description raises SchemaError with example."""
        schema = {
            "pipeline_name": "test",
            "samples": {
                "bad_result": {
                    "type": "string",
                },
            },
        }
        with pytest.raises(SchemaError, match="description") as exc_info:
            ParsedSchema(schema)
        assert "Example:" in str(exc_info.value)

    def test_missing_type_error(self):
        """Result missing type raises SchemaError with example."""
        schema = {
            "pipeline_name": "test",
            "samples": {
                "bad_result": {
                    "description": "Missing type key",
                },
            },
        }
        with pytest.raises(SchemaError, match="type") as exc_info:
            ParsedSchema(schema)
        assert "Example:" in str(exc_info.value)


class TestDemoSchemaValidity:
    def test_demo_schema_valid(self):
        """The repo-root demo schema parses without error."""
        schema_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "pipestat_schema_demo.yaml",
        )
        parsed = ParsedSchema(schema_path)
        assert parsed.pipeline_name == "demo_pipeline"
        assert "alignment_rate" in parsed.sample_level_data

    def test_test_demo_schema_valid(self):
        """The tests/ demo schema parses without error."""
        schema_path = os.path.join(
            os.path.dirname(__file__),
            "demo_pipestat_output_schema.yaml",
        )
        parsed = ParsedSchema(schema_path)
        assert parsed.pipeline_name == "PEPPRO"
        assert "smooth_bw" in parsed.sample_level_data


class TestInferSchemaDescription:
    def test_inferred_schema_has_description(self):
        """Every result in an inferred schema has a 'description' key."""
        from pipestat.infer import infer_schema

        results_data = {
            "test_pipeline": {
                "sample": {
                    "sample1": {
                        "alignment_rate": 95.5,
                        "read_count": 1000,
                        "sample_name": "sample1",
                    },
                },
            },
        }
        with TemporaryDirectory() as tmp_dir:
            results_file = os.path.join(tmp_dir, "results.yaml")
            with open(results_file, "w") as f:
                yaml.dump(results_data, f)

            schema = infer_schema(results_file=results_file)
            for key, value in schema.get("samples", {}).items():
                assert "description" in value, f"Result '{key}' missing 'description'"
