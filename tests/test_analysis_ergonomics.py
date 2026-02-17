"""Tests for analysis ergonomics features: validation modes, optional thumbnail, infer-schema."""

import os

import pytest

from pipestat import PipestatManager
from pipestat.exceptions import ColumnNotFoundError, PipestatDatabaseError, SchemaNotFoundError
from pipestat.infer import infer_schema


class TestOptionalThumbnail:
    """Tests for optional thumbnail_path in image results."""

    def test_report_image_without_thumbnail(self, tmp_path):
        """User can report an image without providing a separate thumbnail."""
        # Create a minimal schema with an image result using type: image
        # which triggers CANONICAL_TYPES replacement where thumbnail_path is optional
        schema_content = """
pipeline_name: test_pipeline
samples:
  my_image:
    type: image
    description: Test image result
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # Report image with only path and title (no thumbnail_path)
        psm.report(
            record_identifier="sample1",
            values={
                "my_image": {
                    "path": "plots/figure1.png",
                    "title": "Figure 1",
                }
            },
        )

        result = psm.retrieve_one("sample1")
        assert result["my_image"]["path"] == "plots/figure1.png"
        assert "thumbnail_path" not in result["my_image"]  # not required


class TestSchemaFreeMode:
    """Tests for schema-free mode (validate_results=False)."""

    def test_schema_free_allows_unschema_results(self, tmp_path):
        """User can report results without a schema when validate_results=False."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,  # no schema provided
        )

        # Report arbitrary results - should not raise
        psm.report(
            record_identifier="sample1", values={"custom_metric": 42, "notes": "looks good"}
        )

        result = psm.retrieve_one("sample1")
        assert result["custom_metric"] == 42
        assert result["notes"] == "looks good"

    def test_schema_free_auto_wraps_image_paths(self, tmp_path):
        """validate_results=False auto-wraps image file paths as image objects."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        # Report a string path to an image - should be auto-wrapped
        psm.report(record_identifier="sample1", values={"my_plot": "figures/plot.png"})

        result = psm.retrieve_one("sample1")
        assert result["my_plot"]["path"] == "figures/plot.png"  # was wrapped
        assert result["my_plot"]["title"] == "my_plot"

    def test_schema_free_auto_wraps_file_paths(self, tmp_path):
        """validate_results=False auto-wraps file paths as file objects."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        # Report a string path to a CSV - should be auto-wrapped
        psm.report(record_identifier="sample1", values={"data_file": "output/data.csv"})

        result = psm.retrieve_one("sample1")
        assert result["data_file"]["path"] == "output/data.csv"  # was wrapped
        assert result["data_file"]["title"] == "data_file"

    def test_schema_free_allows_type_variance(self, tmp_path):
        """Same result key can have different types across records when validate_results=False."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        # Same key, different types - both allowed
        psm.report(record_identifier="sample1", values={"status": "completed"})
        psm.report(record_identifier="sample2", values={"status": {"code": 0, "message": "done"}})

        assert psm.retrieve_one("sample1")["status"] == "completed"
        assert psm.retrieve_one("sample2")["status"]["code"] == 0

    def test_schema_free_with_schema_allows_extra_results(self, tmp_path):
        """With schema + validate_results=False, can report both defined and extra results."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  defined_result:
    type: integer
    description: A defined result
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        # Report both defined and undefined results
        psm.report(
            record_identifier="sample1",
            values={"defined_result": 42, "extra_result": "bonus"},
        )

        result = psm.retrieve_one("sample1")
        assert result["defined_result"] == 42
        assert result["extra_result"] == "bonus"

    def test_strict_mode_requires_schema(self, tmp_path):
        """With validate_results=True (default), reporting without schema raises error."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=True,
        )

        with pytest.raises(SchemaNotFoundError):
            psm.report(record_identifier="sample1", values={"custom_metric": 42})


class TestInferSchema:
    """Tests for schema inference from results file."""

    def test_infer_schema_from_schema_free_results(self, tmp_path):
        """User can generate a schema from results created with validate_results=False."""
        # Create results with validate_results=False
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )
        psm.report(record_identifier="s1", values={"count": 42, "name": "sample1"})
        psm.report(record_identifier="s2", values={"count": 17, "name": "sample2"})

        # Infer schema (auto-detects sample level)
        schema = infer_schema(str(tmp_path / "results.yaml"))

        assert "samples" in schema  # auto-detected
        assert schema["samples"]["count"]["type"] == "integer"
        assert schema["samples"]["name"]["type"] == "string"

    def test_infer_schema_detects_image_type(self, tmp_path):
        """Infer-schema correctly identifies image objects."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )
        psm.report(record_identifier="s1", values={"plot": "fig.png"})  # auto-wrapped

        schema = infer_schema(str(tmp_path / "results.yaml"))

        assert schema["samples"]["plot"]["type"] == "object"
        assert schema["samples"]["plot"]["object_type"] == "image"

    def test_infer_schema_detects_file_type(self, tmp_path):
        """Infer-schema correctly identifies file objects."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )
        psm.report(record_identifier="s1", values={"data": "output.csv"})  # auto-wrapped

        schema = infer_schema(str(tmp_path / "results.yaml"))

        assert schema["samples"]["data"]["type"] == "object"
        assert schema["samples"]["data"]["object_type"] == "file"

    def test_infer_schema_handles_type_conflicts(self, tmp_path):
        """Infer-schema resolves type conflicts by using most common type."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )
        # 3 strings, 1 integer - string should win
        psm.report(record_identifier="s1", values={"status": "done"})
        psm.report(record_identifier="s2", values={"status": "running"})
        psm.report(record_identifier="s3", values={"status": "failed"})
        psm.report(record_identifier="s4", values={"status": 0})  # different type

        schema = infer_schema(str(tmp_path / "results.yaml"))

        assert schema["samples"]["status"]["type"] == "string"  # most common

    def test_infer_schema_writes_to_file(self, tmp_path):
        """Infer-schema writes schema to output file."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )
        psm.report(record_identifier="s1", values={"count": 42})

        output_file = str(tmp_path / "inferred_schema.yaml")
        infer_schema(str(tmp_path / "results.yaml"), output_file=output_file)

        assert os.path.exists(output_file)
        with open(output_file) as f:
            content = f.read()
        assert "count" in content
        assert "integer" in content

    def test_infer_schema_auto_detects_both_levels(self, tmp_path):
        """Infer-schema generates both sample and project sections if both have data."""
        # Create sample-level results
        sample_psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",
            validate_results=False,
        )
        sample_psm.report(record_identifier="s1", values={"read_count": 1000})

        # Create project-level results
        project_psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="project",
            validate_results=False,
        )
        project_psm.report(record_identifier="project", values={"total_reads": 5000})

        # Infer schema - should detect both levels
        schema = infer_schema(str(tmp_path / "results.yaml"))

        assert "samples" in schema
        assert "project" in schema
        assert schema["samples"]["read_count"]["type"] == "integer"
        assert schema["project"]["total_reads"]["type"] == "integer"


class TestValidationModes:
    """Tests for validate and additional_properties parameters."""

    def test_default_validates_schema_items(self, tmp_path):
        """Schema-defined results are validated by default (validate=True)."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # Report valid integer - should work
        psm.report(record_identifier="s1", values={"read_count": 1000})
        assert psm.retrieve_one("s1")["read_count"] == 1000

    def test_default_allows_additional_properties(self, tmp_path):
        """Additional properties allowed by default (additional_properties=True)."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # Report both schema-defined and extra results
        psm.report(
            record_identifier="s1",
            values={"read_count": 1000, "dynamic_result": "extra_value"},
        )

        result = psm.retrieve_one("s1")
        assert result["read_count"] == 1000
        assert result["dynamic_result"] == "extra_value"

    def test_additional_properties_false_rejects_extras(self, tmp_path):
        """Strict mode (additional_properties=False) rejects results not in schema."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            additional_properties=False,
        )

        # Try to report undefined result - should raise
        with pytest.raises(ColumnNotFoundError, match="not defined in schema"):
            psm.report(record_identifier="s1", values={"not_in_schema": 123})

    def test_validate_results_false_skips_all_validation(self, tmp_path):
        """validate_results=False skips all validation."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            validate_results=False,
        )

        # Report anything - no schema needed
        psm.report(
            record_identifier="s1",
            values={"anything": "goes", "no": "schema", "count": "not_an_int"},
        )

        result = psm.retrieve_one("s1")
        assert result["anything"] == "goes"
        assert result["no"] == "schema"
        assert result["count"] == "not_an_int"

    def test_validate_results_true_additional_true_is_flexible_default(self, tmp_path):
        """validate_results=True + additional_properties=True is the flexible default."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            # These are the defaults, but be explicit
            validate_results=True,
            additional_properties=True,
        )

        # Schema-defined result is validated
        psm.report(record_identifier="s1", values={"read_count": 1000})

        # Extra result is allowed
        psm.report(record_identifier="s2", values={"dynamic_result": "allowed"})

        assert psm.retrieve_one("s1")["read_count"] == 1000
        assert psm.retrieve_one("s2")["dynamic_result"] == "allowed"

    def test_validate_results_and_additional_properties_properties(self, tmp_path):
        """Properties validate_results and additional_properties return correct values."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  value:
    type: integer
    description: A value
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        # Default values
        psm1 = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results1.yaml"),
            pipeline_name="test",
        )
        assert psm1.validate_results is True
        assert psm1.additional_properties is True

        # Custom values
        psm2 = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results2.yaml"),
            pipeline_name="test",
            validate_results=False,
            additional_properties=False,
        )
        assert psm2.validate_results is False
        assert psm2.additional_properties is False

    def test_schema_level_additional_properties_false(self, tmp_path):
        """Schema-level additionalProperties: false is respected."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  additionalProperties: false
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            # additional_properties is None (default) - should use schema's value
        )

        # Schema value should be used
        assert psm.additional_properties is False

        # Extra properties should be rejected
        with pytest.raises(ColumnNotFoundError, match="not defined in schema"):
            psm.report(record_identifier="s1", values={"extra_field": "value"})

    def test_schema_level_additional_properties_true(self, tmp_path):
        """Schema-level additionalProperties: true allows extra properties."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  additionalProperties: true
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # Schema value should be used
        assert psm.additional_properties is True

        # Extra properties should be allowed
        psm.report(record_identifier="s1", values={"read_count": 100, "extra": "allowed"})
        result = psm.retrieve_one("s1")
        assert result["extra"] == "allowed"

    def test_additional_properties_override_takes_precedence(self, tmp_path):
        """PSM additional_properties parameter overrides schema value."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  additionalProperties: false
  read_count:
    type: integer
    description: Number of reads
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        # Override schema's false with True
        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            additional_properties=True,  # Override
        )

        # Override should take effect
        assert psm.additional_properties is True

        # Extra properties allowed despite schema saying false
        psm.report(record_identifier="s1", values={"extra": "overridden"})
        assert psm.retrieve_one("s1")["extra"] == "overridden"

    def test_per_level_additional_properties(self, tmp_path):
        """Different levels can have different additionalProperties settings."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  additionalProperties: false
  sample_val:
    type: integer
    description: Sample value
project:
  additionalProperties: true
  project_val:
    type: integer
    description: Project value
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        # Sample level PSM - should NOT allow extras
        sample_psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",
        )
        assert sample_psm.additional_properties is False

        # Project level PSM - SHOULD allow extras
        project_psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="project",
        )
        assert project_psm.additional_properties is True

        # Sample extras rejected
        with pytest.raises(ColumnNotFoundError):
            sample_psm.report(record_identifier="s1", values={"extra": "rejected"})

        # Project extras allowed
        project_psm.report(record_identifier="proj", values={"project_val": 1, "extra": "allowed"})
        assert project_psm.retrieve_one("proj")["extra"] == "allowed"


class TestDualLevelReporting:
    """Tests for dual-level reporting via level parameter."""

    def test_report_with_level_parameter(self, tmp_path):
        """report() accepts level parameter to switch between sample and project."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  sample_metric:
    type: integer
    description: A sample metric
project:
  project_metric:
    type: integer
    description: A project metric
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",  # default to sample
        )

        # Report to sample level (default)
        psm.report(record_identifier="s1", values={"sample_metric": 100})

        # Report to project level via level parameter
        psm.report(record_identifier="summary", values={"project_metric": 500}, level="project")

        # Verify sample result
        assert "s1" in psm.data["test"]["sample"]
        assert psm.data["test"]["sample"]["s1"]["sample_metric"] == 100

        # Verify project result
        assert "summary" in psm.data["test"]["project"]
        assert psm.data["test"]["project"]["summary"]["project_metric"] == 500

    def test_retrieve_with_level_parameter(self, tmp_path):
        """retrieve_one() accepts level parameter to switch between sample and project."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  sample_metric:
    type: integer
    description: A sample metric
project:
  project_metric:
    type: integer
    description: A project metric
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",
        )

        # Report to both levels
        psm.report(record_identifier="s1", values={"sample_metric": 100})
        psm.report(record_identifier="proj1", values={"project_metric": 500}, level="project")

        # Retrieve from sample level (default)
        sample_result = psm.retrieve_one(record_identifier="s1")
        assert sample_result["sample_metric"] == 100

        # Retrieve from project level via level parameter
        project_result = psm.retrieve_one(record_identifier="proj1", level="project")
        assert project_result["project_metric"] == 500

    def test_select_records_with_level_parameter(self, tmp_path):
        """select_records() accepts level parameter to switch between sample and project."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  sample_metric:
    type: integer
    description: A sample metric
project:
  project_metric:
    type: integer
    description: A project metric
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",
        )

        # Report to both levels
        psm.report(record_identifier="s1", values={"sample_metric": 100})
        psm.report(record_identifier="s2", values={"sample_metric": 200})
        psm.report(record_identifier="proj1", values={"project_metric": 500}, level="project")

        # Select from sample level (default)
        sample_records = psm.select_records()
        assert sample_records["total_size"] == 2

        # Select from project level via level parameter
        project_records = psm.select_records(level="project")
        assert project_records["total_size"] == 1

    def test_remove_with_level_parameter(self, tmp_path):
        """remove() accepts level parameter to switch between sample and project."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  sample_metric:
    type: integer
    description: A sample metric
project:
  project_metric:
    type: integer
    description: A project metric
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",
        )

        # Report to both levels
        psm.report(record_identifier="s1", values={"sample_metric": 100})
        psm.report(record_identifier="proj1", values={"project_metric": 500}, level="project")

        # Remove from project level via level parameter
        psm.remove(record_identifier="proj1", level="project")

        # Verify sample still exists
        assert "s1" in psm.data["test"]["sample"]
        # Verify project was removed
        assert "proj1" not in psm.data["test"]["project"]

    def test_original_report_unchanged(self, tmp_path):
        """report() without level parameter uses pipeline_type from init."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  sample_metric:
    type: integer
    description: A sample metric
project:
  project_metric:
    type: integer
    description: A project metric
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        # Init with project type
        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="project",
        )

        # Report without level - should go to project (from init)
        psm.report(record_identifier="proj1", values={"project_metric": 500})

        # Verify went to project
        assert "proj1" in psm.data["test"]["project"]
        assert psm.data["test"]["sample"] == {}

    def test_level_parameter_restores_after_call(self, tmp_path):
        """The level swap is temporary - original pipeline_type is restored after call."""
        schema_content = """
pipeline_name: test_pipeline
samples:
  sample_metric:
    type: integer
    description: A sample metric
project:
  project_metric:
    type: integer
    description: A project metric
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="sample",
        )

        # Verify initial type
        assert psm.pipeline_type == "sample"

        # Report with level override
        psm.report(record_identifier="proj1", values={"project_metric": 500}, level="project")

        # Type should be restored
        assert psm.pipeline_type == "sample"

        # Next report without level should go to sample
        psm.report(record_identifier="s1", values={"sample_metric": 100})
        assert "s1" in psm.data["test"]["sample"]


class TestGalleryMode:
    """Tests for gallery mode in summarize()."""

    def test_summarize_gallery_mode_call(self, tmp_path):
        """User can call summarize with gallery mode without error."""
        # Create a minimal schema
        schema_content = """
pipeline_name: test_pipeline
samples:
  value:
    type: integer
    description: A test value
"""
        schema_file = tmp_path / "schema.yaml"
        schema_file.write_text(schema_content)

        psm = PipestatManager(
            schema_path=str(schema_file),
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )
        psm.report(record_identifier="s1", values={"value": 42})

        # Call summarize with gallery mode - should not raise
        report_path = psm.summarize(mode="gallery")
        assert report_path is not None
        assert os.path.exists(report_path)
