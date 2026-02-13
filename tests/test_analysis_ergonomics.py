"""Tests for analysis ergonomics features: lenient mode, optional thumbnail, infer-schema."""

import os

import pytest

from pipestat import PipestatManager
from pipestat.exceptions import PipestatDatabaseError, SchemaNotFoundError
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


class TestLenientMode:
    """Tests for lenient mode (schema-optional reporting)."""

    def test_lenient_mode_allows_unschema_results(self, tmp_path):
        """User can report results without a schema in lenient mode."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            lenient=True,  # no schema provided
        )

        # Report arbitrary results - should not raise
        psm.report(
            record_identifier="sample1", values={"custom_metric": 42, "notes": "looks good"}
        )

        result = psm.retrieve_one("sample1")
        assert result["custom_metric"] == 42
        assert result["notes"] == "looks good"

    def test_lenient_mode_auto_wraps_image_paths(self, tmp_path):
        """Lenient mode auto-wraps image file paths as image objects."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            lenient=True,
        )

        # Report a string path to an image - should be auto-wrapped
        psm.report(record_identifier="sample1", values={"my_plot": "figures/plot.png"})

        result = psm.retrieve_one("sample1")
        assert result["my_plot"]["path"] == "figures/plot.png"  # was wrapped
        assert result["my_plot"]["title"] == "my_plot"

    def test_lenient_mode_auto_wraps_file_paths(self, tmp_path):
        """Lenient mode auto-wraps file paths as file objects."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            lenient=True,
        )

        # Report a string path to a CSV - should be auto-wrapped
        psm.report(record_identifier="sample1", values={"data_file": "output/data.csv"})

        result = psm.retrieve_one("sample1")
        assert result["data_file"]["path"] == "output/data.csv"  # was wrapped
        assert result["data_file"]["title"] == "data_file"

    def test_lenient_mode_allows_type_variance(self, tmp_path):
        """Same result key can have different types across records in lenient mode."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            lenient=True,
        )

        # Same key, different types - both allowed
        psm.report(record_identifier="sample1", values={"status": "completed"})
        psm.report(record_identifier="sample2", values={"status": {"code": 0, "message": "done"}})

        assert psm.retrieve_one("sample1")["status"] == "completed"
        assert psm.retrieve_one("sample2")["status"]["code"] == 0

    def test_lenient_mode_with_schema_allows_extra_results(self, tmp_path):
        """With schema + lenient mode, can report both schema-defined and extra results."""
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
            lenient=True,
        )

        # Report both defined and undefined results
        psm.report(
            record_identifier="sample1",
            values={"defined_result": 42, "extra_result": "bonus"},
        )

        result = psm.retrieve_one("sample1")
        assert result["defined_result"] == 42
        assert result["extra_result"] == "bonus"

    def test_lenient_requires_file_backend(self, tmp_path):
        """Lenient mode raises error if database backend is requested."""
        # Without a results file and without config, this will fail anyway,
        # but the lenient check should be first
        with pytest.raises(PipestatDatabaseError, match="Lenient mode requires file backend"):
            PipestatManager(
                pipeline_name="test",
                lenient=True,
                # No results_file_path - would try to use DB backend
            )

    def test_strict_mode_requires_schema(self, tmp_path):
        """Without lenient mode, reporting without schema raises error."""
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            lenient=False,
        )

        with pytest.raises(SchemaNotFoundError):
            psm.report(record_identifier="sample1", values={"custom_metric": 42})


class TestInferSchema:
    """Tests for schema inference from results file."""

    def test_infer_schema_from_lenient_results(self, tmp_path):
        """User can generate a schema from results created in lenient mode."""
        # Create results in lenient mode
        psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            lenient=True,
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
            lenient=True,
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
            lenient=True,
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
            lenient=True,
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
            lenient=True,
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
            lenient=True,
        )
        sample_psm.report(record_identifier="s1", values={"read_count": 1000})

        # Create project-level results
        project_psm = PipestatManager(
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            pipeline_type="project",
            lenient=True,
        )
        project_psm.report(record_identifier="project", values={"total_reads": 5000})

        # Infer schema - should detect both levels
        schema = infer_schema(str(tmp_path / "results.yaml"))

        assert "samples" in schema
        assert "project" in schema
        assert schema["samples"]["read_count"]["type"] == "integer"
        assert schema["project"]["total_reads"]["type"] == "integer"


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
