from pipestat import PipestatManager


class TestSharedSchemaKeys:
    """Tests for schemas with overlapping keys at sample and project levels."""

    def test_sample_psm_returns_sample_level_schemas(self, tmp_path, schema_with_shared_keys):
        """Sample pipeline manager exposes only sample-level result schemas."""
        psm = PipestatManager(
            pipeline_type="sample",
            schema_path=schema_with_shared_keys,
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # result_schemas should contain sample-level keys only
        assert "sample_only_result" in psm.result_schemas
        assert "project_only_result" not in psm.result_schemas

        # Shared keys should use sample-level definitions
        assert "Time" in psm.result_schemas
        assert psm.result_schemas["Time"]["description"] == "Sample runtime"

    def test_project_psm_returns_project_level_schemas(self, tmp_path, schema_with_shared_keys):
        """Project pipeline manager exposes only project-level result schemas."""
        psm = PipestatManager(
            pipeline_type="project",
            schema_path=schema_with_shared_keys,
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        # result_schemas should contain project-level keys only
        assert "project_only_result" in psm.result_schemas
        assert "sample_only_result" not in psm.result_schemas

        # Shared keys should use project-level definitions
        assert "Time" in psm.result_schemas
        assert psm.result_schemas["Time"]["description"] == "Project runtime"

    def test_report_shared_result_from_project_pipeline(self, tmp_path, schema_with_shared_keys):
        """Project-level pipeline can report results that also exist at sample level."""
        psm = PipestatManager(
            pipeline_type="project",
            schema_path=schema_with_shared_keys,
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
            record_identifier="project_record",
        )

        # This should NOT raise - Time is defined at project level
        psm.report(record_identifier="project_record", values={"Time": "00:05:23"})
        psm.report(record_identifier="project_record", values={"Success": "02-12-14:30:00"})

        # Results should be retrievable
        results = psm.retrieve_one(record_identifier="project_record")
        assert results["Time"] == "00:05:23"
        assert results["Success"] == "02-12-14:30:00"

    def test_all_result_schemas_returns_both_levels(self, tmp_path, schema_with_shared_keys):
        """all_result_schemas returns schemas organized by level."""
        psm = PipestatManager(
            pipeline_type="sample",
            schema_path=schema_with_shared_keys,
            results_file_path=str(tmp_path / "results.yaml"),
            pipeline_name="test",
        )

        all_schemas = psm.all_result_schemas
        assert "sample" in all_schemas
        assert "project" in all_schemas
        assert "sample_only_result" in all_schemas["sample"]
        assert "project_only_result" in all_schemas["project"]
        assert "Time" in all_schemas["sample"]
        assert "Time" in all_schemas["project"]
