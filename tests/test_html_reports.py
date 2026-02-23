import glob
import os
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pytest

from pipestat import (
    ProjectPipestatManager,
    SamplePipestatManager,
)
from pipestat.exceptions import PipestatSummarizeError
from pipestat.parsed_schema import ParsedSchema

from .conftest import (
    DB_DEPENDENCIES,
    DB_URL,
    SERVICE_UNAVAILABLE,
    get_data_file_path,
)
from .test_db_only_mode import ContextManagerDBTesting

pytestmark = [
    pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies"),
    pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available"),
]


class TestHTMLReport:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_samples_html_report(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in values_sample:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v)
                    psm.set_status(record_identifier=r, status_identifier="running")

            htmlreportpath = psm.summarize(amendment="")
            assert htmlreportpath is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_exception_samples_html_report(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            with pytest.raises(PipestatSummarizeError):
                _ = psm.summarize(amendment="")

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_project_html_report(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            # project level
            psm = ProjectPipestatManager(**args)

            for i in values_project:
                for r, v in i.items():
                    psm.report(
                        record_identifier=r,
                        values=v,
                    )
                    psm.set_status(record_identifier=r, status_identifier="running")

            htmlreportpath = psm.summarize(amendment="")
            assert htmlreportpath is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_html_report_portable(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            # project level
            psm = ProjectPipestatManager(**args)

            for i in values_project:
                for r, v in i.items():
                    psm.report(
                        record_identifier=r,
                        values=v,
                    )
                    psm.set_status(record_identifier=r, status_identifier="running")

            htmlreportpath = psm.summarize(amendment="", portable=True)
            assert htmlreportpath is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_zip_html_report_portable(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            # project level
            psm = ProjectPipestatManager(**args)

            for i in values_project:
                for r, v in i.items():
                    psm.report(
                        record_identifier=r,
                        values=v,
                    )
                    psm.set_status(record_identifier=r, status_identifier="running")

            htmlreportpath = psm.summarize(amendment="", portable=True)

            directory = os.path.dirname(htmlreportpath)
            zip_files = glob.glob(directory)

            assert len(zip_files) > 0

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_report_spaces_in_record_identifiers(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            # project level
            psm = ProjectPipestatManager(**args)

            for i in values_project:
                for r, v in i.items():
                    psm.report(
                        record_identifier=r,
                        values=v,
                    )
                    psm.set_status(record_identifier=r, status_identifier="running")

            # Add record with sspace in name
            r = "SAMPLE Three WITH SPACES"
            psm.report(
                record_identifier=r,
                values={"name_of_something": "name of something string"},
            )
            psm.set_status(record_identifier=r, status_identifier="completed")
            r = "SAMPLE FOUR WITH Spaces"
            psm.report(
                record_identifier=r,
                values={
                    "output file with spaces": {"path": "here is path", "title": "here is a title"}
                },
            )

            htmlreportpath = psm.summarize(amendment="")

            directory_path = os.path.dirname(htmlreportpath)
            all_files = os.listdir(directory_path)

            assert "sample_three_with_spaces.html" in all_files
            assert "output_file_with_spaces.html" in all_files


class TestTableCreation:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_table_for_samples(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in values_sample:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v)
                    psm.set_status(record_identifier=r, status_identifier="running")

            table_paths = psm.table()
            assert table_paths is not None

    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics_table_for_project(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_project,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = ProjectPipestatManager(**args)

            for i in values_project:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v)
                    psm.set_status(record_identifier=r, status_identifier="running")

            table_paths = psm.table()
            assert table_paths is not None


class TestFileTypeLinking:
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_linking(
        self,
        config_file_path,
        output_schema_as_JSON_schema,
        results_file_path,
        backend,
        values_complex_linking,
    ):
        with (
            NamedTemporaryFile() as f,
            TemporaryDirectory() as temp_dir,
            ContextManagerDBTesting(DB_URL),
        ):
            results_file_path = f.name
            args = dict(schema_path=output_schema_as_JSON_schema)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            schema = ParsedSchema(output_schema_as_JSON_schema)
            print(schema)

            for i in values_complex_linking:
                for r, v in i.items():
                    psm.report(record_identifier=r, values=v)
                    psm.set_status(record_identifier=r, status_identifier="running")

            os.mkdir(temp_dir + "/test_file_links")
            output_dir = get_data_file_path(temp_dir + "/test_file_links")
            try:
                linkdir = psm.link(link_dir=output_dir)
            except Exception:
                assert False

            # Test simple
            for root, dirs, files in os.walk(os.path.join(linkdir, "output_file")):
                assert "sample1_output_file_ex1.txt" in files
            # Test complex types
            for root, dirs, files in os.walk(os.path.join(linkdir, "output_file_in_object")):
                assert "sample2_example_property_1_ex1.txt" in files

            for root, dirs, files in os.walk(os.path.join(linkdir, "output_file_nested_object")):
                # TODO This example will have collision if the file names and property names are the same
                print(files)
