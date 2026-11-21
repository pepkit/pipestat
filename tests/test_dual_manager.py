from tempfile import NamedTemporaryFile

import pytest

from pipestat import PipestatDualManager

from .conftest import (
    db_context,
)


class TestPipestatDualManager:
    @pytest.mark.backends("file", "db")
    def test_basic_pipestat_dual_manager(
        self,
        config_file_path,
        output_schema_html_report,
        results_file_path,
        backend,
        values_sample,
        values_project,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=output_schema_html_report)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)

            psb = PipestatDualManager(**args)

            for i in values_sample:
                for r, v in i.items():
                    psb.sample.report(record_identifier=r, values=v)
                    psb.sample.set_status(record_identifier=r, status_identifier="running")
            for i in values_project:
                for r, v in i.items():
                    psb.project.report(record_identifier=r, values=v)
                    psb.project.set_status(record_identifier=r, status_identifier="running")
