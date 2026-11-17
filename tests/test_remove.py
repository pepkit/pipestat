from tempfile import NamedTemporaryFile

import pytest

from pipestat import SamplePipestatManager

from .conftest import (
    db_context,
)


class TestRemoval:
    @pytest.mark.parametrize(["rec_id", "res_id", "val"], [("sample2", "number_of_things", 1)])
    @pytest.mark.backends("file", "db")
    def test_remove_basic(
        self,
        rec_id,
        res_id,
        val,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
        val_dict,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            vals = [val_dict]
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for val in vals:
                for key, value in val.items():
                    psm.report(record_identifier=key, values=value)

            psm.remove(result_identifier=res_id, record_identifier=rec_id)

            col_name = res_id
            value = list(vals[0].values())[1][res_id]
            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": col_name,
                        "operator": "eq",
                        "value": value,
                    }
                ]
            )
            assert len(result["records"]) == 0

    @pytest.mark.parametrize("rec_id", ["sample1"])
    @pytest.mark.backends("file", "db")
    def test_remove_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend, val_dict
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            vals = [val_dict]
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            for val in vals:
                for key, value in val.items():
                    psm.report(record_identifier=key, values=value)
            psm.remove(record_identifier=rec_id)
            col_name = list(list(vals[0].values())[0].keys())[0]
            value = list(list(vals[0].values())[0].values())[0]
            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": col_name,
                        "operator": "eq",
                        "value": value,
                    }
                ]
            )
            assert len(result["records"]) == 0

    @pytest.mark.parametrize(
        ["rec_id", "res_id"], [("sample2", "nonexistent"), ("sample2", "bogus")]
    )
    @pytest.mark.backends("file", "db")
    def test_remove_nonexistent_result(
        self,
        rec_id,
        res_id,
        schema_file_path,
        config_file_path,
        results_file_path,
        backend,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            assert not psm.remove(record_identifier=rec_id, result_identifier=res_id)

    @pytest.mark.parametrize("rec_id", ["nonexistent", "bogus"])
    @pytest.mark.backends("file", "db")
    def test_remove_nonexistent_record(
        self, rec_id, schema_file_path, config_file_path, results_file_path, backend
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)
            assert not psm.remove(record_identifier=rec_id)
