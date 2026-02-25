from tempfile import TemporaryDirectory

import pytest

from pipestat import PipestatManager
from pipestat.exceptions import PipestatPEPHubError

from .conftest import (
    PEPHUB_URL,
)


@pytest.mark.pephub
class TestPEPHUBBackend:
    """
    These tests require PEPhub login. Use `phc login` to sign in,
    then run with: pytest --pephub
    """

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            (
                "test_pipestat_01",
                {
                    "name_of_something": "test_name",
                    "number_of_things": 42,
                    "md5sum": "example_md5sum",
                    "percentage_of_things": 10,
                },
            ),
            (
                "test_pipestat_02",
                {
                    "name_of_something": "test_name_02",
                    "number_of_things": 52,
                    "md5sum": "example_md5sum_02",
                    "percentage_of_things": 30,
                },
            ),
        ],
    )
    def test_pephub_backend_report(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        # Value already exists should give an error unless forcing overwrite

        # force overwrite defaults to true, so it should have no problem reporting
        psm.report(record_identifier=rec_id, values=val)

        print("done")

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("test_pipestat_01", {"name_of_something": "test_name"}),
        ],
    )
    def test_pephub_backend_retrieve_one(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        result = psm.retrieve_one(record_identifier=rec_id)

        assert len(result.keys()) == 5

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("test_pipestat_01", {"name_of_something": "test_name"}),
        ],
    )
    def test_pephub_backend_config_file(
        self,
        rec_id,
        val,
        config_file_path_pephub,
        schema_file_path,
    ):

        # Can pipestat obtain pephub url from config file AND successfully retrieve values?
        psm = PipestatManager(config_file=config_file_path_pephub, schema_path=schema_file_path)

        result = psm.retrieve_one(record_identifier=rec_id)

        assert len(result.keys()) == 5

    def test_pephub_backend_retrieve_many(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):

        rec_ids = ["test_pipestat_01", "test_pipestat_02"]

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        results = psm.retrieve_many(record_identifiers=rec_ids)

        assert len(results["records"]) == 2

    def test_set_status_pephub_backend(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):
        rec_ids = ["test_pipestat_01"]

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        result = psm.set_status(record_identifier=rec_ids[0], status_identifier="completed")

        assert result is None

    def test_get_status_pephub_backend(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):
        rec_ids = ["sample1", "test_pipestat_01"]

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        result = psm.get_status(record_identifier=rec_ids[0])

        assert result is None

        result = psm.get_status(record_identifier=rec_ids[1])

        assert result == "completed"

    def test_pephub_backend_remove(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):

        rec_ids = ["test_pipestat_01"]

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        results = psm.remove(record_identifier=rec_ids[0], result_identifier="name_of_something")

        assert results is True

    def test_pephub_backend_remove_record(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):

        rec_ids = ["test_pipestat_01"]

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        psm.remove_record(record_identifier=rec_ids[0], rm_record=False)

        psm.remove_record(record_identifier=rec_ids[0], rm_record=True)

    def test_pephub_unsupported_funcs(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):

        rec_ids = ["test_pipestat_01"]

        psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)

        results = psm.retrieve_history(record_identifier=rec_ids[0])

        assert results is None

        psm.list_recent_results()

    def test_pephub_backend_summarize(
        self,
        config_file_path,
        schema_file_path,
    ):

        with TemporaryDirectory() as d:
            psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)
            report_path = psm.summarize(output_dir=d)

            assert report_path

    def test_pephub_backend_link(
        self,
        config_file_path,
        schema_file_path,
    ):

        with TemporaryDirectory() as d:
            psm = PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)
            report_path = psm.link(link_dir=d)

            assert report_path

    def test_pephub_bad_path(
        self,
        config_file_path,
        schema_file_path,
        results_file_path,
        range_values,
    ):
        with pytest.raises(PipestatPEPHubError):
            PipestatManager(pephub_path="bogus_path", schema_path=schema_file_path)
