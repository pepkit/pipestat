import pytest

from pipestat import SamplePipestatManager
from pipestat.const import *

from ..conftest import DB_URL, ContextManagerDBTesting


class TestDatabaseOnly:
    # TODO: parameterize this against different schemas.
    def test_manager_can_be_built_without_exception(self, config_file_path, schema_file_path):
        with ContextManagerDBTesting(DB_URL):
            try:
                SamplePipestatManager(
                    schema_path=schema_file_path,
                    record_identifier="irrelevant",
                    config_file=config_file_path,
                )
                print("done")
            except Exception as e:
                pytest.fail(f"Pipestat manager construction failed: {e})")

    @pytest.mark.parametrize(["rec_id", "res_id"], [("sample2", "number_of_things")])
    def test_select_invalid_filter_column__raises_expected_exception(
        self,
        rec_id,
        res_id,
        config_file_path,
        schema_file_path,
    ):
        with ContextManagerDBTesting(DB_URL):
            args = dict(schema_path=schema_file_path, config_file=config_file_path)
            psm = SamplePipestatManager(**args)
            with pytest.raises(ValueError):
                assert psm.select_records(
                    columns=[res_id],
                    filter_conditions=[
                        {
                            "key": "bogus_name",
                            "operator": "eq",
                            "value": rec_id,
                        },
                    ],
                )

    @pytest.mark.parametrize("res_id", ["number_of_things"])
    @pytest.mark.parametrize("filter_condition", [("column", "eq", 1), "a", [1, 2, 3]])
    def test_select_invalid_filter_structure__raises_expected_exception(
        self,
        res_id,
        config_file_path,
        schema_file_path,
        filter_condition,
    ):
        with ContextManagerDBTesting(DB_URL):
            args = dict(schema_path=schema_file_path, config_file=config_file_path)
            psm = SamplePipestatManager(**args)
            with pytest.raises(AttributeError):
                psm.select_records(
                    filter_conditions=[filter_condition],
                    columns=[res_id],
                )

    @pytest.mark.parametrize(["rec_id", "res_id"], [("sample2", "number_of_things")])
    @pytest.mark.parametrize("limit", [1, 2, 3, 15555])
    def test_select_limit(
        self,
        rec_id,
        res_id,
        config_file_path,
        schema_file_path,
        limit,
    ):
        with ContextManagerDBTesting(DB_URL):
            args = dict(schema_path=schema_file_path, config_file=config_file_path)
            psm = SamplePipestatManager(**args)
            result = psm.select_records(
                filter_conditions=[
                    {
                        "key": "record_identifier",
                        "operator": "eq",
                        "value": rec_id,
                    }
                ],
                columns=[res_id],
                limit=limit,
            )
            assert len(result["records"]) <= limit

    @pytest.mark.parametrize(
        ["offset", "limit"], [(0, 0), (0, 1), (0, 2), (0, 11111), (1, 1), (1, 0)]
    )
    def test_select_pagination(
        self,
        config_file_path,
        schema_file_path,
        offset,
        limit,
    ):
        with ContextManagerDBTesting(DB_URL):
            args = dict(schema_path=schema_file_path, config_file=config_file_path)
            psm = SamplePipestatManager(**args)
            result = psm.select_records(cursor=offset, limit=limit)
            print(result)
            assert len(result["records"]) == min(max((psm.record_count - offset), 0), limit)
