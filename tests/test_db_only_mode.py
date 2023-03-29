import pytest

from pipestat import PipestatManager
from pipestat.const import *


class TestDatabaseOnly:
    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    def test_report(
        self,
        val,
        config_file_path,
        schema_file_path,
    ):
        psm = PipestatManager(
            schema_path=schema_file_path,
            namespace="test",
            record_identifier="constant_record_id",
            database_only=True,
            config=config_file_path,
        )
        psm.report(values=val)
        assert len(psm.data) == 0
        val_name = list(val.keys())[0]
        assert psm.select(filter_conditions=[(val_name, "eq", str(val[val_name]))])

    @pytest.mark.parametrize(["rec_id", "res_id"], [("sample2", "number_of_things")])
    def test_select_invalid_filter_column(
        self,
        rec_id,
        res_id,
        config_file_path,
        schema_file_path,
    ):
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        with pytest.raises(ValueError):
            psm.select(
                filter_conditions=[("bogus_column", "eq", rec_id)],
                columns=[res_id],
            )

    @pytest.mark.parametrize("res_id", ["number_of_things"])
    @pytest.mark.parametrize("filter", [("column", "eq", 1), "a", [1, 2, 3]])
    def test_select_invalid_filter_structure(
        self,
        res_id,
        config_file_path,
        schema_file_path,
        filter,
    ):
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        with pytest.raises((ValueError, TypeError)):
            psm.select(
                filter_conditions=[filter],
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
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        result = psm.select(
            filter_conditions=[(RECORD_ID, "eq", rec_id)],
            columns=[res_id],
            limit=limit,
        )
        assert len(result) <= limit

    @pytest.mark.parametrize("offset", [0, 1, 2, 3, 15555])
    def test_select_offset(
        self,
        config_file_path,
        schema_file_path,
        offset,
    ):
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        result = psm.select(offset=offset)
        print(result)
        assert len(result) == max((psm.record_count - offset), 0)

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
        args = dict(
            schema_path=schema_file_path, namespace="test", config=config_file_path
        )
        psm = PipestatManager(**args)
        result = psm.select(offset=offset, limit=limit)
        print(result)
        assert len(result) == min(max((psm.record_count - offset), 0), limit)
