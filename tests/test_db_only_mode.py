import pytest

from pipestat import SamplePipestatManager
from pipestat.const import *
from .conftest import DB_URL

from sqlmodel import SQLModel, create_engine
from sqlmodel.main import default_registry


class ContextManagerDBTesting:
    """
    Creates context manager to connect to database at db_url and drop everything from the database upon exit to ensure
    the db is empty for each new test.
    """

    def __init__(self, db_url):
        self.db_url = db_url

    def __enter__(self):
        self.engine = create_engine(self.db_url, echo=True)
        self.connection = self.engine.connect()
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        SQLModel.metadata.drop_all(self.engine)
        default_registry.dispose()
        self.connection.close()


class TestDatabaseOnly:
    # TODO: parameterize this against different schemas.
    def test_manager_can_be_built_without_exception(self, config_file_path, schema_file_path):
        with ContextManagerDBTesting(DB_URL):
            try:
                SamplePipestatManager(
                    schema_path=schema_file_path,
                    record_identifier="irrelevant",
                    database_only=True,
                    config_file=config_file_path,
                )
            except Exception as e:
                pytest.fail(f"Pipestat manager construction failed: {e})")

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    # TODO: need to test reporting of more complex types
    def test_report(
        self,
        val,
        config_file_path,
        schema_file_path,
    ):
        with ContextManagerDBTesting(DB_URL):
            psm = SamplePipestatManager(
                schema_path=schema_file_path,
                record_identifier="constant_record_id",
                database_only=True,
                config_file=config_file_path,
            )
            psm.report(record_identifier="constant_record_id", values=val, force_overwrite=True)
            val_name = list(val.keys())[0]

            assert psm.select_records(
                filter_conditions=[
                    {
                        "key": val_name,
                        "operator": "eq",
                        "value": val[val_name],
                    },
                ],
            )

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            {"number_of_things": 1},
            {"percentage_of_things": 10.1},
        ],
    )
    @pytest.mark.parametrize("pipeline_type", ["project", "sample"])
    def test_report_samples_and_project(
        self,
        val,
        config_file_path,
        schema_with_project_with_samples_without_status,
        pipeline_type,
    ):
        with ContextManagerDBTesting(DB_URL):
            psm = SamplePipestatManager(
                schema_path=schema_with_project_with_samples_without_status,
                record_identifier="constant_record_id",
                database_only=True,
                config_file=config_file_path,
            )
            val_name = list(val.keys())[0]
            if pipeline_type is True:
                if val_name in psm.cfg[SCHEMA_KEY].project_level_data:
                    psm.report(
                        values=val,
                        force_overwrite=True,
                        strict_type=False,
                        # pipeline_type=pipeline_type,
                    )
                    assert psm.backend.select(filter_conditions=[(val_name, "eq", val[val_name])])
                else:
                    pass
                    # assert that this would fail to report otherwise.
            if pipeline_type == "sample":
                if val_name in psm.cfg[SCHEMA_KEY].sample_level_data:
                    psm.report(
                        values=val,
                        force_overwrite=True,
                        strict_type=False,
                        # pipeline_type=pipeline_type,
                    )
                    val_name = list(val.keys())[0]
                    assert psm.backend.select(filter_conditions=[(val_name, "eq", val[val_name])])
                else:
                    pass
                    # assert that this would fail to report otherwise.

    @pytest.mark.parametrize(
        "val",
        [
            {
                "collection_of_images": [
                    {
                        "items": {
                            "properties": {
                                "prop1": {
                                    "properties": {
                                        "path": "pathstring",
                                        "title": "titlestring",
                                    }
                                }
                            }
                        }
                    }
                ]
            },
            {"output_file": {"path": "path_string", "title": "title_string"}},
            {
                "output_image": {
                    "path": "path_string",
                    "thumbnail_path": "thumbnail_path_string",
                    "title": "title_string",
                }
            },
            {
                "output_file_in_object": {
                    "properties": {
                        "prop1": {"properties": {"path": "pathstring", "title": "titlestring"}}
                    }
                }
            },
        ],
    )
    def test_complex_object_report(
        self,
        val,
        config_file_path,
        recursive_schema_file_path,
    ):
        with ContextManagerDBTesting(DB_URL):
            REC_ID = "constant_record_id"
            psm = SamplePipestatManager(
                schema_path=recursive_schema_file_path,
                record_identifier=REC_ID,
                database_only=True,
                config_file=config_file_path,
            )
            psm.report(
                record_identifier=REC_ID, values=val, force_overwrite=True
            )  # Force overwrite so that resetting the SQL DB is unnecessary.
            val_name = list(val.keys())[0]
            assert psm.select_records(
                filter_conditions=[
                    {
                        "key": val_name,
                        "operator": "eq",
                        "value": val[val_name],
                    }
                ]
            )

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
