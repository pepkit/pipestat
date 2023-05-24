import pytest

from pipestat import PipestatManager
from pipestat.const import *

from sqlmodel import Session, SQLModel, create_engine


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
        self.connection.close()


class TestDatabaseOnly:
    # TODO: parameterize this against different schemas.
    def test_manager_can_be_built_without_exception(
        self, config_file_path, schema_file_path
    ):
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            try:
                PipestatManager(
                    schema_path=schema_file_path,
                    record_identifier="irrelevant",
                    database_only=True,
                    config=config_file_path,
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
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            psm = PipestatManager(
                schema_path=schema_file_path,
                record_identifier="constant_record_id",
                database_only=True,
                config=config_file_path,
            )
            psm.report(values=val, force_overwrite=True)
            assert len(psm.data) == 0
            val_name = list(val.keys())[0]
            assert psm.select(filter_conditions=[(val_name, "eq", val[val_name])])

    @pytest.mark.parametrize(
        "val",
        [
            {"name_of_something": "test_name"},
            # {"number_of_things": 1},
            # {"percentage_of_things": 10.1},
        ],
    )
    def test_report_samples_and_project(
        self,
        val,
        config_file_path,
        schema_with_project_with_samples_without_status,
    ):
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            psm = PipestatManager(
                schema_path=schema_with_project_with_samples_without_status,
                record_identifier="constant_record_id",
                database_only=True,
                config=config_file_path,
            )
            # temp overwrite values
            # Report and select project variables
            # TODO refactor this test to align with the others in testing suite
            val = {"number_of_things": 1}
            psm.report(
                values=val, force_overwrite=True, strict_type=False, project_level=True
            )
            val_name = list(val.keys())[0]
            assert psm.select(filter_conditions=[(val_name, "eq", val[val_name])])

            # Report and select project variables
            val = {"smooth_bw": "STRING"}
            psm.report(
                values=val, force_overwrite=True, strict_type=False, project_level=False
            )
            val_name = list(val.keys())[0]
            assert psm.select(filter_conditions=[(val_name, "eq", val[val_name])])

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
                        "prop1": {
                            "properties": {"path": "pathstring", "title": "titlestring"}
                        }
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
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            REC_ID = "constant_record_id"
            psm = PipestatManager(
                schema_path=recursive_schema_file_path,
                record_identifier=REC_ID,
                database_only=True,
                config=config_file_path,
            )
            psm.report(
                values=val, force_overwrite=True
            )  # Force overwrite so that resetting the SQL DB is unnecessary.
            val_name = list(val.keys())[0]
            assert psm.select(json_filter_conditions=[(val_name, "eq", val[val_name])])

    @pytest.mark.parametrize(["rec_id", "res_id"], [("sample2", "number_of_things")])
    def test_select_invalid_filter_column__raises_expected_exception(
        self,
        rec_id,
        res_id,
        config_file_path,
        schema_file_path,
    ):
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            args = dict(schema_path=schema_file_path, config=config_file_path)
            psm = PipestatManager(**args)
            with pytest.raises(ValueError):
                psm.select(
                    filter_conditions=[("bogus_column", "eq", rec_id)],
                    columns=[res_id],
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
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            args = dict(schema_path=schema_file_path, config=config_file_path)
            psm = PipestatManager(**args)
            with pytest.raises((ValueError, TypeError)):
                psm.select(
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
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            args = dict(schema_path=schema_file_path, config=config_file_path)
            psm = PipestatManager(**args)
            result = psm.select(
                filter_conditions=[(RECORD_ID, "eq", rec_id)],
                columns=[res_id],
                limit=limit,
            )
            assert len(result) <= limit

    @pytest.mark.parametrize("offset", [0, 1, 2, 3, 15555])
    @pytest.mark.xfail(reason="Need to reimplement psm.record_count")
    def test_select_offset(
        self,
        config_file_path,
        schema_file_path,
        offset,
    ):
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            args = dict(schema_path=schema_file_path, config=config_file_path)
            psm = PipestatManager(**args)
            result = psm.select(offset=offset)
            print(result)
            assert len(result) == max((psm.record_count - offset), 0)

    @pytest.mark.parametrize(
        ["offset", "limit"], [(0, 0), (0, 1), (0, 2), (0, 11111), (1, 1), (1, 0)]
    )
    @pytest.mark.xfail(reason="Need to reimplement psm.record_count")
    def test_select_pagination(
        self,
        config_file_path,
        schema_file_path,
        offset,
        limit,
    ):
        db_url = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"
        with ContextManagerDBTesting(db_url) as connection:
            args = dict(schema_path=schema_file_path, config=config_file_path)
            psm = PipestatManager(**args)
            result = psm.select(offset=offset, limit=limit)
            print(result)
            assert len(result) == min(max((psm.record_count - offset), 0), limit)
