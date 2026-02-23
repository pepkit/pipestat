import time
from tempfile import NamedTemporaryFile

import pytest

from pipestat import SamplePipestatManager

from .conftest import (
    DB_DEPENDENCIES,
    DB_URL,
    SERVICE_UNAVAILABLE,
)
from .test_db_only_mode import ContextManagerDBTesting

pytestmark = [
    pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies"),
    pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available"),
]


class TestRetrieveHistory:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_select_history_basic(
        self,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
        range_values,
        rec_id,
        val,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            val["number_of_things"] = 1

            psm.report(record_identifier=rec_id, values=val)

            val = {"name_of_something": "MODIFIED_test_name", "number_of_things": 2}

            time.sleep(1)

            psm.report(record_identifier=rec_id, values=val)

            history_result = psm.retrieve_history(
                record_identifier="sample1", result_identifier="name_of_something"
            )

            all_history_result = psm.retrieve_history(record_identifier="sample1")

            assert len(all_history_result.keys()) == 2
            assert len(history_result.keys()) == 1
            assert len(history_result["name_of_something"].keys()) == 1

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_select_history_multi_results(
        self,
        config_file_path,
        results_file_path,
        schema_file_path,
        backend,
        range_values,
        rec_id,
        val,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            val["number_of_things"] = 1

            psm.report(record_identifier=rec_id, values=val)

            val = {"name_of_something": "MODIFIED_test_name", "number_of_things": 2}

            time.sleep(1)

            psm.report(record_identifier=rec_id, values=val)

            history_result = psm.retrieve_history(
                record_identifier="sample1",
                result_identifier=["name_of_something", "number_of_things"],
            )

            assert len(history_result.keys()) == 2
            assert len(history_result["name_of_something"].keys()) == 1

    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            (
                "sample1",
                {
                    "output_image": {
                        "path": "path_string",
                        "thumbnail_path": "thumbnail_path_string",
                        "title": "title_string",
                    }
                },
            ),
        ],
    )
    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_select_history_complex_objects(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
        rec_id,
        val,
    ):
        with NamedTemporaryFile() as f, ContextManagerDBTesting(DB_URL):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            psm.report(record_identifier=rec_id, values=val)

            val = {
                "output_image": {
                    "path": "path_string2",
                    "thumbnail_path": "thumbnail_path_string2",
                    "title": "title_string2",
                }
            }

            time.sleep(1)

            psm.report(record_identifier=rec_id, values=val)

            val = {
                "output_image": {
                    "path": "path_string3",
                    "thumbnail_path": "thumbnail_path_string3",
                    "title": "title_string3",
                }
            }

            time.sleep(1)

            psm.report(record_identifier=rec_id, values=val)

            history_result = psm.retrieve_history(
                record_identifier="sample1",
                result_identifier="output_image",
            )

            assert len(history_result.keys()) == 1
            assert "output_image" in history_result
            assert len(history_result["output_image"].keys()) == 2
