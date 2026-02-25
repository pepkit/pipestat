from tempfile import NamedTemporaryFile

import pytest

from pipestat.cli import main

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


class TestPipestatCLI:
    @pytest.mark.parametrize(
        ["rec_id", "val"],
        [
            ("sample1", {"name_of_something": "test_name"}),
        ],
    )
    @pytest.mark.parametrize("backend", ["file", "db"])
    def test_basics(
        self,
        rec_id,
        val,
        config_file_path,
        schema_file_path,
        results_file_path,
        backend,
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

            # report
            if backend != "db":
                x = [
                    "report",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--value",
                    list(val.values())[0],
                    "--results-file",
                    results_file_path,
                    "--schema",
                    schema_file_path,
                ]
            else:
                x = [
                    "report",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--value",
                    list(val.values())[0],
                    "--config",
                    config_file_path,
                    "--schema",
                    schema_file_path,
                ]

            with pytest.raises(
                SystemExit
            ):  # pipestat cli normal behavior is to end with a "sys.exit(0)"
                main(test_args=x)

            # retrieve
            if backend != "db":
                x = [
                    "retrieve",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--results-file",
                    results_file_path,
                    "--schema",
                    schema_file_path,
                ]
            else:
                x = [
                    "retrieve",
                    "--record-identifier",
                    rec_id,
                    "--result-identifier",
                    list(val.keys())[0],
                    "--config",
                    config_file_path,
                    "--schema",
                    schema_file_path,
                ]

            with pytest.raises(
                SystemExit
            ):  # pipestat cli normal behavior is to end with a "sys.exit(0)"
                main(test_args=x)

            # history
            if backend != "db":
                x = [
                    "history",
                    "--record-identifier",
                    rec_id,
                    "--results-file",
                    results_file_path,
                    "--schema",
                    schema_file_path,
                ]
            else:
                x = [
                    "history",
                    "--record-identifier",
                    rec_id,
                    "--config",
                    config_file_path,
                    "--schema",
                    schema_file_path,
                ]

            with pytest.raises(
                SystemExit
            ):  # pipestat cli normal behavior is to end with a "sys.exit(0)"
                main(test_args=x)
