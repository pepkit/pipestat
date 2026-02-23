from tempfile import NamedTemporaryFile

import pytest

from pipestat import SamplePipestatManager
from pipestat.exceptions import InvalidTimeFormatError

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


class TestTimeStamp:
    @pytest.mark.parametrize("backend", ["db", "file"])
    def test_list_recent_results(
        self,
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
            psm = SamplePipestatManager(**args)

            # Report a few values
            val = {"number_of_things": 1}
            for i in range(10):
                rid = "sample" + str(i)
                psm.report(record_identifier=rid, values=val)

            # Modify a couple of records
            val = {"number_of_things": 2}
            psm.report(record_identifier="sample3", values=val)
            psm.report(record_identifier="sample4", values=val)

            # Test default
            results = psm.list_recent_results()
            assert len(results["records"]) == 10

            # Test limit
            results = psm.list_recent_results(limit=2)
            assert len(results["records"]) == 2

            # Test garbled time raises error
            with pytest.raises(InvalidTimeFormatError):
                psm.list_recent_results(start="2100-01-01dsfds", end="1970-01-01")

            # Test large window
            results = psm.list_recent_results(start="2100-01-01 0:0:0", end="1970-01-01 0:0:0")
            assert len(results["records"]) == 10
