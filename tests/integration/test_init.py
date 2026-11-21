"""PostgreSQL-backed tests for tests/test_init.py."""

import pytest

from pipestat import SamplePipestatManager
from pipestat.exceptions import SchemaNotFoundError


class TestPipestatManagerInstantiationDB:
    def test_obj_creation_db(self, config_file_path):
        """Object constructor works with database as backend"""
        assert isinstance(
            SamplePipestatManager(config_file=config_file_path), SamplePipestatManager
        )

    def test_schema_is_required_to_create_manager(self, config_no_schema_file_path):
        """
        Object constructor raises exception if schema is not provided.
        """
        # ToDo this test may be redundant with the modified test_report_requires_schema
        with pytest.raises(SchemaNotFoundError):
            # psm = PipestatManager(results_file_path=results_file_path)
            SamplePipestatManager(config_file=config_no_schema_file_path)
