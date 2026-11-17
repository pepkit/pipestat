"""PostgreSQL-backed tests for tests/test_env_vars.py."""

import pytest

from pipestat import SamplePipestatManager
from pipestat.const import ENV_VARS


class TestEnvVarsDB:
    def test_config__psm_is_built_from_config_file_env_var(self, monkeypatch, config_file_path):
        """PSM can be created from config parsed from env var value."""
        monkeypatch.setenv(ENV_VARS["config"], config_file_path)
        try:
            SamplePipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")
