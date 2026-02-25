import pytest

from pipestat import SamplePipestatManager
from pipestat.const import *
from pipestat.exceptions import *

from .conftest import (
    DB_DEPENDENCIES,
    SERVICE_UNAVAILABLE,
)


@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
class TestEnvVars:
    def test_no_config__psm_is_built_from_env_vars(
        self, monkeypatch, results_file_path, schema_file_path
    ):
        """
        test that the object can be created if the arguments
        are provided as env vars
        """

        monkeypatch.setenv(ENV_VARS["record_identifier"], "sample1")
        monkeypatch.setenv(ENV_VARS["results_file"], results_file_path)
        monkeypatch.setenv(ENV_VARS["schema"], schema_file_path)
        try:
            SamplePipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")

    def test_config__psm_is_built_from_config_file_env_var(self, monkeypatch, config_file_path):
        """PSM can be created from config parsed from env var value."""
        monkeypatch.setenv(ENV_VARS["config"], config_file_path)
        try:
            SamplePipestatManager()
        except Exception as e:
            pytest.fail(f"Error during pipestat manager creation: {e}")


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="requires service X to be available")
@pytest.mark.skipif(not DB_DEPENDENCIES, reason="Requires dependencies")
def test_no_constructor_args__raises_expected_exception():
    """See Issue #3 in the repository."""
    with pytest.raises(NoBackendSpecifiedError):
        SamplePipestatManager()
