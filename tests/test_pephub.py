"""Tests for the PEPhub backend.

By default these tests replace pephubclient's PEPHubClient with FakePEPHubClient, an
in-memory stand-in that follows PEPhub's rules for samples (see the class docstring). That
exercises all of pipestat's PephubBackend logic without network access or a login.

To run the same tests against the live pephub.databio.org service instead, use
`pytest tests/test_pephub.py --pephub-live`. That needs `phc login` and WRITES to the
PEP at PEPHUB_URL, so only do it on purpose.
"""

import pytest

pytest.importorskip(
    "pephubclient", reason="PEPhub backend needs: uv pip install 'pipestat[pephub]'"
)

import peppy  # noqa: E402
from pephubclient.exceptions import ResponseError  # noqa: E402
from ubiquerg import parse_registry_path  # noqa: E402

from pipestat import PipestatManager  # noqa: E402
from pipestat.backends.pephub_backend import pephubbackend  # noqa: E402
from pipestat.exceptions import PipestatPEPHubError  # noqa: E402

PEPHUB_URL = "databio/pipestat_demo:default"


class _FakeSampleAPI:
    """In-memory version of `PEPHubClient().sample` (pephubclient.modules.sample)."""

    def __init__(self, store):
        self._store = store

    def _samples(self, namespace, name, tag):
        return self._store.setdefault(
            (namespace, name, tag), {"sample1": {"sample_name": "sample1"}}
        )

    def create(self, namespace, name, tag, sample_name, sample_dict, overwrite=False):
        samples = self._samples(namespace, name, tag)
        if sample_name in samples and not overwrite:
            raise ResponseError(f"Sample '{sample_name}' already exists.")
        # PEPhub replaces the whole sample when overwriting (pepdbagent full_update=True).
        samples[sample_name] = {"sample_name": sample_name, **sample_dict}

    def update(self, namespace, name, tag, sample_name, sample_dict):
        samples = self._samples(namespace, name, tag)
        if sample_name not in samples:
            raise ResponseError(f"Sample '{sample_name}' does not exist.")
        samples[sample_name].update(sample_dict)  # PEPhub merges on update

    def remove(self, namespace, name, tag, sample_name):
        samples = self._samples(namespace, name, tag)
        if sample_name not in samples:
            raise ResponseError(f"Sample '{sample_name}' does not exist.")
        del samples[sample_name]


class FakePEPHubClient:
    """In-memory stand-in for pephubclient.PEPHubClient, covering what pipestat uses.

    Projects start with one sample, "sample1", with no results, because a PEP on PEPhub
    always has samples. `load_project` builds a real peppy.Project from the stored samples,
    the same way PEPHubClient.load_project does, so empty cells come back as "".
    """

    def __init__(self, store):
        self._store = store
        self.sample = _FakeSampleAPI(store)

    def load_project(self, project_registry_path, query_param=None):
        reg = parse_registry_path(project_registry_path)
        samples = self.sample._samples(reg["namespace"], reg["item"], reg["tag"])
        raw = {
            "_config": {"pep_version": "2.1.0", "name": reg["item"]},
            "_sample_dict": [dict(s) for s in samples.values()],
        }
        return peppy.Project().from_dict(raw)


@pytest.fixture(autouse=True)
def pephub_store(request, monkeypatch):
    """Use the fake client unless --pephub-live was given. Returns the fake's sample store."""
    if request.config.getoption("--pephub-live"):
        return None
    store = {}
    monkeypatch.setattr(pephubbackend, "PEPHubClient", lambda: FakePEPHubClient(store))
    return store


@pytest.fixture
def psm(schema_file_path):
    return PipestatManager(pephub_path=PEPHUB_URL, schema_path=schema_file_path)


VAL_01 = {
    "name_of_something": "test_name",
    "number_of_things": 42,
    "md5sum": "example_md5sum",
    "percentage_of_things": 10,
}
VAL_02 = {
    "name_of_something": "test_name_02",
    "number_of_things": 52,
    "md5sum": "example_md5sum_02",
    "percentage_of_things": 30,
}


def same_value(observed, expected):
    """PEPhub data goes through a peppy sample table, so 42 may come back as 42.0 or "42"."""
    if isinstance(expected, (int, float)):
        return float(observed) == float(expected)
    return observed == expected


def assert_has_values(record, values):
    for key, value in values.items():
        assert same_value(record[key], value), (key, record[key], value)


class TestPEPHUBBackend:
    @pytest.mark.parametrize(
        ["rec_id", "val"], [("test_pipestat_01", VAL_01), ("test_pipestat_02", VAL_02)]
    )
    def test_pephub_backend_report(self, psm, rec_id, val):
        # force_overwrite defaults to True, so reporting twice works
        for _ in range(2):
            formatted = psm.report(record_identifier=rec_id, values=val)
            assert len(formatted) == len(val)
        assert_has_values(psm.retrieve_one(record_identifier=rec_id), val)

    def test_pephub_backend_report_no_overwrite(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        changed = {"number_of_things": 7}
        assert (
            psm.report(record_identifier="test_pipestat_01", values=changed, force_overwrite=False)
            is False
        )
        assert same_value(psm.retrieve_one("test_pipestat_01", "number_of_things"), 42)
        psm.report(record_identifier="test_pipestat_01", values=changed)
        assert same_value(psm.retrieve_one("test_pipestat_01", "number_of_things"), 7)

    def test_pephub_backend_report_keeps_other_results(self, psm):
        psm.report(record_identifier="test_pipestat_01", values={"md5sum": "abc"})
        psm.report(record_identifier="test_pipestat_01", values={"name_of_something": "x"})
        record = psm.retrieve_one(record_identifier="test_pipestat_01")
        assert record["md5sum"] == "abc"

    def test_pephub_backend_retrieve_one(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        result = psm.retrieve_one(record_identifier="test_pipestat_01")
        assert result["record_identifier"] == "test_pipestat_01"
        assert_has_values(result, VAL_01)
        name = psm.retrieve_one(record_identifier="test_pipestat_01", result_identifier="md5sum")
        assert name == "example_md5sum"

    def test_pephub_backend_config_file(self, tmp_path, schema_file_path):
        # Can pipestat get the PEPhub path from a config file, and report and retrieve with it?
        config = tmp_path / "config.yaml"
        config.write_text(f'pephub_path: "{PEPHUB_URL}"\n')
        psm = PipestatManager(config_file=str(config), schema_path=schema_file_path)
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        assert_has_values(psm.retrieve_one(record_identifier="test_pipestat_01"), VAL_01)

    def test_pephub_backend_retrieve_many(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        psm.report(record_identifier="test_pipestat_02", values=VAL_02)
        results = psm.retrieve_many(record_identifiers=["test_pipestat_01", "test_pipestat_02"])
        assert len(results["records"]) == 2

    def test_pephub_backend_select_records(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        psm.report(record_identifier="test_pipestat_02", values=VAL_02)
        hits = psm.select_records(
            filter_conditions=[{"key": "md5sum", "operator": "eq", "value": "example_md5sum_02"}]
        )
        assert [r["record_identifier"] for r in hits["records"]] == ["test_pipestat_02"]
        either = psm.select_records(
            filter_conditions=[
                {"key": "record_identifier", "operator": "eq", "value": "test_pipestat_01"},
                {"key": "record_identifier", "operator": "eq", "value": "test_pipestat_02"},
            ],
            bool_operator="OR",
        )
        assert len(either["records"]) == 2
        assert psm.select_records(columns=["no_such_column"])["records"] == []

    def test_set_and_get_status_pephub_backend(self, psm):
        assert psm.get_status(record_identifier="sample1") is None
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        assert (
            psm.set_status(record_identifier="test_pipestat_01", status_identifier="completed")
            is None
        )
        assert psm.get_status(record_identifier="test_pipestat_01") == "completed"
        # A sample that never had a status reads back as None, not "".
        assert psm.get_status(record_identifier="sample1") is None

    def test_set_unknown_status_pephub_backend(self, psm):
        from pipestat.exceptions import UnrecognizedStatusError

        with pytest.raises(UnrecognizedStatusError):
            psm.set_status(record_identifier="test_pipestat_01", status_identifier="bogus")

    def test_pephub_backend_remove(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        assert psm.remove(
            record_identifier="test_pipestat_01", result_identifier="name_of_something"
        )
        record = psm.retrieve_one(record_identifier="test_pipestat_01")
        assert record["name_of_something"] == ""
        assert same_value(record["number_of_things"], 42)
        # Removing a record that doesn't exist reports failure instead of raising.
        assert psm.remove(record_identifier="no_such_record") is False

    def test_pephub_backend_remove_record(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        psm.remove_record(record_identifier="test_pipestat_01", rm_record=False)
        assert psm.backend.check_record_exists("test_pipestat_01")
        psm.remove_record(record_identifier="test_pipestat_01", rm_record=True)
        assert not psm.backend.check_record_exists("test_pipestat_01")

    def test_pephub_unsupported_funcs(self, psm):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        assert psm.retrieve_history(record_identifier="test_pipestat_01") is None
        psm.list_recent_results()

    def test_pephub_backend_summarize(self, psm, tmp_path):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        assert psm.summarize(output_dir=str(tmp_path))

    def test_pephub_backend_link(self, psm, tmp_path):
        psm.report(record_identifier="test_pipestat_01", values=VAL_01)
        assert psm.link(link_dir=str(tmp_path))

    def test_pephub_bad_path(self, schema_file_path):
        with pytest.raises(PipestatPEPHubError):
            PipestatManager(pephub_path="bogus_path", schema_path=schema_file_path)
