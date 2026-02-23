from pipestat import SamplePipestatManager


class TestRoundTrips:
    """Unit tests: report a value, retrieve it, verify it matches."""

    def _make_psm(self, schema_file_path, tmp_path):
        return SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=str(tmp_path / "results.yaml"),
        )

    def test_round_trip_integer(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"number_of_things": 42})
        assert psm.retrieve_one(record_identifier="s1", result_identifier="number_of_things") == 42

    def test_round_trip_number(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"percentage_of_things": 3.14})
        assert psm.retrieve_one(record_identifier="s1", result_identifier="percentage_of_things") == 3.14

    def test_round_trip_string(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"name_of_something": "hello"})
        assert psm.retrieve_one(record_identifier="s1", result_identifier="name_of_something") == "hello"

    def test_round_trip_boolean(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"switch_value": True})
        assert psm.retrieve_one(record_identifier="s1", result_identifier="switch_value") is True

    def test_round_trip_file(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        val = {"path": "/tmp/f.txt", "title": "My file"}
        psm.report(record_identifier="s1", values={"output_file": val})
        result = psm.retrieve_one(record_identifier="s1", result_identifier="output_file")
        assert result["path"] == val["path"]
        assert result["title"] == val["title"]

    def test_round_trip_image(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        val = {"path": "/tmp/img.png", "thumbnail_path": "/tmp/thumb.png", "title": "My image"}
        psm.report(record_identifier="s1", values={"output_image": val})
        result = psm.retrieve_one(record_identifier="s1", result_identifier="output_image")
        assert result["path"] == val["path"]
        assert result["thumbnail_path"] == val["thumbnail_path"]
        assert result["title"] == val["title"]

    def test_round_trip_additional_property(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"not_in_schema": 99})
        assert psm.retrieve_one(record_identifier="s1", result_identifier="not_in_schema") == 99

    def test_round_trip_full_record(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"number_of_things": 10, "name_of_something": "test"})
        record = psm.retrieve_one(record_identifier="s1")
        assert record["number_of_things"] == 10
        assert record["name_of_something"] == "test"

    def test_round_trip_retrieve_many(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"number_of_things": 1})
        psm.report(record_identifier="s2", values={"number_of_things": 2})
        result = psm.retrieve_many(record_identifiers=["s1", "s2"])
        assert result["total_size"] == 2
        assert len(result["records"]) == 2
        values = {r["record_identifier"]: r["number_of_things"] for r in result["records"]}
        assert values["s1"] == 1
        assert values["s2"] == 2

    def test_round_trip_persistence(self, schema_file_path, tmp_path):
        results_path = str(tmp_path / "results.yaml")
        psm1 = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=results_path,
        )
        psm1.report(record_identifier="s1", values={"number_of_things": 77})
        psm2 = SamplePipestatManager(
            schema_path=schema_file_path,
            results_file_path=results_path,
        )
        assert psm2.retrieve_one(record_identifier="s1", result_identifier="number_of_things") == 77

    def test_round_trip_count_records(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"name_of_something": "a"})
        psm.report(record_identifier="s2", values={"name_of_something": "b"})
        psm.report(record_identifier="s3", values={"name_of_something": "c"})
        assert psm.count_records() == 3

    def test_round_trip_select_records(self, schema_file_path, tmp_path):
        psm = self._make_psm(schema_file_path, tmp_path)
        psm.report(record_identifier="s1", values={"number_of_things": 10})
        psm.report(record_identifier="s2", values={"number_of_things": 20})
        psm.report(record_identifier="s3", values={"number_of_things": 30})
        result = psm.select_records(
            filter_conditions=[{"key": "number_of_things", "operator": "ge", "value": 20}]
        )
        assert len(result["records"]) == 2
        ids = {r["record_identifier"] for r in result["records"]}
        assert ids == {"s2", "s3"}
