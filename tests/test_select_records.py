from tempfile import NamedTemporaryFile

import pytest

from pipestat import SamplePipestatManager

from .conftest import (
    db_context,
)


class TestSelectRecords:
    @pytest.mark.backends("file", "db")
    def test_select_records_basic(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 80,
                    },
                ],
            )

            assert len(result1["records"]) == 4

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 80,
                    },
                    {
                        "key": "percentage_of_things",
                        "operator": "eq",
                        "value": 1,
                    },
                ],
            )

            assert len(result1["records"]) == 2

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 80,
                    },
                    {
                        "key": "percentage_of_things",
                        "operator": "eq",
                        "value": 1,
                    },
                ],
                bool_operator="OR",
            )
            assert len(result1["records"]) == 8

    @pytest.mark.backends("file", "db")
    def test_select_records_columns(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            result1 = psm.select_records(
                filter_conditions=[
                    {
                        "key": "number_of_things",
                        "operator": "ge",
                        "value": 0,
                    },
                ],
                columns=["number_of_things"],
            )

            print(result1)
            assert len(result1["records"][0]) == 2

    @pytest.mark.backends("file", "db")
    def test_select_records_columns_record_identifier(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            result1 = psm.select_records(
                columns=["record_identifier"],
            )
            assert len(list(result1["records"][0].keys())) == 1

    @pytest.mark.backends("file", "db")
    def test_select_no_filter(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            result1 = psm.select_records()

            print(result1)
            assert len(result1["records"]) == 2

    @pytest.mark.backends("file", "db")
    def test_select_no_filter_limit(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            result1 = psm.select_records(limit=1)

            print(result1)
            assert len(result1["records"]) == 1

    @pytest.mark.backends("file", "db")
    def test_select_records_bad_operator_bad_key(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            with pytest.raises(ValueError):
                # Unknown operator raises error
                psm.select_records(
                    # columns=["md5sum"],
                    filter_conditions=[
                        {
                            "key": ["output_file_in_object_nested", "prop1", "prop2"],
                            "operator": "bad_operator",
                            "value": [7, 21],
                        },
                    ],
                    limit=50,
                    bool_operator="or",
                )

            with pytest.raises(ValueError):
                # bad key raises error
                psm.select_records(
                    filter_conditions=[
                        {
                            "_garbled_key": "number_of_things",
                            "operator": "eq",
                            "value": 0,
                        }
                    ],
                    limit=50,
                    bool_operator="or",
                )

    @pytest.mark.backends("file", "db")
    def test_select_records_complex_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            psm.select_records(
                filter_conditions=[
                    {
                        "key": ["output_image", "path"],
                        "operator": "eq",
                        "value": "path_to_1",
                    },
                ],
            )

            psm.select_records(
                filter_conditions=[
                    {
                        "key": ["output_file_in_object_nested", "prop1", "prop2"],
                        "operator": "eq",
                        "value": 1,
                    },
                ],
            )

    @pytest.mark.backends("file", "db")
    def test_select_records_retrieve_one(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:6]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            # Gets one or many records
            result1 = psm.retrieve_one(record_identifier="sample1")

            assert result1["record_identifier"] == "sample1"

    @pytest.mark.backends("file", "db")
    def test_select_records_retrieve_many(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:6]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            result = psm.retrieve_many(["sample1", "sample3", "sample5"])
            assert len(result["records"]) == 3

            assert result["records"][2]["record_identifier"] == "sample5"

    @pytest.mark.backends("file", "db")
    def test_select_records_retrieve_multi_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            # Gets one or many records
            result1 = psm.retrieve_one(
                record_identifier="sample1", result_identifier=["md5sum", "number_of_things"]
            )
            print(result1)

    @pytest.mark.backends("file", "db")
    def test_select_records_retrieve_many_result(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:2]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            # Gets one or many records
            result1 = psm.retrieve_many(
                record_identifiers=["sample0", "sample1"], result_identifier="md5sum"
            )

            assert len(result1["records"]) == 2

    @pytest.mark.backends("file", "db")
    def test_select_distinct(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:10]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            for i in range(0, 10, 2):
                r_id = "sample" + str(i)
                val = {
                    "md5sum": "hash0",
                    "number_of_things": 500,
                }
                # Overwrite a couple of results such that they are not all unique
                psm.report(record_identifier=r_id, values=val)

            val = {
                "number_of_things": 900,
            }
            psm.report(record_identifier="sample2", values=val)
            # Gets one or many records
            result1 = psm.select_distinct(
                columns=["md5sum", "number_of_things", "record_identifier"]
            )
            assert len(result1) == 10
            result2 = psm.select_distinct(columns=["md5sum", "number_of_things"])
            assert len(result2) == 7
            result3 = psm.select_distinct(columns=["md5sum"])
            assert len(result3) == 6

    @pytest.mark.backends("file", "db")
    def test_select_distinct_string(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:10]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            for i in range(0, 10, 2):
                r_id = "sample" + str(i)
                val = {
                    "md5sum": "hash0",
                    "number_of_things": 500,
                }
                # Overwrite a couple of results such that they are not all unique
                psm.report(record_identifier=r_id, values=val)

            val = {
                "number_of_things": 900,
            }
            psm.report(record_identifier="sample2", values=val)
            # Gets one or many records
            result3 = psm.select_distinct(columns="md5sum")
            assert len(result3) == 6


class TestPipestatIter:
    @pytest.mark.backends("file", "db")
    def test_pipestat_iter(
        self,
        config_file_path,
        results_file_path,
        recursive_schema_file_path,
        backend,
        range_values,
    ):
        with NamedTemporaryFile() as f, db_context(backend):
            results_file_path = f.name
            args = dict(schema_path=recursive_schema_file_path)
            backend_data = (
                {"config_file": config_file_path}
                if backend == "db"
                else {"results_file_path": results_file_path}
            )
            args.update(backend_data)
            psm = SamplePipestatManager(**args)

            for i in range_values[:12]:
                r_id = i[0]
                val = i[1]
                psm.report(record_identifier=r_id, values=val)

            cursor = 10
            limit = 6
            count = 0
            for j in psm.iter_records(cursor=cursor, limit=limit):
                count += 1

            if backend == "db":
                assert count == 2
            if backend == "file":
                assert count == 6
