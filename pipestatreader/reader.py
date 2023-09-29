import argparse
import fastapi
import os
import logging
import uvicorn
from typing import Optional
from pipestat import RecordNotFoundError, SamplePipestatManager
from pipestat.reports import fetch_pipeline_results
from pydantic import BaseModel


_LOGGER = logging.getLogger(__name__)

app = fastapi.FastAPI(
    title="Pipestat Reader",
    description="Allows reading pipestat files from a PostgreSQL Database",
    version="0.01",
)


class FilterQuery(BaseModel):
    column_names: list[str] | None = None
    filter_conditions: list[tuple[str, str, str]] | None = None


@app.get("/")
async def home():
    """
    Display the home page
    """
    return {"Home": "Welcome"}


@app.get("/data/{record_identifier}")
async def retrieve_results_one_record(record_identifier: str):
    """
    Get all the results for one record
    """
    try:
        result = psm.retrieve(record_identifier=record_identifier)
    except RecordNotFoundError:
        return {"result": "Record not found"}
    return {"result": result}


@app.get("/data/")
async def retrieve_all_records():
    """
    Get all reported records
    """
    try:
        result = psm.get_records()
    except RecordNotFoundError:
        return {"result": "Record not found"}
    return {"result": result}


@app.get("/data/{record_identifier}/{result_identifier}")
async def retrieve_results(record_identifier: str, result_identifier: str):
    """
    Get specific result given a record identifier and a result identifier
    """
    try:
        result = psm.retrieve(
            record_identifier=record_identifier, result_identifier=result_identifier
        )
    except RecordNotFoundError:
        # TODO this should be more specific than record not found because maybe it's just the result that does not exist
        return {"result": "Record not found"}
    return {"result": result}


@app.get("/output_schema/")
async def retrieve_output_schema(pipeline_type: Optional[str] = None):
    """
    Get the output_schema used by the PipestatManager.
    """

    if pipeline_type == "sample":
        return {"output schema": psm.schema._sample_level_data}
    if pipeline_type == "project":
        return {"output schema": psm.schema._project_level_data}
    if pipeline_type is None:
        return {"output schema": psm.schema}
    else:
        return {"output schema": "output schema not found"}


@app.get("/all_table_contents/")
async def retrieve_table_contents():
    """
    Get all table contents
    """
    # Add skip and limit here as well.
    results = psm.backend.select()
    return {"table_contents": results}


@app.get("/{file_type}/")
async def retrieve_filetype(file_type: str):
    """
    Get all records by filetype
    """
    records_by_filetype = []
    all_records = psm.get_records()["records"]
    for sample in all_records:
        file_result = fetch_pipeline_results(
            project=psm,
            pipeline_name=psm.pipeline_name,
            sample_name=sample,
            inclusion_fun=lambda x: x == file_type,
        )
        records_by_filetype.append(file_result)
    return {"records_by_filetype": records_by_filetype}


@app.post("/filtered_table_contents/")
async def retrieve_filtered_table_contents(query_filter: Optional[FilterQuery] = None):
    """
        Get column contents for specific column names and/or filter conditions

    {
      "column_names": [
        "md5sum", "status"
      ],
      "filter_conditions": [["record_identifier", "eq", "random_sample_id2"]]
    }

    """
    try:
        results = psm.backend.select(
            columns=query_filter.column_names, filter_conditions=query_filter.filter_conditions
        )
    except AttributeError:
        return {"response": f"Attribute error for query: {query_filter.column_names}"}
    return {"response": results}


def create_global_pipestatmanager(pipestatcfg):
    """
    build a global pipestatmanager to be used by the endpoints
    """
    global psm
    psm = SamplePipestatManager(config_file=pipestatcfg)
    print("GLOBAL PSM CREATED")


if __name__ != "__main__":
    if os.environ.get("PIPESTAT_CONFIG") is not None:
        pipestatcfg = os.environ.get("PIPESTAT_CONFIG")
        create_global_pipestatmanager(pipestatcfg)
    else:
        _LOGGER.error("Configure by setting PIPESTAT_CONFIG env var")


def main(configfile):
    pipestatcfg = configfile or os.environ.get("PIPESTAT_CONFIG")
    create_global_pipestatmanager(pipestatcfg)
    # Note input argument app vs "reader:app" causes different behavior when using uvicorn.run
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
    )


if __name__ == "__main__":
    main()
