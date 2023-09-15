import fastapi
import uvicorn
from typing import Optional
from pipestat import RecordNotFoundError, SamplePipestatManager


# For testing, simply hardcode config atm
pipestatcfg = "/home/drc/PythonProjects/pipestat/testimport/drcdbconfig.yaml"
psm = SamplePipestatManager(config_file=pipestatcfg)

app = fastapi.FastAPI(
    title="Pipestat Reader",
    description="Allows reading pipestat files from a POSTGRESQL Server",
    version="0.01",
)


@app.get("/")
async def home():
    """
    Display the home page
    """
    return {"Home": "Welcome"}


@app.get("/data/{record_identifier}")
async def retrieve_results(record_identifier: str):
    """
    Get all the results for one record
    """
    try:
        result = psm.retrieve(record_identifier=record_identifier)
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
    Get specific result given a record identifier and a result identifier
    """

    if pipeline_type == "sample":
        return {"output schema": psm.schema._sample_level_data}
    if pipeline_type == "project":
        return {"output schema": psm.schema._project_level_data}
    if pipeline_type is None:
        return {"output schema": psm.schema}
    else:
        return {"output schema": "output schema not found"}


def main():
    uvicorn.run("reader:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
