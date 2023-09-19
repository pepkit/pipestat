import fastapi
import uvicorn
from typing import Optional
from pipestat import RecordNotFoundError, SamplePipestatManager
from fastapi import Query

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


@app.get("/all_table_contents/")
async def retrieve_table_contents():
    """
    Get all table contents
    """
    # Add skip and limit here as well.
    results = psm.backend.select()
    return {"table_contents": results}

@app.get("/images/")
async def retrieve_images():
    """
    Get all image paths reported in the table.
    """
    # Add skip and limit here as well.
    #results = psm.backend.select()
    # Should be able to determine if result schema has image type and then use select function to retrieve those
    # check result schema first?
    return {"image_paths": "Not implemented."}

@app.get("/files/")
async def retrieve_images():
    """
    Get all image paths reported in the table.
    """
    # Add skip and limit here as well.
    # results = psm.backend.select()
    # Should be able to determine if result schema has file type and then use select function to retrieve those
    # check result schema first?
    return {"file_paths": "Not implemented."}

@app.get("/filtered_table_contents/")
async def retrieve_filtered_table_contents(
        column_name_list: Optional[list[str]] = Query(default=None, description="List of column names to select from the table"),
        #filter_conditions: Optional[list[tuple[str,str,str]]] = Query(None),
                                   ):
    """
    Get column contents for a specific column name
    """
    # Add skip and limit here as well.
    try:
        results = psm.backend.select(columns=column_name_list, filter_conditions=None)
    except AttributeError:
        return {"column_contents": f"Attribute error for query: {column_name_list}"}
    if results is not None:
        return {"column_contents": results}
    else:
        return {"column_contents": "This column does not exist."}

# @app.get("/filtered_table_contents/")
# async def retrieve_filtered_column_contents(column_name: Optional[str] = None, filter_conditions: Optional[tuple[str,str,str]] = None):
#     """
#     Filter table contents based on column names and/or filter conditions
#     """
#     # Add skip and limit here as well.
#     # filter_conditions=[("record_identifier", "eq", "random_sample_i2")])
#     if column_name is None and filter_conditions is None:
#         return {"filtered_table_results": psm.backend.select()}
#     if column_name and filter_conditions:
#         results = psm.backend.select(columns=[column_name], filter_conditions=[filter_conditions])
#     if column_name:
#         results = psm.backend.select(columns=[column_name])
#     if filter_conditions:
#         filter_conditions = tuple(filter_conditions)
#         results = psm.backend.select(filter_conditions=filter_conditions)
#     if results is not None:
#         return {"column_contents": results}
#     else:
#         return {"column_contents": "not found"}


def main():
    uvicorn.run("reader:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
