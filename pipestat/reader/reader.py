import fastapi
import uvicorn

def pipestat_reader(db_config):
    print("hello from reader")
    # Create a FastAPI instance
    app = fastapi.FastAPI(
    title="PIPESTATREADER",
    description="Allows reading pipestat files from a POSTGRESQL Server",
    version="0.01",
    )
    #uvicorn.run("app:app", host='0.0.0.0', port=8000, reload=True, workers=3)
    uvicorn.run("app:app", host='127.0.0.1', port=5432, reload=True, workers=3)
    print(db_config)
    @app.get("/items/{item_id}")
    async def read_item(item_id):
        return {"item_id": item_id}
