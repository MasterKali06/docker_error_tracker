import logging

from fastapi import FastAPI
from fastapi.responses import FileResponse

from engine import (get_container_errors, get_container_list,
                    process_all_containers, retrieve_stats)

app = FastAPI()


@app.get("/api/containers/stats")
async def container_stats():
    res = retrieve_stats()
    return res


@app.get("/api/containers")
async def container_list():
    res = get_container_list()
    return res


@app.get("/api/errors/{container_name}")
async def container_errors(
    container_name: str, search: str | None = None, limit: int = 50, offset: int = 0
):
    """Get errors for specific container with pagination"""
    result = get_container_errors(
        container_name=container_name, search=search, limit=limit, offset=offset
    )
    return {
        "container": container_name,
        "errors": result["errors"],
        "pagination": result["pagination"],
    }


@app.get("/")
async def serve_dashboard():
    return FileResponse("templates/index.html")


def init_system():

    try:
        process_all_containers()
    except Exception as e:
        logging.error("[System Loop] error processing containers %s", e)


if __name__ == "__main__":
    import uvicorn

    init_system()
    uvicorn.run(app, host="0.0.0.0", port=8502)
