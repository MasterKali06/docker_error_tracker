import atexit
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from engine import (get_container_errors, get_container_list,
                    get_error_summary, process_all_containers, retrieve_stats,
                    shutdown, start_background_tasks)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def init_system():
    """Initialize the monitoring system"""
    logger.info("Initializing Docker container monitoring system...")

    try:
        # Start monitoring all containers
        process_all_containers()
        logger.info("Container monitoring initialized")

        # Start background tasks (stats polling, log cleanup)
        start_background_tasks()
        logger.info("Background tasks started")

        logger.info("System initialization complete")

    except Exception as e:
        logger.error(f"[System Init] Error: {e}")
        raise


def cleanup_on_exit():
    """Cleanup function called on application exit"""
    logger.info("Application shutting down...")
    shutdown()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting application lifespan...")
    init_system()
    yield
    # Shutdown
    logger.info("Shutting down application...")
    shutdown()


app = FastAPI(
    title="Docker Container Monitor",
    description="Monitor Docker containers, stats, and error logs",
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/api/containers/stats")
async def container_stats():
    """Get cached container statistics (always fast)"""
    try:
        res = retrieve_stats()
        return {"success": True, "data": res, "cached": True}
    except Exception as e:
        logger.error(f"Error retrieving stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/containers")
async def container_list():
    """Get list of running containers"""
    try:
        res = get_container_list()
        return {"success": True, "containers": res, "count": len(res)}
    except Exception as e:
        logger.error(f"Error retrieving container list: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/errors/{container_name}")
async def container_errors(
    container_name: str, search: str | None = None, limit: int = 50, offset: int = 0
):
    """Get errors for specific container with pagination and search"""
    try:
        if limit > 1000:
            limit = 1000  # Prevent excessive queries

        result = get_container_errors(
            container_name=container_name, search=search, limit=limit, offset=offset
        )

        return {
            "success": True,
            "container": container_name,
            "errors": result["errors"],
            "pagination": result["pagination"],
        }
    except Exception as e:
        logger.error(f"Error retrieving errors for {container_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/errors/summary/{container_name}")
async def container_error_summary(container_name: str, hours: int = 24):
    """Get error summary for specific container"""
    try:
        if hours > 168:  # Max 1 week
            hours = 168

        result = get_error_summary(container_name=container_name, hours=hours)
        return {"success": True, "container": container_name, "summary": result}
    except Exception as e:
        logger.error(f"Error retrieving error summary for {container_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/errors/summary")
async def all_containers_error_summary(hours: int = 24):
    """Get error summary for all containers"""
    try:
        if hours > 168:  # Max 1 week
            hours = 168

        result = get_error_summary(hours=hours)
        return {"success": True, "summary": result, "hours": hours}
    except Exception as e:
        logger.error(f"Error retrieving error summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        containers = get_container_list()
        stats = retrieve_stats()

        return {
            "status": "healthy",
            "containers_monitored": len(containers),
            "stats_available": len(stats) > 0,
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
        }


@app.get("/")
async def serve_dashboard():
    """Serve the main dashboard"""
    return FileResponse("templates/index.html")


# Register cleanup handler
atexit.register(cleanup_on_exit)


if __name__ == "__main__":
    import uvicorn

    try:
        init_system()

        logger.info("Starting FastAPI server on 0.0.0.0:8502")
        uvicorn.run(app, host="0.0.0.0", port=8502, log_level="info")

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        cleanup_on_exit()
