import logging
import threading
import time
from datetime import datetime

import docker
from docker.errors import APIError, NotFound

from db import LiteDb

logger = logging.getLogger(__name__)

# Docker client with auto-reconnect capability
client = None
db = LiteDb()

# Stats caching with thread-safe access
stats_data = {}
stats_lock = threading.Lock()

# Container monitoring state
container_threads = {}
container_threads_lock = threading.Lock()
shutdown_event = threading.Event()


def get_docker_client():
    """Get or create Docker client with error handling"""
    global client
    try:
        if client is None:
            client = docker.from_env()
        # Test connection
        client.ping()
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Docker daemon: {e}")
        client = None
        return None


def process_container_errors(container_name: str):
    """Read stderr and save new errors to database with reconnection logic"""
    retry_delay = 5
    max_retry_delay = 60
    last_container_id = None

    while not shutdown_event.is_set():
        try:
            docker_client = get_docker_client()
            if not docker_client:
                logger.warning(f"Docker client unavailable, retrying in {retry_delay}s")
                shutdown_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                continue

            container = docker_client.containers.get(container_name)
            current_container_id = container.id

            # Check if container was recreated
            if last_container_id and last_container_id != current_container_id:

                # We'll continue with the new container
                last_container_id = current_container_id

            # Check if container is running
            container.reload()
            if container.status != "running":

                break

            # Reset retry delay on successful connection
            retry_delay = 5

            # Get logs from the beginning when container is recreated
            # This ensures we don't miss any logs after recreation
            since_timestamp = int(datetime.now().timestamp())  # Start from beginning
            logs = container.logs(
                stdout=False,
                stderr=True,
                stream=True,
                since=since_timestamp,
                follow=True,
            )

            for log_chunk in logs:
                if shutdown_event.is_set():
                    break

                try:
                    error = log_chunk.decode("utf-8").strip()
                    if error:
                        db.save_error(container_name, error)
                except Exception as e:
                    logger.error(
                        f"Error processing log chunk for {container_name}: {e}"
                    )

        except NotFound:
            break
        except APIError as e:
            logger.error(f"Docker API error for {container_name}: {e}")
            shutdown_event.wait(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error monitoring {container_name}: {e}")
            shutdown_event.wait(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)

    # Cleanup
    with container_threads_lock:
        if container_name in container_threads:
            del container_threads[container_name]


def start_container_monitoring(container_name: str):
    """Start monitoring a specific container"""
    with container_threads_lock:
        # Always stop existing monitoring for this container to ensure fresh start
        if container_name in container_threads:
            if container_threads[container_name].is_alive():
                logger.debug(f"Stopping existing monitoring for {container_name}")
                # We can't actually stop the thread, but we'll replace it
            del container_threads[container_name]

        thread = threading.Thread(
            target=process_container_errors,
            args=(container_name,),
            daemon=True,
            name=f"monitor-{container_name}-{int(time.time())}",  # Unique name
        )
        thread.start()
        container_threads[container_name] = thread


def stop_container_monitoring(container_name: str):
    """Stop monitoring a specific container"""
    with container_threads_lock:
        if container_name in container_threads:
            # Thread will exit on next check of shutdown_event or container status
            del container_threads[container_name]


def sync_container_monitoring():
    """Synchronize monitoring threads with actual running containers"""
    docker_client = get_docker_client()
    if not docker_client:
        return

    try:
        running_containers = {}
        for container in docker_client.containers.list():
            running_containers[container.name] = container.id  # Store both name and ID

        with container_threads_lock:
            # Stop monitoring containers that no longer exist OR have different IDs
            monitored_containers = set(container_threads.keys())
            for container_name in list(
                monitored_containers
            ):  # Use list to avoid modification during iteration
                if container_name not in running_containers:

                    stop_container_monitoring(container_name)
                else:
                    # Check if we need to restart monitoring (container was recreated)
                    current_thread = container_threads[container_name]
                    if not current_thread.is_alive():

                        stop_container_monitoring(container_name)
                        start_container_monitoring(container_name)

            # Start monitoring new containers
            for container_name in set(running_containers.keys()) - monitored_containers:
                start_container_monitoring(container_name)

    except Exception as e:
        logger.error(f"Error syncing container monitoring: {e}")


def process_all_containers():
    """Process errors for all running containers"""
    docker_client = get_docker_client()
    if not docker_client:
        logger.error("Cannot start container monitoring: Docker unavailable")
        return

    try:
        containers = docker_client.containers.list()

        for container in containers:
            start_container_monitoring(container.name)

    except Exception as e:
        logger.error(f"Error processing containers: {e}")


def poll_stats_continuously():
    """Background thread that polls stats every N seconds"""
    poll_interval = 30  # Reduced from 30 to 10 seconds for faster detection

    while not shutdown_event.is_set():
        try:
            fresh_stats = get_container_stats()

            if fresh_stats:
                with stats_lock:
                    global stats_data
                    stats_data = fresh_stats
            else:
                logger.warning("Stats polling returned empty data")

            # Also sync container monitoring (more frequent now)
            sync_container_monitoring()

        except Exception as e:
            logger.error(f"Error polling stats: {e}", exc_info=True)

        shutdown_event.wait(poll_interval)


def cleanup_old_logs():
    """Periodic cleanup of old logs"""
    cleanup_interval = 3600  # 1 hour

    while not shutdown_event.is_set():
        shutdown_event.wait(cleanup_interval)

        if not shutdown_event.is_set():
            try:
                db.cleanup_old_logs()
            except Exception as e:
                logger.error(f"Error during log cleanup: {e}")


def start_background_tasks():
    """Start all background tasks"""
    # Initialize stats immediately on startup
    try:
        initial_stats = get_container_stats()
        with stats_lock:
            global stats_data
            stats_data = initial_stats
    except Exception as e:
        logger.error(f"Error initializing stats: {e}")

    # Stats polling thread
    stats_thread = threading.Thread(
        target=poll_stats_continuously,
        daemon=True,
        name="stats-poller",
    )
    stats_thread.start()

    # Log cleanup thread
    cleanup_thread = threading.Thread(
        target=cleanup_old_logs,
        daemon=True,
        name="log-cleanup",
    )
    cleanup_thread.start()


def get_container_list():
    """Get list of running container names"""
    docker_client = get_docker_client()
    if not docker_client:
        return []

    try:
        return [c.name for c in docker_client.containers.list()]
    except Exception as e:
        logger.error(f"Error getting container list: {e}")
        return []


def retrieve_stats():
    """Return cached stats immediately (always fast)"""
    with stats_lock:
        stats_copy = stats_data.copy()

    if not stats_copy:
        logger.warning(
            "Stats data is empty, background polling may not have started yet"
        )

    return stats_copy


def get_container_stats():
    """Fetch fresh container stats from Docker"""
    docker_client = get_docker_client()
    if not docker_client:
        logger.error("Docker client not available for stats")
        return {}

    container_stats = {}

    try:
        containers = docker_client.containers.list()
        logger.debug(f"Fetching stats for {len(containers)} containers")

        for container in containers:
            try:
                cm = container.name

                # Get container status
                container.reload()
                status = container.status

                # Get uptime
                started_at = container.attrs.get("State", {}).get("StartedAt", "")
                uptime_seconds = 0
                if started_at:
                    try:
                        start_time = datetime.fromisoformat(
                            started_at.replace("Z", "+00:00")
                        )
                        uptime_seconds = (
                            datetime.now(start_time.tzinfo) - start_time
                        ).total_seconds()
                    except Exception:
                        pass

                # Get real-time stats (only if running)
                cpu_percent = 0
                memory_usage = 0
                memory_limit = 0
                memory_percent = 0

                if status == "running":
                    stats = container.stats(stream=False)

                    # CPU calculation
                    cpu_delta = (
                        stats["cpu_stats"]["cpu_usage"]["total_usage"]
                        - stats["precpu_stats"]["cpu_usage"]["total_usage"]
                    )
                    system_delta = (
                        stats["cpu_stats"]["system_cpu_usage"]
                        - stats["precpu_stats"]["system_cpu_usage"]
                    )
                    num_cpus = stats["cpu_stats"].get("online_cpus", 1)
                    cpu_percent = (
                        (cpu_delta / system_delta) * num_cpus * 100.0
                        if system_delta > 0
                        else 0
                    )

                    # Memory calculation
                    memory_usage = stats["memory_stats"].get("usage", 0)
                    memory_limit = stats["memory_stats"].get("limit", 0)
                    memory_percent = (
                        (memory_usage / memory_limit) * 100.0 if memory_limit > 0 else 0
                    )

                container_stats[cm] = {
                    "status": status,
                    "uptime_seconds": int(uptime_seconds),
                    "memory_bytes": memory_usage,
                    "memory_limit_bytes": memory_limit,
                    "memory_percent": round(memory_percent, 2),
                    "cpu_percent": round(cpu_percent, 2),
                }

                logger.debug(
                    f"Got stats for {cm}: CPU={cpu_percent:.1f}%, Mem={memory_percent:.1f}%"
                )

            except Exception as e:
                logger.error(f"Error getting stats for {container.name}: {e}")
                container_stats[container.name] = {
                    "status": "error",
                    "error": str(e),
                }

    except Exception as e:
        logger.error(f"Error getting container stats: {e}", exc_info=True)

    return container_stats


def get_container_errors(
    container_name: str, search: str | None = None, limit: int = 50, offset: int = 0
):
    """Get errors for a specific container"""
    return db.get_errors(
        container_name=container_name, search=search, limit=limit, offset=offset
    )


def get_error_summary(container_name: str | None = None, hours: int = 24):
    """Get error summary statistics"""
    return db.get_error_summary(container_name=container_name, hours=hours)


def shutdown():
    """Graceful shutdown of all background tasks"""
    shutdown_event.set()

    # Wait for threads to finish (with timeout)
    with container_threads_lock:
        for name, thread in container_threads.items():
            thread.join(timeout=5)

    # Close database connections
    db.close_connection()
