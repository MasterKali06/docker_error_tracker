import threading
from datetime import datetime, timedelta

import docker

from db import LiteDb

client = docker.from_env()
db = LiteDb()


def process_container_errors(container_name):
    """Read stderr and save new errors to database"""
    container = client.containers.get(container_name)

    # Start from now, follow continuously
    logs = container.logs(
        stdout=False,
        stderr=True,
        stream=True,
        since=int(datetime.now().timestamp()),  # Start from now
        follow=True,
    )

    for log_chunk in logs:
        error = log_chunk.decode("utf-8").strip()
        if error:
            db.save_error(container_name, error)


def process_all_containers():
    """Process errors for all running containers"""
    containers = client.containers.list()
    for container in containers:
        threading.Thread(
            target=process_container_errors, args=(container.name,), daemon=True
        ).start()


def get_container_list():
    return [c.name for c in client.containers.list()]


stats_cache = {}
cache_expiry = timedelta(minutes=5)


def retrieve_stats():
    global stats_cache
    now = datetime.now()

    # Return cached stats if not expired
    if "data" in stats_cache and "timestamp" in stats_cache:
        if now - stats_cache["timestamp"] < cache_expiry:
            return stats_cache["data"]

    # Fetch fresh stats and cache them
    fresh_stats = get_container_stats()
    stats_cache = {"data": fresh_stats, "timestamp": now}
    return fresh_stats


def get_container_stats():

    container_stats = {}
    containers = client.containers.list()

    for container in containers:
        cm = container.name

        # Get real-time stats
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
        cpu_percent = (cpu_delta / system_delta) * 100.0 if system_delta > 0 else 0

        # Memory calculation
        memory_usage = stats["memory_stats"]["usage"]

        container_stats[cm] = {"memory": memory_usage, "cpu": cpu_percent}

    return container_stats


def get_container_errors(
    container_name: str, search: str | None = None, limit: int = 50, offset: int = 0
):
    return db.get_errors(
        container_name=container_name, search=search, limit=limit, offset=offset
    )
