import logging
import os
import time

from celery import Celery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("celery")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
TASK_SECONDS = float(os.getenv("TASK_SECONDS", "1"))
BEAT_SECONDS = float(os.getenv("CELERY_BEAT_SECONDS", "30"))

# One Celery app; run as a worker or as beat (same code, different command).
celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL)
celery.conf.timezone = "UTC"
# Job scheduling: Beat enqueues heartbeat_report on a fixed interval.
celery.conf.beat_schedule = {
    "heartbeat-every-interval": {
        "task": "heartbeat_report",
        "schedule": BEAT_SECONDS,
    }
}

_beat_count = 0  # per-worker-process counter for the periodic report


def build_result(user: str, action: str) -> dict:
    """Pure: the result an on-demand task returns for a (user, action)."""
    return {"user": user, "action": action, "status": "processed"}


def build_report(count: int) -> str:
    """Pure: render the periodic heartbeat report line."""
    return f"⏰ [celery-beat] heartbeat #{count} — scheduler is alive"


@celery.task(name="process_task")
def process_task(user: str, action: str) -> dict:
    """On-demand task (job queue): target of POST /api/queue."""
    log.info("⚙️ [celery-worker] running task for %s doing %s", user, action)
    time.sleep(TASK_SECONDS)  # simulate work
    result = build_result(user, action)
    log.info("⚙️ [celery-worker] done: %s", result)
    return result


@celery.task(name="heartbeat_report")
def heartbeat_report() -> str:
    """Periodic task (job scheduling): fired by Celery Beat."""
    global _beat_count
    _beat_count += 1
    line = build_report(_beat_count)
    log.info(line)
    return line
