import logging
import os
import socket
import time

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("monitor")

app = FastAPI(title="monitor-server")
STARTED_AT = time.time()


@app.get("/monitor")
def monitor():
    log.info("🔎 [monitor] served by %s", socket.gethostname())
    return {
        "service": "monitor-server",
        "host": socket.gethostname(),
        "uptime_seconds": round(time.time() - STARTED_AT, 1),
    }


@app.get("/monitor/health")
def health():
    return {"service": "monitor-server", "status": "ok"}
