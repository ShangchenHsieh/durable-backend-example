import monitor as mon
from fastapi.testclient import TestClient


def test_monitor_identifies_itself():
    client = TestClient(mon.app)
    resp = client.get("/monitor")
    assert resp.status_code == 200
    assert resp.json()["service"] == "monitor-server"


def test_monitor_health_ok():
    client = TestClient(mon.app)
    resp = client.get("/monitor/health")
    assert resp.status_code == 200
    assert resp.json() == {"service": "monitor-server", "status": "ok"}
