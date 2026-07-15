import app as api
from fastapi.testclient import TestClient


class FakeProducer:
    """Stand-in for confluent_kafka.Producer (a C object we can't patch methods on)."""

    def __init__(self):
        self.captured = {}

    def produce(self, topic, key=None, value=None, callback=None):
        self.captured = {"topic": topic, "key": key, "value": value}

    def poll(self, _timeout):
        return 0


class FakeAsyncResult:
    id = "task-123"


def test_health_ok():
    client = TestClient(api.app)
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_stream_returns_202_and_produces(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(api, "producer", fake)

    client = TestClient(api.app)
    resp = client.post("/api/stream", json={"user": "alice", "action": "resize-image"})

    assert resp.status_code == 202
    assert "job_id" in resp.json()
    assert fake.captured["topic"] == "jobs"   # topic keeps its domain name
    assert fake.captured["key"] == b"alice"
    assert b"resize-image" in fake.captured["value"]


def test_queue_returns_202_and_sends_celery_task(monkeypatch):
    captured = {}

    def fake_send_task(name, args=None):
        captured["name"] = name
        captured["args"] = args
        return FakeAsyncResult()

    monkeypatch.setattr(api.celery, "send_task", fake_send_task)

    client = TestClient(api.app)
    resp = client.post("/api/queue", json={"user": "alice", "action": "resize-image"})

    assert resp.status_code == 202
    assert resp.json()["task_id"] == "task-123"
    assert captured["name"] == "process_task"
    assert captured["args"] == ["alice", "resize-image"]
