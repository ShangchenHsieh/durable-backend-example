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


def test_health_ok():
    client = TestClient(api.app)
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_create_job_returns_202_and_produces(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(api, "producer", fake)

    client = TestClient(api.app)
    resp = client.post("/api/jobs", json={"user": "alice", "action": "resize-image"})

    assert resp.status_code == 202
    assert "job_id" in resp.json()
    assert fake.captured["topic"] == "jobs"
    assert fake.captured["key"] == b"alice"
    assert b"resize-image" in fake.captured["value"]
