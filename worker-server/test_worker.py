import os

import worker


def test_heartbeat_touches_file(tmp_path):
    hb = tmp_path / "worker-alive"
    assert not hb.exists()
    worker.heartbeat(hb)
    assert hb.exists()


def test_heartbeat_refreshes_mtime(tmp_path):
    hb = tmp_path / "worker-alive"
    worker.heartbeat(hb)
    old = hb.stat().st_mtime
    os.utime(hb, (old - 100, old - 100))  # backdate so a refresh is observable
    worker.heartbeat(hb)
    assert hb.stat().st_mtime > old - 100


def test_build_event_marks_done_and_keeps_fields():
    payload = {"job_id": "j1", "user": "alice", "action": "resize-image", "ts": 1.0}
    event = worker.build_event(payload)
    assert event["job_id"] == "j1"
    assert event["user"] == "alice"
    assert event["action"] == "resize-image"
    assert event["status"] == "done"
    assert "ts" in event
