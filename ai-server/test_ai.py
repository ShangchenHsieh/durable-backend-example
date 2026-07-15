import ai


def test_enrich_mentions_job_user_and_action():
    event = {
        "job_id": "j1",
        "user": "alice",
        "action": "resize-image",
        "status": "done",
        "ts": 1.0,
    }
    line = ai.enrich(event)
    assert "j1" in line
    assert "alice" in line
    assert "resize-image" in line
