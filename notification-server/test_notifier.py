import notifier


def test_format_notification_contains_user_and_action():
    event = {
        "job_id": "j1",
        "user": "alice",
        "action": "resize-image",
        "status": "done",
        "ts": 1.0,
    }
    line = notifier.format_notification(event)
    assert "alice" in line
    assert "resize-image" in line
    assert "j1" in line
    assert "done" in line
