import tasks


def test_build_result_marks_processed_and_keeps_fields():
    result = tasks.build_result("alice", "resize-image")
    assert result["user"] == "alice"
    assert result["action"] == "resize-image"
    assert result["status"] == "processed"


def test_build_report_contains_count_and_alive():
    line = tasks.build_report(7)
    assert "7" in line
    assert "alive" in line
