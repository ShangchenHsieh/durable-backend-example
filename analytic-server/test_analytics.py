from collections import Counter

import analytics


def test_update_tally_counts_actions():
    tally = Counter()
    analytics.update_tally(tally, {"action": "resize-image"})
    analytics.update_tally(tally, {"action": "resize-image"})
    analytics.update_tally(tally, {"action": "transcode"})
    assert tally["resize-image"] == 2
    assert tally["transcode"] == 1


def test_format_stats_shows_total_and_breakdown():
    tally = Counter({"resize-image": 2, "transcode": 1})
    line = analytics.format_stats(tally)
    assert "3" in line
    assert "resize-image=2" in line
    assert "transcode=1" in line
