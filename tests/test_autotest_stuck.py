from bot.autotest.runner import StuckDetector


def test_stuck_detector_same_item_trigger():
    detector = StuckDetector(max_same_item=2, max_no_progress=5)
    assert detector.record(question_key="q1", progress_key="q1") is None
    assert detector.record(question_key="q1", progress_key="q1") is None
    reason = detector.record(question_key="q1", progress_key="q1")
    assert reason == ("same_item", 3)


def test_stuck_detector_no_progress_trigger():
    detector = StuckDetector(max_same_item=10, max_no_progress=2)
    assert detector.record(question_key="q1", progress_key="p1") is None
    assert detector.record(question_key="q2", progress_key="p1") is None
    reason = detector.record(question_key="q3", progress_key="p1")
    assert reason == ("no_progress", 3)
