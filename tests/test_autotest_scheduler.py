import random

from bot.autotest.runner import MistakeScheduler


def test_mistake_scheduler_triggers_on_interval():
    rng = random.Random(0)
    scheduler = MistakeScheduler(mistake_min=2, mistake_max=2, rng=rng)
    results = [scheduler.should_force_wrong() for _ in range(5)]
    assert results == [False, True, False, True, False]
