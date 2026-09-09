from __future__ import annotations

import pytest

from run_v3_canonical_worker import run_worker_loop


class FakeWorker:
    def __init__(self):
        self.calls: list[int] = []

    def run_once(self, *, limit: int):
        self.calls.append(limit)
        return {"selected": 0}


def test_worker_loop_repeats_until_interrupted():
    worker = FakeWorker()
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 2:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_worker_loop(
            worker,
            limit=25,
            interval_seconds=2.5,
            sleep_fn=fake_sleep,
        )

    assert worker.calls == [25, 25]
    assert sleeps == [2.5, 2.5]


def test_worker_loop_clamps_unsafe_values():
    worker = FakeWorker()

    def stop_after_first(seconds: float) -> None:
        assert seconds == 0.25
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        run_worker_loop(
            worker,
            limit=0,
            interval_seconds=0,
            sleep_fn=stop_after_first,
        )

    assert worker.calls == [1]
