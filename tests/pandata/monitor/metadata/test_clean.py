import datetime
from monitor.metadata.clean import now_subtract


def test_now_subtract(monkeypatch):
    FAKE_TIME = datetime.datetime(2020, 1, 2, 0, 0)

    class MockDateTime:
        @classmethod
        def now(cls, *args, **kwargs):
            return FAKE_TIME

    monkeypatch.setattr("monitor.metadata.clean.datetime", MockDateTime)
    assert now_subtract(days=1) == datetime.datetime(2020, 1, 1, 0, 0)
