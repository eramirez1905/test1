from datetime import datetime, timedelta, timezone

from airflow.models import Base
from airflow.utils.db import provide_session
from sqlalchemy.orm import scoped_session


@provide_session
def delete_object(object: Base, session: scoped_session, days_before: int = 30):
    max_entry_datetime = now_subtract(days=days_before)
    query = session.query(object)

    try:
        query.filter(object.execution_date <= max_entry_datetime).delete()
    except AttributeError:
        query.filter(object.last_scheduler_run <= max_entry_datetime).delete()


def now_subtract(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)
