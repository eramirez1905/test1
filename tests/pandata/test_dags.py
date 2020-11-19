from airflow.models import DagBag


def test_validate_dags():
    dagbag = DagBag()
    assert len(dagbag.import_errors) == 0
