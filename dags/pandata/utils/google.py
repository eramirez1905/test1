from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException

from constants.airflow import GOOGLE_CLOUD_CONN


def get_first_row_first_col(dataset_location: str, sql: str) -> str:
    hook = BigQueryHook(
        bigquery_conn_id=GOOGLE_CLOUD_CONN,
        use_legacy_sql=False,
        location=dataset_location,
    )
    cursor = hook.get_conn().cursor()
    cursor.execute(sql)
    return cursor.fetchone()[0]


def update_view(
    bigquery_conn_id: str, project_id: str, dataset_id: str, table_id: str, sql: str
) -> None:

    hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id)
    cursor = hook.get_conn().cursor()
    try:
        cursor.create_empty_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            view={"query": sql, "useLegacySql": False},
        )
    except AirflowException as err:
        if "ALREADY_EXISTS" in str(err):
            cursor.patch_table(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                view={"query": sql, "useLegacySql": False},
            )
        else:
            raise err
