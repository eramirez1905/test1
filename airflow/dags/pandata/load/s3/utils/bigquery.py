import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook


def copy_table(
    bigquery_conn_id: str,
    source_project_dataset_tables: str,
    destination_project_dataset_table: str,
):
    hook = BigQueryHook(bigquery_conn_id=bigquery_conn_id)
    cursor = hook.get_conn().cursor()
    try:
        cursor.run_copy(
            source_project_dataset_tables=source_project_dataset_tables,
            destination_project_dataset_table=destination_project_dataset_table,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )
    except Exception as err:
        if "currently a VIEW" in str(err):
            logging.info(f"Deleting view: {destination_project_dataset_table}...")
            cursor.run_table_delete(
                destination_project_dataset_table, ignore_if_missing=True
            )
            cursor.run_copy(
                source_project_dataset_tables=source_project_dataset_tables,
                destination_project_dataset_table=destination_project_dataset_table,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
            )
