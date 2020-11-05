from typing import Optional

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python_operator import PythonOperator

from load.s3.utils.bigquery import copy_table


def load_table_and_update(
    dir_name: str,
    table_id: str,
    load_tables_dataset_id: str,
    final_tables_dataset_id: str,
    project_id: str,
    bigquery_conn_id: str,
    dag: DAG,
    bucket_name: str,
    key_prefix: str,
    upstream_task: Optional[BaseOperator] = None,
):
    table_loaded = load_table(
        dir_name=dir_name,
        table_id=table_id,
        load_tables_dataset_id=load_tables_dataset_id,
        project_id=project_id,
        bigquery_conn_id=bigquery_conn_id,
        dag=dag,
        bucket_name=bucket_name,
        key_prefix=key_prefix,
    )

    update_table = PythonOperator(
        task_id=f"update__{table_id}",
        python_callable=copy_table,
        op_kwargs={
            "bigquery_conn_id": bigquery_conn_id,
            "source_project_dataset_tables": (
                f"{project_id}."
                f"{load_tables_dataset_id}."
                f"{table_id}_{{{{ ds_nodash }}}}"
            ),
            "destination_project_dataset_table": (
                f"{project_id}.{final_tables_dataset_id}.{table_id}"
            ),
        },
        dag=dag,
    )
    if upstream_task:
        upstream_task >> table_loaded

    table_loaded >> update_table


def load_table(
    dir_name: str,
    table_id: str,
    load_tables_dataset_id: str,
    project_id: str,
    bigquery_conn_id: str,
    dag: DAG,
    bucket_name: str,
    key_prefix: str,
) -> PythonOperator:
    table = f"{project_id}.{load_tables_dataset_id}.{table_id}_{{{{ ds_nodash }}}}"

    load_gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id=f"load__{table_id}",
        bucket=bucket_name,
        source_objects=[f"{dir_name}/{table_id}/{key_prefix}*.parquet"],
        destination_project_dataset_table=table,
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        bigquery_conn_id=bigquery_conn_id,
        google_cloud_storage_conn_id=bigquery_conn_id,
        ignore_unknown_values=True,
        dag=dag,
    )

    return load_gcs_to_bigquery
