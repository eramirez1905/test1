from datetime import datetime, timedelta
import json

from airflow import DAG, configuration
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable

version = 1
dags_folder = configuration.get('core', 'dags_folder')
schedule = None
default_args = {
    'start_date': datetime(2019, 2, 28),
    'retries': 3,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=30),
}

import_project = Variable.get('ds-gcs-import-output-project', default_var='dh-logistics-data-science')
import_schema = Variable.get('ds-gcs-import-output-schema', default_var='imports')
import_bucket = Variable.get('ds-gcs-import-input-bucket', default_var='data-science-imports')

import_files_str = Variable.get('ds-gcs-import-files', default_var='{"csv": [], "json": []}')
import_files = json.loads(import_files_str)


def create_gcs_to_bq_operator(parent_dag, file, ext):
    source_format_mapping = {'csv': 'CSV',
                             'json': 'NEWLINE_DELIMITED_JSON'}
    source_format = source_format_mapping.get(ext)

    # use file name with underscore instead of dot as table_name
    table_name = file.replace('.', '_')

    operator = GoogleCloudStorageToBigQueryOperator(
        dag=parent_dag,
        bucket=import_bucket,
        source_objects=[file],
        task_id=table_name,
        destination_project_dataset_table=f'{import_project}.{import_schema}.{table_name}',
        field_delimiter=';',
        source_format=source_format,
        write_disposition='WRITE_TRUNCATE',
        # skip_leading_rows=1 + autodetect -> correct header detection
        skip_leading_rows=1,
        autodetect=True,
        google_cloud_storage_conn_id='bigquery_default'
    )
    return operator


dag = DAG(
    dag_id=f'import-datascience-gcs-imports-v{version}',
    description=f'Imports csv or newline-delimited json files from '
                f'datascience import bucket to import-schema, '
                f'truncating the table if it already exists.',
    schedule_interval=schedule,
    default_args=default_args,
    max_active_runs=1,
    catchup=False
)

dag.doc_md = """
You can specify csv (semicolon delimited) or json (newline formatted) files
in the variable `ds-gcs-import-files`:

```{
    "csv": ["file1.csv", "file2.csv"],
    "json": ["file3.json"]
}```

These will be imported from the defined gcs bucket (`ds-gcs-import-input-bucket`),
parsed using BigQuery's autodetect feature and then stored as tables named
`file1_csv, file2_csv, file3_json` in the specified project (`ds-gcs-import-output-project`)
and schema (`ds-gcs-import-output-schema`).
Make sure the file names are alphanumerical, the dot in front of the extension will be
replaced with an underscore. If the tables already exist, they will be recreated.
You can find sample files (`sample.csv` and `sample.json`) in the bucket `datascience_imports`.
"""

start_operator = DummyOperator(dag=dag, task_id='start')
end_operator = DummyOperator(dag=dag, task_id='end')

for file_ext, file_names in import_files.items():
    start_files_ext_operator = DummyOperator(dag=dag, task_id=f'start_{file_ext}_import')
    start_operator >> start_files_ext_operator

    # connect to end if there are no files to be imported
    if len(file_names) == 0:
        start_files_ext_operator >> end_operator
    else:
        for file_name in file_names:
            gcs_to_bq_operator = create_gcs_to_bq_operator(parent_dag=dag, file=file_name, ext=file_ext)
            start_files_ext_operator >> gcs_to_bq_operator >> end_operator
