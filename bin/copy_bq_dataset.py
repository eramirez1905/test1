#!/usr/bin/env python3

import argparse
from google.cloud import bigquery
from google_auth_oauthlib import flow

parser = argparse.ArgumentParser()
parser.add_argument("oauth_client_secret", help="Path of the oauth client secret file")
parser.add_argument("--source-project-id", help="BigQuery source project ID", required=True)
parser.add_argument("--target-project-id", help="BigQuery target project ID", required=True)
parser.add_argument("--source-dataset", help="BigQuery source dataset", required=True)
parser.add_argument("--target-dataset", help="BigQuery target dataset", required=True)
args = parser.parse_args()

project = args.source_project_id
target_project = args.target_project_id
dataset_id = args.source_dataset
target_dataset_id = args.target_dataset

launch_browser = True
app_flow = flow.InstalledAppFlow.from_client_secrets_file(args.oauth_client_secret,
                                                          scopes=['https://www.googleapis.com/auth/bigquery'])

if launch_browser:
    app_flow.run_local_server()
else:
    app_flow.run_console()

credentials = app_flow.credentials

client = bigquery.Client(project=project, credentials=credentials)

tables = client.list_tables(dataset_id)

job_config = bigquery.CopyJobConfig()
job_config.write_disposition = "WRITE_TRUNCATE"

print("Tables contained in '{}':".format(dataset_id))
for table in tables:
    dest_table_ref = client.dataset(target_dataset_id, target_project).table(table.table_id)
    source_table_name = f'{table.project}.{table.dataset_id}.{table.table_id}'
    target_table_name = f'{dest_table_ref.project}.{dest_table_ref.dataset_id}.{dest_table_ref.table_id}'
    print(f'Copy `{source_table_name}` to `{target_table_name}`')

    job = client.copy_table(
        table,
        dest_table_ref,
        location="US",
        job_config=job_config,
    )
    job.result()
    print(f'`{target_table_name}` state={job.state}')
