#!/usr/bin/env python3
import argparse
import os
import shutil

from google.cloud import bigquery

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('project_id', type=str, help='BigQuery project')
parser.add_argument('business_unit', type=str, help='Business Unit')
parser.add_argument("-d", "--dry-run", action="store_true", help="dry run")
args = parser.parse_args()

project_id = args.project_id
dry_run = args.dry_run

client = bigquery.Client(
    project=project_id,
)

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
os.environ["AIRFLOW_BUSINESS_UNIT"] = args.business_unit


def get_logger():
    from airflow import LoggingMixin
    return LoggingMixin().log


def init(log):
    from configuration import config

    business_unit = config.get('business-unit').get('name')
    if business_unit != os.environ['AIRFLOW_BUSINESS_UNIT']:
        log.error(f"Please replace BUSINESS_UNIT with '{os.environ['AIRFLOW_BUSINESS_UNIT']}' in "
                  f"dags/{os.environ['AIRFLOW_BUSINESS_UNIT']}/configuration/yaml/config.yaml folder.")
        exit(1)
    log.info(f"Creating the environment for {business_unit}")

    return business_unit, config


def create_dataset(business_unit, log, config):
    datasets = config.get('bigquery').get('dataset')
    for key, dataset_id in datasets.items():
        if dataset_id != f"{key}_{os.environ['AIRFLOW_BUSINESS_UNIT'].replace('-', '_')}":
            log.error(f"Dataset '{dataset_id}' is invalid. "
                      f"Please update the dataset '{key}' with {key}_{os.environ['AIRFLOW_BUSINESS_UNIT']}")
            exit(1)

        log.info(f"Creating dataset {dataset_id}...")
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US"
        if key == "staging" or (key == "raw" and "staging" in project_id):
            log.info(f"Set default_table_expiration_ms to one day for {dataset_id}")
            dataset.default_table_expiration_ms = 60 * 60 * 24 * 1000  # one day
        if not dry_run:
            dataset = client.create_dataset(dataset, exists_ok=True, timeout=30)
            log.info(f"Created dataset {client.project}.{dataset.dataset_id}")

    print('----------------------------------------------------------------------------')
    google_iam = f"            # {business_unit}"
    for key, dataset_id in datasets.items():
        google_iam = google_iam + f"""
            {dataset_id}:
              WRITER:
                groupByEmail:
                  - team-group-email@deliveryhero.com
                userByEmail:
                  - service-account@bigquery-project.iam.gserviceaccount.com"""
    print(google_iam)
    print('----------------------------------------------------------------------------')
    log.info(f"Add the above config to src/datahub/access_control/google_iam/yaml/google_dataset_iam_production.yaml in the section {project_id}")


def create_business_unit_folder(log):
    skeleton_folder = f"{AIRFLOW_HOME}/dags/_skeleton"
    business_unit_folder = f"{AIRFLOW_HOME}/dags/{os.environ['AIRFLOW_BUSINESS_UNIT']}"
    if not os.path.isdir(business_unit_folder):
        log.info(f"Folder {business_unit_folder} does not exist.")
        if not dry_run:
            shutil.copytree(skeleton_folder, business_unit_folder)
        log.info(f"Folder {business_unit_folder} created")

        log.warning(f"Please replace BUSINESS_UNIT with '{os.environ['AIRFLOW_BUSINESS_UNIT']}' in "
                    f"dags/{os.environ['AIRFLOW_BUSINESS_UNIT']}/configuration/yaml/config.yaml folder.")
        log.warning("Then run again this command")
        exit(0)


def main():
    log = get_logger()

    create_business_unit_folder(log)
    business_unit, config = init(log)
    create_dataset(business_unit, log, config)

    if dry_run:
        log.info(f"Please add: \n"
                 f"------------------------------------------------\n"
                 f"CREATE DATABASE IF NOT EXISTS `airflow_{business_unit}`;\n"
                 f"------------------------------------------------\n"
                 f"to docker/mysql/docker-entrypoint-initdb.d/50-create-databases.sql\n")


main()
