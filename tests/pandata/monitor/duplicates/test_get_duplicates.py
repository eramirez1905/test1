from configs import CONFIG
from configs.bigquery.datasets.constants import INTERMEDIATE_DATASET
from monitor.duplicates.constants import DUPLICATES_SQL
from utils.file import read_sql


def test_get_duplicates_sql(dry_run_query):
    dataset = CONFIG.gcp.bigquery.get(INTERMEDIATE_DATASET)
    dry_run_query(
        read_sql(
            DUPLICATES_SQL,
            id_fields="id",
            project=CONFIG.gcp.project,
            dataset=dataset.name,
            table="pd_cities",
            partition_filter="",
        )
    )
