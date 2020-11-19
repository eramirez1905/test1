from configs.bigquery.datasets.staging import BQ_CONFIG


def test_config_is_valid():
    for d in BQ_CONFIG.datasets:
        d.validate()
