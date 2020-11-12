
from textwrap import dedent

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.hooks.S3_hook import S3Hook

from configuration import config


def get_yesterday():
    """Get yesterday's date (as local date)."""
    # - when data for the forecast is consumed from the feature store,
    # we use the data until yesterday, stored in {feature_store_path}/{date}/{country_code}/
    # - to get yesterdays date, instead of {{ds}} we use {{macros.ds_add(next_ds, -1)}} to reflect
    # yesterday of the date of the execution (next_execution_date)
    # - if the dag's {{next_execution_date}} is anything between 2019-05-06 00:00:00
    # and 2019-05-06 23:59:59 {{macros.ds_add(next_ds, -1)}} will be 2019-05-05.
    # - if we would use plain ds we would have a different date depending on the schedule
    return '{{macros.ds_add(next_ds, -1)}}'


def get_run_date():
    """Get today's date, i.e. the run date (as local date)."""
    return '{{next_ds}}'


def get_countries(region,
                  countries_inactive=None,
                  countries_disabled=None):
    """Return only active and enabled countries."""
    if countries_disabled is None:
        countries_disabled = []
    if countries_inactive is None:
        countries_inactive = []
    return list(set(config['regions'][region]['countries'])
                - set(countries_inactive)
                - set(countries_disabled))


def get_region(country_code):
    """Get the region for specific country_code. Used for both the Order Forecast and the Incident Forecast"""
    # TODO: this can be done more elegantly..
    for region in config['regions']:
        countries = config['regions'][region]['countries']
        if country_code in countries:
            return region
    for cc_region in config['contact_center']['regions']:
        environments = config['contact_center']['regions'][cc_region]['environments']
        if country_code in environments:
            return cc_region
    for dmart_region in config['dmart-order-forecast']['regions']:
        countries = config['dmart-order-forecast']['regions'][dmart_region]['countries']
        if country_code in countries:
            return dmart_region
    return KeyError


def get_queue_url(region):
    """Get the SQS queue URL for a specific region. Used for both the Order Forecast and the Incident Forecast"""
    order_forecast_sqs = config['order-forecast']['sqs']
    incident_forecast_sqs = config['contact_center']['sqs']

    if region in order_forecast_sqs:
        return order_forecast_sqs[region]
    elif region in incident_forecast_sqs:
        return incident_forecast_sqs[region]
    else:
        return KeyError


def get_contact_center_environments(region):
    return config['contact_center']['regions'][region]['environments']


def get_aws_region(country_code):
    region = get_region(country_code)
    return config['regions'][region]['aws_region']


def get_aws_conn_id(region):
    """"Get aws_conn_id of the correct region, i.e. with correct region specified as extra in the connections.
        For contact center regions, which are called cc-eu, cc-ap..., drop 'cc-'."""

    region = region.split('-')[1] if 'cc-' in region else region
    return f'aws_default_' + region


def country_is_up_to_date(country_code):
    bq_hook = BigQueryHook(use_legacy_sql=False)
    conn = bq_hook.get_conn()
    cursor = conn.cursor()
    # select zones were datetime_end_midnight > datetime_end,
    # i.e. were midnight is after last data load, so the data
    # load is not complete yet; there should be none.
    sql = f'''\
    SELECT
      zone_id
    FROM forecasting.zones
    WHERE
      country_code = '{country_code}'
      AND datetime_end_midnight > datetime_end
    '''
    cursor.execute(dedent(sql))
    results = cursor.fetchall()

    # TODO: this could be designed a bit nicer, i.e.
    # not using a hook but bq to panda functionality,
    # fetching zones as data frame and then log the ones that
    # failed etc.

    return len(results) == 0


def model_weights_need_update():
    bq_hook = BigQueryHook(use_legacy_sql=False)
    conn = bq_hook.get_conn()
    cursor = conn.cursor()

    # query returns nothing if weights have been updated over the last 28 days
    sql = f'''\
    SELECT * FROM `forecasting.dataset_ts_model_weights`
    WHERE updated_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
    '''

    cursor.execute(dedent(sql))
    results = cursor.fetchall()

    return len(results) > 0


def forecast_does_not_exist(templates_dict, *args, **kwargs):
    s3_hook = S3Hook()

    model_output_bucket = templates_dict['model_output_bucket']
    model_output_path = templates_dict['model_output_path']
    date = templates_dict['date']
    country_code = templates_dict['country_code']
    model_name = templates_dict['model_name']

    # TODO: use model response json?
    s3_key = f'{model_output_path}/{date}/{country_code}/{model_name}/forecasts.csv.gz'

    file_exists = s3_hook.check_for_key(key=s3_key, bucket_name=model_output_bucket)

    return not file_exists


def get_forecast_output_schema():
    return [
        {
            "name": "country_code",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "zone_id",
            "type": "INT64",
            "mode": "REQUIRED",
        },
        {
            "name": "datetime",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "timezone",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "orders",
            "type": "FLOAT64",
            "mode": "REQUIRED",
        },
        {
            "name": "forecast_date",
            "type": "DATE",
            "mode": "REQUIRED",
        },
        {
            "name": "metadata",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "created_at",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "model_name",
            "type": "STRING",
            "mode": "REQUIRED",
        },
    ]


def get_incident_forecast_output_schema():
    return [
        {
            "name": "datetime",
            "type": "DATETIME",
            "mode": "NULLABLE",
        },
        {
            "name": "hour",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "country_iso",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "brand",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "contact_center",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "dispatch_center",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "timezone_local",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "timezone_cs_ps",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "timezone_dp",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "incident_type",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "incidents",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "created_at",
            "type": "DATETIME",
            "mode": "NULLABLE",
        },
        {
            "name": "model_name",
            "type": "STRING",
            "mode": "REQUIRED",
        }
    ]
