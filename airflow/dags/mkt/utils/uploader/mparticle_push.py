"""
Defines a function to push from S3 to mParticle using the mParticle Python SDK.

It is intended do be used as `python_callable` in a  PythonVirtualenvOperator to overcome the fact
that mParticle Python SDK is not installed in the Airflow environment. All caveats as per
https://airflow.apache.org/docs/stable/_api/airflow/operators/python_operator/index.html#airflow.operators.python_operator.PythonVirtualenvOperator
apply.

For example
```python
audience_mparticle_feed_mparticle_audience_uploader = PythonVirtualenvOperator(
    task_id="audience_mparticle_feed_mparticle_audience_uploader",
    python_callable=uploader_orchestration,
    requirements=["mparticle"],
    op_kwargs={
        "mparticle_conn_id": "{{ mparticle_conn_id }}",
        "aws_conn_id": AWS_CONNECTION,
        "mparticle_s3_bucket": MPARTICLE_S3_BUCKET,
        "mparticle_s3_prefix": MPARTICLE_S3_PREFIX,
        "environment": ENVIRONMENT,
    },
    retries=0,
)
```
"""


def mparticle_uploader_orchestration(
    mparticle_conn_id,
    aws_conn_id,
    mparticle_s3_bucket,
    mparticle_s3_prefix,
    environment="development",
):
    """
    Orchestration method to iterate over data to build mParticle batches
    and push them

    :param mparticle_conn_variable_id: name of the Airflow variable that contains mParticle
           authorization
    :type: string

    :param aws_conn_id: name of the Airflow connection that contains AWS authorization
    :type: string

    :param mparticle_s3_bucket: mparticle_s3_bucket
    :type: string

    :param mparticle_s3_prefix: mparticle_s3_prefix
    :type: string

    :param environment: production or development
    :type: string
    """
    import csv
    import gzip
    import io
    import logging

    # TODO: add threads/multiprocess, please read the url below ;)
    # https://github.com/mParticle/mparticle-python-sdk/blob/master/mparticle/apis/events_api.py#L72
    import mparticle

    from airflow.exceptions import AirflowException
    from airflow.hooks.base_hook import BaseHook
    from airflow.hooks.S3_hook import S3Hook

    logger = logging.getLogger(__name__)

    MPARTICLE_BATCH_SIZE = 100
    MPARTICLE_HTTP_DEBUG = False

    def get_audience_data(aws_conn_id, mparticle_s3_bucket, mparticle_s3_prefix):
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)

        if not s3_hook.check_for_prefix(mparticle_s3_bucket, mparticle_s3_prefix, "/"):
            raise AirflowException(
                f"Diff {mparticle_s3_prefix} file not found in S3! Make sure that "
                "the unload S3 diff task has finished successfully or potentially "
                "rerun it."
            )

        for s3_key in s3_hook.list_keys(mparticle_s3_bucket, mparticle_s3_prefix):
            s3_file_object = s3_hook.get_key(s3_key, mparticle_s3_bucket)

            with gzip.GzipFile(fileobj=s3_file_object.get()["Body"]) as gzipfile:
                # https://docs.python.org/3/library/gzip.html#gzip.GzipFile
                csv_reader = csv.reader(io.TextIOWrapper(gzipfile))
                header = next(csv_reader)

                yield from (dict(zip(header, row)) for row in csv_reader)

    def mparticle_user_identities(data):
        identities = mparticle.UserIdentities()
        identities.customerid = data["customer_id"]
        return identities

    def mparticle_user_attributes(data):
        return {
            "source_id": data["source_id"],
            "customer_id": data["customer_id"],
            "audience_name": data["audience"],
            "channel_control_group": data["channel_control_group"],
        }

    def mparticle_device_info(data):
        device_info = mparticle.DeviceInformation()

        if data["device_type"] == "gps_adid":
            device_info.android_advertising_id = data["device_id"]
            device_info.device = device_info.android_advertising_id
        else:
            device_info.ios_advertising_id = data["device_id"]
            device_info.device = device_info.ios_advertising_id

        return device_info

    def mparticle_payload_batch(identities, user_attributes, device_info, environment):
        """
        Create mParticle Batch object

        :param identities: user information
        :type: mparticle.models.user_identities.UserIdentities

        :param user_attributes: custom user attributes
        :type: dict

        :param device_info: device information i.e. gpsadid or idfa
        :type: mparticle.models.device_information.DeviceInformation

        :param environment: production or development
        :type: str

        :return: valid batch object
        :rtype: mparticle.models.batch.Batch
        """
        logger.debug("Creating batch...")
        batch = mparticle.Batch()
        batch.environment = environment

        batch.user_identities = identities
        batch.user_attributes = user_attributes
        batch.device_info = device_info

        return batch

    def mparticle_get_config(mparticle_conn_id):
        """
        Set up mParticle configuration

        :param mparticle_conn_id: name of the Airflow variable that contains mParticle authorization
        :type: string

        :param http_debug: enable logging of HTTP traffic
        :type: boolean

        :return: mParticle object configuration
        :rtype: mparticle.configuration.Configuration
        """
        logger.info("Setting up mParticle configs...")

        mparticle_conn = BaseHook.get_connection(mparticle_conn_id)
        mparticle_api_key = mparticle_conn.login
        mparticle_api_secret = mparticle_conn.password

        configuration = mparticle.Configuration()
        configuration.api_key = mparticle_api_key
        configuration.api_secret = mparticle_api_secret
        configuration.debug = MPARTICLE_HTTP_DEBUG

        return configuration

    def push_mparticle_audience_data(config, payload):
        """
        Push events to mParticle

        :param config: mParticle object configuration
        :type: mparticle.configuration.Configuration

        :param payload: valid batch object
        :type: mparticle.models.batch.Batch
        """
        api_instance = mparticle.EventsApi(config)

        try:
            if isinstance(payload, list):
                api_instance.bulk_upload_events(payload)
                logger.info("Bulk of %s successfully uploaded", len(payload))
            else:
                api_instance.upload_events(payload)
                logger.info("Batch successfully uploaded")
        except mparticle.rest.ApiException as e:
            logger.exception("Exception while calling mParticle: %s", e)
            raise e

    mparticle_config = mparticle_get_config(mparticle_conn_id)

    logger.info(
        "Uploader configuration: %s",
        {
            "mparticle_config": mparticle_config,
            "environment": environment,
            "bulk_size": MPARTICLE_BATCH_SIZE,
            "http_debug": MPARTICLE_HTTP_DEBUG,
        },
    )
    logger.info("Bulking batches...")

    bulk_batch = []
    batch_counter = 0

    for data in get_audience_data(
        aws_conn_id, mparticle_s3_bucket, mparticle_s3_prefix
    ):
        # User batch
        batch = mparticle_payload_batch(
            mparticle_user_identities(data),
            mparticle_user_attributes(data),
            mparticle_device_info(data),
            environment,
        )
        bulk_batch.append(batch)
        batch_counter += 1

        if len(bulk_batch) == MPARTICLE_BATCH_SIZE:
            push_mparticle_audience_data(config=mparticle_config, payload=bulk_batch)
            bulk_batch.clear()

    # push bulk less than 100 batches
    if len(bulk_batch) > 0:
        push_mparticle_audience_data(config=mparticle_config, payload=bulk_batch)

    logger.info("%s batches pushed to mParticle", batch_counter)
