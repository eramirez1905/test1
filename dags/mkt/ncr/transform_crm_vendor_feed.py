from io import BytesIO

from airflow.hooks.S3_hook import S3Hook
import pandas as pd


def transform_crm_vendor_feed(
    s3_key_src: str,
    s3_bucket_src: str,
    s3_key_dst: str,
    s3_bucket_dst: str,
    src_aws_conn_id: str,
    dst_aws_conn_id: str,
):
    """
    Gets object csv object from S3, loads into memory, extracts columns and saves
    as csv object in destination S3 bucket.
    """
    hook_src = S3Hook(aws_conn_id=src_aws_conn_id)
    hook_dst = S3Hook(aws_conn_id=dst_aws_conn_id)

    # Getting Dataframe from S3 object
    io = BytesIO()
    src_object = hook_src.get_key(s3_key_src, bucket_name=s3_bucket_src)
    src_object.download_fileobj(io)
    io.seek(0)

    df = pd.read_csv(io, low_memory=False)

    # Extracting necessary columns
    df = df[
        [
            "dwh_source_code",
            "local_vendor_id",
            "local_created_at",
            "local_deal_description",
            "local_deal_end_date",
            "local_deal_name",
            "local_deal_start_date",
            "local_deal_type",
        ]
    ]

    # Uploading to S3
    hook_dst.load_string(df.to_csv(index=None), s3_key_dst, bucket_name=s3_bucket_dst)
