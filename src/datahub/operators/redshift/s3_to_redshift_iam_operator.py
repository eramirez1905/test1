"""
Loads data from S3 to AWS Redshift using a Redshift COPY operation and authenticates using IAM
roles.

IAM role ARN(s) is read from redshift_s3_connection_roles variable in redshift_connection extra dict.
To setup permissions and IAM role see these links
- Single account - Authorizing Amazon Redshift to access other AWS services
    https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html
- Cross account - create cross-account access between Amazon Redshift and Amazon S3
    https://aws.amazon.com/premiumsupport/knowledge-center/redshift-s3-cross-account/

Loosely based on:
https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/s3_to_redshift.py
"""
from typing import List, Optional

from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftIamOperator(BaseOperator):
    """
    Executes an COPY command to load files from S3 to Redshift

    :param schema: reference to a specific schema in Redshift database
    :type schema: str
    :param table: reference to a specific table in Redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key
    :type s3_key: str
    :param columns: reference to a list of columns to COPY in the Redshift table (optional)
    :type columns: list
    :param redshift_conn_id: reference to a specific Redshift database
    :type redshift_conn_id: str
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    :param autocommit: If set to True it will automatically commit the UNLOAD statement.
        Otherwise it will be committed right before the redshift connection gets closed.
    :type autocommit: bool
    """

    template_fields = ("schema", "table", "s3_bucket", "s3_key")
    template_ext = ()
    ui_color = "#dedede"

    @apply_defaults
    def __init__(
        self,
        schema: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        columns: Optional[List] = None,
        redshift_conn_id: str = "redshift_default",
        copy_options: Optional[List] = None,
        autocommit: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super(S3ToRedshiftIamOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id
        self.copy_options = copy_options or []
        self.autocommit = autocommit

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rs_conn_extra = postgres_hook.get_connection(self.redshift_conn_id).extra_dejson

        if "redshift_s3_connection_roles" not in rs_conn_extra:
            raise AirflowException(
                "redshift_s3_connection_roles is missing from connection:"
                f"{self.redshift_conn_id}'s extra field. At least one IAM role"
                " is required to use this operator. For cross account setup"
                " use comma seprated string of ARNs."
            )

        cred_string = f"aws_iam_role={rs_conn_extra['redshift_s3_connection_roles']}"
        columns_string = ""
        if self.columns:
            columns_string = ", ".join(self.columns)
            columns_string = f"({columns_string})"
        copy_options = "\n\t\t\t".join(self.copy_options)

        copy_query = f"""
            COPY {self.schema}.{self.table} {columns_string}
            FROM 's3://{self.s3_bucket}/{self.s3_key}'
            WITH CREDENTIALS '{cred_string}'
            {copy_options};
        """

        self.log.info("Executing COPY command...")
        postgres_hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")
