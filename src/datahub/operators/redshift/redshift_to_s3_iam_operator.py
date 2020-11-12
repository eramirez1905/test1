"""
Transfers data from AWS Redshift into a S3 Bucket using IAM roles.
IAM role ARN(s) is read from redshift_s3_connection_roles variable in redshift_connection extra dict.
To setup permissions and IAM role see these links
- Single account - Authorizing Amazon Redshift to access other AWS services
    https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html
- Cross account - create cross-account access between Amazon Redshift and Amazon S3
    https://aws.amazon.com/premiumsupport/knowledge-center/redshift-s3-cross-account/
"""
from typing import List, Optional

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class RedshiftToS3IamOperator(BaseOperator):
    """
    Executes an UNLOAD command to s3

    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_prefix: reference to a specific S3 path inside the bucket.
    :type s3_prefix: str
    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table_name: reference to a specific table_name in redshift database
    :type table_name: str
    :param columns: reference to a list of columns in redshift table_name
    :type columns: list
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param unload_options: reference to a list of UNLOAD options
    :type unload_options: list
    :param autocommit: If set to True it will automatically commit the UNLOAD statement.
        Otherwise it will be committed right before the redshift connection gets closed.
    :type autocommit: bool
    :param include_header: If set to True the s3 file contains the header columns.
    :type include_header: bool
    """

    template_fields = ("schema", "table_name", "s3_bucket", "s3_prefix")
    template_ext = ()
    ui_color = "#ededed"

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        s3_bucket: str,
        s3_prefix: str,
        schema: str,
        table_name: str,
        columns: List = ["*"],
        redshift_conn_id: str = "redshift_default",
        unload_options: Optional[List] = None,
        autocommit: bool = True,
        include_header: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.schema = schema
        self.table_name = table_name
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id
        self.unload_options = unload_options or []
        self.autocommit = autocommit
        self.include_header = include_header

        if self.include_header and "HEADER" not in [
            uo.upper().strip() for uo in self.unload_options
        ]:
            self.unload_options = list(self.unload_options) + [
                "HEADER",
            ]

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
        columns_string = ", ".join(self.columns)
        unload_options = "\n\t\t\t".join(self.unload_options)

        select_query = f"SELECT {columns_string} FROM {self.schema}.{self.table_name}"
        unload_query = f"""
                    UNLOAD ('{select_query}')
                    TO 's3://{self.s3_bucket}/{self.s3_prefix}/{self.table_name}_'
                    WITH CREDENTIALS '{cred_string}'
                    {unload_options};
                    """

        self.log.info("Executing UNLOAD command...")
        postgres_hook.run(unload_query, self.autocommit)
        self.log.info("UNLOAD command complete.")
