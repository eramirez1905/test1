from collections import namedtuple
from pathlib import Path
import re
from typing import Dict, List, Tuple, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from jinja2 import Template


Column = namedtuple(
    "Column",
    ["name", "data_type", "column_attributes", "column_constraints"],
    defaults=[None, None],
)


class RedshiftCreateTableOperator(BaseOperator):
    """
    Creates table in Redshift.
    For more information about parameters take a look at:
        https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

    :param schema: reference to a specific schema in redshift database
    :param table_name: reference to a specific table_name in redshift schema
    :param columns: list of tuples containing column information in this order:
        (name, data_type, column_attributes, column_constraints), last two are optional.
    :param entity_to_privilege_grant: if set operator will also grant permission
        to newly created table, this should be a mapping between entity (group/user)
        and privilege to grant, e.g. (ALL/SELECT), example:
        {'GROUP "mkt_prd"': "ALL", "qa": "SELECT"}
    :param redshift_conn_id: reference to a specific redshift database
    """

    template_fields = ("schema", "table_name")

    with (Path(__file__).parent / "templates" / "ddl.sql").open() as fp:
        ddl_template = Template(fp.read())

    @apply_defaults
    def __init__(
        self,
        schema: str,
        table_name: str,
        columns: List[Tuple[str]],
        backup: bool = True,
        diststyle: Optional[str] = None,
        distkey: Optional[str] = None,
        sortkey_style: Optional[str] = None,
        sortkey_columns: Optional[List[str]] = None,
        entity_to_privilege_grant: Optional[Dict[str, str]] = None,
        redshift_conn_id: str = "redshift_default",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.table_name = table_name
        self.columns = columns
        self.backup = backup
        self.diststyle = diststyle
        self.distkey = distkey
        self.sortkey_style = sortkey_style
        self.sortkey_columns = sortkey_columns
        self.entity_to_privilege_grant = entity_to_privilege_grant
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        columns = []
        for column in self.columns:
            try:
                columns.append(Column(*column))
            except TypeError as e:
                # Checking which required column information is missing.
                missing_arguments = re.findall(r"'([a-z_]+)'", e.args[0])
                raise AirflowException(
                    f"Column {column} is missing {', '.join(missing_arguments)}."
                )

        backup = "YES" if self.backup else "NO"

        if self.sortkey_columns:
            sortkey = {
                "style": self.sortkey_style,
                "columns": ", ".join(self.sortkey_columns),
            }
        elif self.sortkey_style:
            raise AirflowException(
                "Cannot specify sortkey_style without specifying sortkey_columns."
            )
        else:
            sortkey = None

        if not self.diststyle == "KEY" and self.distkey:
            raise AirflowException("Can only specify distkey if diststyle is `KEY`")

        ddl_parameters = {
            "ddl_operator": {
                "schema": self.schema,
                "table_name": self.table_name,
                "columns": columns,
                "backup": backup,
                "diststyle": self.diststyle,
                "distkey": self.distkey,
                "sortkey": sortkey,
                "entity_to_privilege_grant": self.entity_to_privilege_grant,
            }
        }
        ddl_query = self.ddl_template.render(ddl_parameters)

        postgres_hook.run(ddl_query)
