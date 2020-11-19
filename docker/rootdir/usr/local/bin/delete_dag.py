import sys

from airflow import configuration
from sqlalchemy import create_engine

dag_input = sys.argv[1]

dsn = configuration.get('core', 'sql_alchemy_conn')
engine = create_engine(dsn, echo=True)
conn = engine.connect()

for t in ["xcom", "task_instance", "sla_miss", "log", "job", "dag_run", "dag"]:
    sql = "delete from {} where dag_id like '{}%%'".format(t, dag_input)
    print(sql)
    conn.execute(sql)
