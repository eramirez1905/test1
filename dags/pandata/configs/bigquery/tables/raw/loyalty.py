from configs.constructors.table import BigQueryTable

LOYALTY_TABLES = (
    BigQueryTable(name="order"),
    BigQueryTable(name="vendor"),
)
