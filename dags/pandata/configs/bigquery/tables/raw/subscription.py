from configs.constructors.table import BigQueryTable

SUBSCRIPTION_TABLES = (
    BigQueryTable(name="order"),
    BigQueryTable(name="subscription"),
    BigQueryTable(name="subscription_payment"),
)
