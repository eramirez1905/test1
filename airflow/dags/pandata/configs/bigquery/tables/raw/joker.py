from configs.constructors.table import BigQueryTable

JOKER_TABLES = (
    BigQueryTable(name="action_logs"),
    BigQueryTable(name="promotions"),
    BigQueryTable(name="reservation_statuses"),
    BigQueryTable(name="reservations"),
    BigQueryTable(name="restaurants"),
    BigQueryTable(name="users"),
)
