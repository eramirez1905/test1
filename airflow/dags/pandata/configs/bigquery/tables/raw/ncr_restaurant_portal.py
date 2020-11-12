from configs.constructors.table import BigQueryTable

NCR_RESTAURANT_PORTAL_TABLES = (
    BigQueryTable(name="restaurant"),
    BigQueryTable(name="user"),
    BigQueryTable(name="user_check_log"),
    BigQueryTable(name="user_restaurant"),
)
