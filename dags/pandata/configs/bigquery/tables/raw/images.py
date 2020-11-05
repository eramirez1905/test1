from configs.constructors.table import BigQueryTable

IMAGES_TABLES = (
    BigQueryTable(name="analysis"),
    BigQueryTable(name="directories"),
    BigQueryTable(name="images"),
)
