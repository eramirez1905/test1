from configs.constructors.table import BigQueryTable

CLARISIGHTS_TABLES = (
    BigQueryTable(name="twitter"),
    BigQueryTable(name="snapchat"),
    BigQueryTable(name="adwords"),
    BigQueryTable(name="facebook"),
    BigQueryTable(name="apple"),
    BigQueryTable(name="liftoff"),
    BigQueryTable(name="remerge"),
    BigQueryTable(name="moloco"),
)
