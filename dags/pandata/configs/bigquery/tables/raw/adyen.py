from configs.constructors.table import BigQueryTable

ADYEN_TABLES = (
    BigQueryTable(name="dispute_report"),
    BigQueryTable(name="hpp_conversion_detail_report"),
    BigQueryTable(name="payments_accounting_report"),
    BigQueryTable(name="received_payments_report"),
    BigQueryTable(name="settlement_detail_report"),
    BigQueryTable(name="threedsecure_conversion_report"),
)
