from configs.constructors.table import BigQueryTable

PAYPAL_TABLES = (
    BigQueryTable(name="transaction_reconciliation_report"),
    BigQueryTable(name="settlement_report"),
    BigQueryTable(name="dispute_report"),
)
