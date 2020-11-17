from configs.constructors.table import BigQueryTable

REWARD_TABLES = (
    BigQueryTable(name="badge"),
    BigQueryTable(name="challenge"),
    BigQueryTable(name="challenge_action"),
    BigQueryTable(name="challenge_reward"),
    BigQueryTable(name="customer_badge"),
    BigQueryTable(name="customer_challenge"),
    BigQueryTable(name="customer_challenge_action"),
    BigQueryTable(name="customer_challenge_program"),
    BigQueryTable(name="customer_challenge_reward"),
    BigQueryTable(name="customer_scratch_card"),
    BigQueryTable(name="customer_voucher"),
    BigQueryTable(name="goose_db_version"),
    BigQueryTable(name="order_status_event"),
    BigQueryTable(name="order_submitted_event"),
    BigQueryTable(name="point_transaction"),
    BigQueryTable(name="scratch_card"),
    BigQueryTable(name="scratch_card_voucher"),
    BigQueryTable(name="timeline_event"),
    BigQueryTable(name="vendor"),
    BigQueryTable(name="vendor_attribute"),
    BigQueryTable(name="vendor_attribute_pivot"),
    BigQueryTable(name="vendor_chain"),
    BigQueryTable(name="vendor_event"),
    BigQueryTable(name="voucher"),
)
