from configs.constructors.table import BigQueryTable

REFER_A_FRIEND_TABLES = (
    BigQueryTable(name="order"),
    BigQueryTable(name="referral"),
    BigQueryTable(name="voucher"),
)
