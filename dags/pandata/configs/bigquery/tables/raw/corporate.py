from configs.constructors.table import BigQueryTable

CORPORATE_TABLES = (
    BigQueryTable(name="address"),
    BigQueryTable(name="administrator"),
    BigQueryTable(name="company"),
    BigQueryTable(name="company_cost_center"),
    BigQueryTable(name="company_department"),
    BigQueryTable(name="company_location"),
    BigQueryTable(name="company_vendor"),
    BigQueryTable(name="expense_code"),
    BigQueryTable(name="invoice"),
    BigQueryTable(name="migrations"),
    BigQueryTable(name="order"),
    BigQueryTable(name="order_payment"),
    BigQueryTable(name="ordering_rule"),
    BigQueryTable(name="ordering_rule_company_department"),
    BigQueryTable(name="ordering_rule_company_location"),
    BigQueryTable(name="user"),
    BigQueryTable(name="user_expense_code"),
    BigQueryTable(name="user_location"),
    BigQueryTable(name="vendor"),
)
