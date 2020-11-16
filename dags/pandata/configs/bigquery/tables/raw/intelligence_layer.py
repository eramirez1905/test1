from configs.constructors.table import BigQueryTable

INTELLIGENCE_LAYER_TABLES = (
    BigQueryTable(name="dim_group_order"),
    BigQueryTable(name="dim_group_order_expedition_type"),
    BigQueryTable(name="dim_group_order_participant"),
    BigQueryTable(name="dim_group_order_product"),
    BigQueryTable(name="dim_group_order_product_topping"),
    BigQueryTable(name="fct_order_commissions"),
    BigQueryTable(name="v_commissions"),
    BigQueryTable(name="v_crm_comp_issues_tickets"),
    BigQueryTable(name="v_dh_review_food_ratings"),
    BigQueryTable(name="v_dim_countries"),
    BigQueryTable(name="v_dim_date"),
    BigQueryTable(name="v_dim_exchange_rates"),
    BigQueryTable(name="v_salesforce_dh_dim_accounts"),
    BigQueryTable(name="v_salesforce_dh_dim_additional_charges"),
    BigQueryTable(name="v_salesforce_dh_dim_billing_contacts"),
    BigQueryTable(name="v_salesforce_dh_dim_case_activities"),
    BigQueryTable(name="v_salesforce_dh_dim_case_history"),
    BigQueryTable(name="v_salesforce_dh_dim_cases"),
    BigQueryTable(name="v_salesforce_dh_dim_contracts"),
    BigQueryTable(name="v_salesforce_dh_dim_leads"),
    BigQueryTable(name="v_salesforce_dh_dim_opportunities"),
    BigQueryTable(name="v_salesforce_dh_dim_opportunity_history"),
    BigQueryTable(name="v_salesforce_dh_dim_opportunity_line_items"),
    BigQueryTable(name="v_salesforce_dh_dim_users"),
    BigQueryTable(name="v_salesforce_dim_accounts"),
    BigQueryTable(name="v_salesforce_dim_approval_history"),
    BigQueryTable(name="v_salesforce_dim_case_activities"),
    BigQueryTable(name="v_salesforce_dim_case_history"),
    BigQueryTable(name="v_salesforce_dim_cases"),
    BigQueryTable(name="v_salesforce_dim_contracts"),
    BigQueryTable(name="v_salesforce_dim_opportunities"),
    BigQueryTable(name="v_salesforce_dim_opportunity_history"),
    BigQueryTable(name="v_salesforce_dim_opportunity_products"),
    BigQueryTable(name="v_salesforce_dim_partner_billing_contacts"),
    BigQueryTable(name="v_salesforce_dim_partner_leads"),
)