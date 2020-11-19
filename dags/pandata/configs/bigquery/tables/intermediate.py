"""
Each table must have 'uuid' as a primary key or in other words, unique
within the table.
"""
from configs.constructors.table import (
    BigQueryTable,
    Field,
    TimePartitioningConfig,
    TimePartitioningType,
)


class InvalidPrefix(BaseException):
    pass


class PandoraBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "pd_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class BillingBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "bl_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class CorporateBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "cp_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class SubscriptionBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "sb_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class LogisticsBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "lg_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class RpsBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "rps_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class VciBigQueryView(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "vci_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


class VciBigQueryIncTable(BigQueryTable):
    def validate(self):
        BigQueryTable.validate(self)
        prefix = "vci_"
        if not self.name.startswith(prefix):
            raise InvalidPrefix(f"'{prefix}' is required as a prefix!")


INTERMEDIATE_TABLES = (
    BillingBigQueryView(
        name="bl_commission_tiers", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_contacts", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_clients", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_client_tax", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_commissions", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_fees", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_invoices", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_invoice_fees", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_invoice_order_data", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    BillingBigQueryView(
        name="bl_tax", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    CorporateBigQueryView(
        name="cp_companies", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    CorporateBigQueryView(
        name="cp_company_departments", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    CorporateBigQueryView(
        name="cp_addresses", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    CorporateBigQueryView(
        name="cp_company_locations", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_dates", fields=(Field(name="id", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_accounting", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_accounting_vat_groups",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_areas", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_basket_updates", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_basket_update_products",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_basket_update_product_toppings",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_calls", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_configuration", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_countries", fields=(Field(name="id", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_chain_menu_groups", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_cuisines", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_delivery_providers", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_cities", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_customers", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_customer_addresses", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_discounts", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_global_cuisines", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_discount_schedules", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_food_characteristics",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_fraud_validation_transactions",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_languages", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_menus", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_option_values", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_orders", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_order_payments", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_payment_types", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_products", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_blacklisted_phone_numbers",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_newsletter_users", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_product_variations", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_order_assignment_flows",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_order_products", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_order_toppings", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_menu_categories", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_roles", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_schedules", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_special_schedules", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_status", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_status_flows", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_social_logins", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_translations", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_users", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_users_roles", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_vendors", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_vouchers", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_vendors_additional_info",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_vendors_cuisines", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_vendors_payment_types",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_vendor_flows", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_vendor_configuration",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    PandoraBigQueryView(
        name="pd_vendor_chains", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_vendor_discounts", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    PandoraBigQueryView(
        name="pd_vendors_food_characteristics",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_applicants", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_orders", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_countries", fields=(Field(name="code", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_issues", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    VciBigQueryView(
        name="vci_vendors", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    VciBigQueryView(
        name="vci_categories", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    VciBigQueryView(
        name="vci_menu_items", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    VciBigQueryIncTable(
        name="vci_menu_items_inc",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="_ingested_at_utc",
        ),
    ),
    VciBigQueryIncTable(
        name="vci_categories_inc",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="_ingested_at_utc",
        ),
    ),
    LogisticsBigQueryView(
        name="lg_order_forecasts", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_riders", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_rider_deposit_payments",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_rider_referral_payments",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_delivery_areas_events",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_payments_basic_rules",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_payments_scoring_rules",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_payments_referral_rules",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_payments_quest_rules",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_payments_deposit_rules",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_vendors", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_shifts", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_rider_payments", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_rider_kpi", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_rider_special_payments",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_rider_wallet_transactions",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_unassigned_shifts", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    LogisticsBigQueryView(
        name="lg_utr_target_periods", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    RpsBigQueryView(
        name="rps_connectivity", fields=(Field(name="uuid", is_primary_key=True),),
    ),
    SubscriptionBigQueryView(
        name="sb_subscriptions", fields=(Field(name="uuid", is_primary_key=True),)
    ),
    SubscriptionBigQueryView(
        name="sb_subscription_payments",
        fields=(Field(name="uuid", is_primary_key=True),),
    ),
)
