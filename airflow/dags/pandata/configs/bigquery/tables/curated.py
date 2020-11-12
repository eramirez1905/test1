from configs.constructors.table import (
    BigQueryTable,
    Field,
    ClusterFieldConfig,
    TimePartitioningConfig,
    TimePartitioningType,
)


class PandoraTable(BigQueryTable):
    pass


class BillingTable(BigQueryTable):
    pass


class CorporateTable(BigQueryTable):
    pass


class SubscriptionTable(BigQueryTable):
    pass


class LogisticsTable(BigQueryTable):
    pass


class RpsTable(BigQueryTable):
    pass


class VciTable(BigQueryTable):
    pass


CURATED_TABLES = (
    PandoraTable(
        name="accounting",
        description="Table of accounting entries with each row representing one accounting entry",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents an accounting entry",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="basket_updates",
        description="Table of basket updates with each row representing one basket update",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a basket update",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="blacklisted_phone_numbers",
        description="Table of phone numbers blacklisted from the platform",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a blacklisted phone number",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    CorporateTable(
        name="cp_companies",
        description="Table of companies from corporate squad",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a company",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    CorporateTable(
        name="cp_company_addresses",
        description="Table of company addresses from corporate squad",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a company address",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="cp_company_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a company",
            ),
        ),
    ),
    PandoraTable(
        name="countries",
        description="Table of countries with each row representing one country nested with areas and cities",
        fields=(
            Field(
                name="id",
                is_primary_key=True,
                description="Each id is unique and represents a country",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="customers",
        description="Table of customers with each row representing one customer nested with addresses",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a customer",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="dates",
        description="Table of dates with each row representing one date",
        fields=(
            Field(
                name="id",
                is_primary_key=True,
                description="Each id is unique and represents a date",
            ),
        ),
    ),
    PandoraTable(
        name="customer_addresses",
        description="Table of customer addresses with each row representing one customer address",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a customer address",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="customer_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="This address belongs to the customer with this uuid. A customer can have more than one address.",
            ),
        ),
    ),
    PandoraTable(
        name="discounts",
        description="Table of discounts with each row representing one discount",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a discount",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="fraud_validation_transactions",
        description="Table of fraud validation transactions with each row representing one fraud validation transaction",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a fraud validation transaction",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="languages",
        description="Table of languages with each row representing one language",
        fields=(
            Field(name="uuid", is_primary_key=True),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="newsletter_users",
        description="Table of newsletter users with each row representing one person who signed up for the newsletter",
        fields=(
            Field(name="uuid", is_primary_key=True),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="products",
        description="Table of products with each row representing one product",
        fields=(
            Field(name="uuid", is_primary_key=True),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="orders",
        description="Table of orders with each row representing one order nested with products and toppings, calls",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents an order",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="created_date_utc",
                description="Order created date in UTC and partitioned by this field",
            ),
            Field(name="vat_rate", description="Vat rate in percentage"),
        ),
    ),
    PandoraTable(
        name="translations",
        description="Table of translation with each row representing one translation",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a translation",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    PandoraTable(
        name="users",
        description="Table of users with each row representing one user. A user is a user on pandora's backend, and is different from a customer that purchases from the platform.",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a user",
            ),
            Field(name="rdbms_id", cluster=ClusterFieldConfig()),
        ),
    ),
    PandoraTable(
        name="vouchers",
        description="Table of vouchers with each row representing one voucher",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a voucher",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="customer_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a customer",
            ),
        ),
    ),
    PandoraTable(
        name="vendors",
        description="Table of vendors with each row representing one vendor.",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a vendor",
            ),
            Field(name="rdbms_id", cluster=ClusterFieldConfig()),
        ),
    ),
    LogisticsTable(
        name="lg_applicants",
        description="Table of applicants for riders",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        fields=(
            Field(
                name="id",
                is_primary_key=True,
                description="Each id is unique and represents an applicant",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    BillingTable(
        name="bl_clients",
        description="Clients from Foodpanda Billing Tool",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each id is unique and represents a client",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    BillingTable(
        name="bl_invoices",
        description="Invoices from Foodpanda Billing Tool",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents an invoice",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="client_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Represents a unique client",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_countries",
        description="Table of countries in logistics",
        fields=(
            Field(
                name="code",
                is_primary_key=True,
                description="Each country code is unique and represents a country",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_issues",
        description="Table of issues in logistics",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each id is unique and represents an issue",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    VciTable(
        name="vci_menu_items",
        description="Table of vendor menu items from vendor competitive intelligence",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a menu item of a vendor in a platform",
            ),
            Field(
                name="created_date_utc",
                description="The time when the menu item was scraped in UTC timezone.",
            ),
        ),
    ),
    VciTable(
        name="vci_vendors",
        description="Table of vendors from vendor competitive intelligence",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a vendor",
            ),
            Field(
                name="created_date_utc",
                description="The time when the vendor was scraped in UTC timezone.",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_orders",
        description="Table of orders in logistics. Partitioned by created_date_utc, clustered by rdbms_id.",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents an order in logistics. Different from platform's, like pandora.",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_order_forecasts",
        description="Table of order forecasts from a year ago in logistics. Partitioned by created_date_utc, clustered by rdbms_id.",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents an order forecast in logistics",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_riders",
        description="Table of riders in logistics",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a rider in logistics",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_delivery_areas_events",
        description="Table of delivery area events in logistics. Partitioned by created_date_utc",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a transaction that is part of an event.",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_rider_referral_payments",
        description="Rider referral payments",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid represents a referral payment",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_rider_special_payments",
        description="Rider special payments in logistics",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid represents a referral payment",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_daily_rider_payments",
        description="Rider payments aggregated by rider and date",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid represents an aggregation of rider payments in a date. So this should not be used for any JOIN as it is not the payment_id",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_daily_rider_zone_kpi",
        description="Rider kpi aggregated by rider, date, zone, batch, vehicle name,",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_local",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid represents an aggregation of rider kpi by rider, date, zone, batch, vehicle name. So this should not be used for any JOIN",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_rider_deposit_payments",
        description="Rider deposit payments in logistics",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid represents a rider deposit payment",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_rider_wallet_transactions",
        description="Rider wallet transactions in logistics",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid represents a rider wallet transaction",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_vendors",
        description="Table of vendors in logistics. A vendor in pandora could belong to more than one entity in logistics.",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a vendor.",
            ),
            Field(name="code", description="Platform code. Same as pandora's.",),
            Field(name="pd_vendor_id", description="Pandora's vendor id",),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_shifts",
        description="Table of shifts in logistics.",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a shift",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="lg_rider_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a rider in logistics",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_payments_basic_rules",
        description="Payments basic rules with each row representing one rule",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a rule",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_payments_quest_rules",
        description="Payments quest rules with each row representing one rule",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a rule",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_payments_scoring_rules",
        description="Payments scoring rules with each row representing one rule",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a rule",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_payments_deposit_rules",
        description="Payments deposit rules with each row representing one rule",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a rule",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_payments_referral_rules",
        description="Payments referral rules with each row representing one rule",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a rule",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_utr_target_periods",
        description="UTR by periods with Historical changes of UTR suggestions",
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a UTR target period",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    LogisticsTable(
        name="lg_unassigned_shifts",
        description="Unassigned shifts in logistics",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents an unassigned shift",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
        ),
    ),
    RpsTable(
        name="rps_vendor_connectivity_slots",
        description="Table of vendor connectivity slots in RPS",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents vendor connectivity slot",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="vendor_code",
                cluster=ClusterFieldConfig(order=2),
                description="The code of a vendor on the platform.",
            ),
        ),
    ),
    SubscriptionTable(
        name="sb_subscriptions",
        description="Table of subscriptions with each row representing one subscription and it's associated payment(s).",
        time_partitioning_config=TimePartitioningConfig(
            partition_type=TimePartitioningType.DAY, field="created_date_utc",
        ),
        is_partition_filter_required=True,
        fields=(
            Field(
                name="uuid",
                is_primary_key=True,
                description="Each uuid is unique and represents a subscription",
            ),
            Field(
                name="rdbms_id",
                cluster=ClusterFieldConfig(order=1),
                description="Each country has one id and this table is clustered by this field",
            ),
            Field(
                name="customer_uuid",
                cluster=ClusterFieldConfig(order=2),
                description="Each uuid represents a customer",
            ),
        ),
    ),
)
