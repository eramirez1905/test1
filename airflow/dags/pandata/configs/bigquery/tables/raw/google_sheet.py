from configs.constructors.table import GoogleSheetTable

from google.cloud.bigquery.schema import SchemaField

GOOGLE_SHEET_TABLES = [
    GoogleSheetTable(
        name="apac_lockdown_offline_vendors",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "1OCmRK7v398Uzbsi8iH4NLaalxz6F2ViAzXizisl2X2s"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="vendor_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="Country", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="Asked_on_", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="Offline_duration", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="rdbms_id", field_type="INTEGER", mode="NULLABLE"),
        ),
    ),
    GoogleSheetTable(
        name="commercial_staffing_onboarding",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "14KVuKzCe7R-7jI0fJW6ig9r1iZpcVOwYffLglcpI8h8"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="country", field_type="STRING", mode="NULLABLE"),
            SchemaField(
                name="date",
                field_type="DATE",
                mode="NULLABLE",
                description="%E4Y-%m-%d",
            ),
            SchemaField(name="sales_targets", field_type="FLOAT", mode="NULLABLE"),
            SchemaField(
                name="sales_targets_adjusted", field_type="FLOAT", mode="NULLABLE"
            ),
            SchemaField(name="nv_targets", field_type="INTEGER", mode="NULLABLE"),
            SchemaField(
                name="activation_staffing_forecast", field_type="FLOAT", mode="NULLABLE"
            ),
            SchemaField(
                name="activation_staffing_nv_forecast",
                field_type="FLOAT",
                mode="NULLABLE",
            ),
            SchemaField(
                name="content_2stg_staffing", field_type="FLOAT", mode="NULLABLE"
            ),
            SchemaField(
                name="menu_creation_staffing", field_type="FLOAT", mode="NULLABLE"
            ),
            SchemaField(
                name="menu_creation_qc_staffing", field_type="FLOAT", mode="NULLABLE"
            ),
            SchemaField(
                name="herolisting_staffing", field_type="INTEGER", mode="NULLABLE"
            ),
            SchemaField(
                name="postlive_menu_update_nonphoto_staffing",
                field_type="FLOAT",
                mode="NULLABLE",
            ),
            SchemaField(
                name="postlive_menu_update_photo_staffing",
                field_type="FLOAT",
                mode="NULLABLE",
            ),
            SchemaField(
                name="postlive_deal_creation_staffing",
                field_type="INTEGER",
                mode="NULLABLE",
            ),
            SchemaField(name="nv_staffing", field_type="FLOAT", mode="NULLABLE"),
            SchemaField(
                name="activation_productivity", field_type="INTEGER", mode="NULLABLE"
            ),
            SchemaField(
                name="troubleshooting_productivity",
                field_type="INTEGER",
                mode="NULLABLE",
            ),
            SchemaField(
                name="content_approval_productivity",
                field_type="INTEGER",
                mode="NULLABLE",
            ),
            SchemaField(
                name="menu_creation_productivity", field_type="INTEGER", mode="NULLABLE"
            ),
            SchemaField(
                name="menu_creation_qc_productivity",
                field_type="INTEGER",
                mode="NULLABLE",
            ),
            SchemaField(
                name="nv_creation_productivity", field_type="INTEGER", mode="NULLABLE"
            ),
            SchemaField(
                name="nv_update_productivity", field_type="INTEGER", mode="NULLABLE"
            ),
            SchemaField(
                name="onboarding_staffing_actuals",
                field_type="INTEGER",
                mode="NULLABLE",
            ),
            SchemaField(
                name="troubleshooting_staffing_actuals",
                field_type="INTEGER",
                mode="NULLABLE",
            ),
            SchemaField(
                name="troubleshooting_incidence_rate",
                field_type="FLOAT",
                mode="NULLABLE",
            ),
        ),
    ),
    GoogleSheetTable(
        name="billing_additional_payments",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "1-RVFd70GFGBbNlFbatKx5xWnZ53313P5m8CmCnfNFIc"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="country_iso", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="order_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="requester", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="reason", field_type="STRING", mode="NULLABLE"),
            SchemaField(
                name="order_type_affected", field_type="STRING", mode="NULLABLE"
            ),
            SchemaField(name="amount_local", field_type="FLOAT64", mode="NULLABLE"),
            SchemaField(name="created_at", field_type="TIMESTAMP", mode="NULLABLE"),
        ),
    ),
    GoogleSheetTable(
        name="lateness_penalty_fees",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "1rjvDXfStmQjjXDURCu7iZZKpCUoGj_cL2eYcWQ6ztug"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="country_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(
                name="lateness_13_minutes_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="lateness_33_minutes_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="cancellation_fee_local", field_type="NUMERIC", mode="NULLABLE"
            ),
            SchemaField(
                name="complaints_fee_local", field_type="NUMERIC", mode="NULLABLE"
            ),
            SchemaField(
                name="vendor_cancellation_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="lateness_13_minutes_discounted_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="lateness_33_minutes_discounted_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="cancellation_discounted_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="complaints_discounted_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
            SchemaField(
                name="vendor_cancellation_discounted_fee_local",
                field_type="NUMERIC",
                mode="NULLABLE",
            ),
        ),
    ),
    GoogleSheetTable(
        name="vendor_payout_ratio_adjustments",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "1Z8nlwmEQrhQVESOrhaogzTaSbLlDrKFA3ZMv0wGbssw"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="rdbms_id", field_type="INTEGER", mode="NULLABLE"),
            SchemaField(name="vendor_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="payout_ratio", field_type="FLOAT", mode="NULLABLE"),
        ),
    ),
    GoogleSheetTable(
        name="vendor_score_boosting",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "1VFd4JqWNH5HIM8nbkP_U8R55ye0qbDV4WvYSNFTXLtI"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="country_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="vendor_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="boost", field_type="NUMERIC", mode="NULLABLE"),
        ),
    ),
    GoogleSheetTable(
        name="vendors_kitchens_concepts",
        source_uri=(
            "https://docs.google.com/spreadsheets/d/"
            "1D-cuBE6zHddeomIkKCTtAZVZQ10wNQ9kw5NGRMS1xpk"  # pragma: allowlist secret
        ),
        schema_fields=(
            SchemaField(name="rdbms_id", field_type="INTEGER", mode="NULLABLE"),
            SchemaField(name="kitchen_id", field_type="INTEGER", mode="NULLABLE"),
            SchemaField(name="country", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="vendor_name", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="vendor_code", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="kitchen_name", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="kitchen_unit", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="rent_amount", field_type="STRING", mode="NULLABLE"),
            SchemaField(name="area", field_type="FLOAT", mode="NULLABLE"),
            SchemaField(name="is_vendor_active", field_type="BOOLEAN", mode="NULLABLE"),
            SchemaField(name="is_concepts", field_type="BOOLEAN", mode="NULLABLE"),
            SchemaField(name="restaurant_type", field_type="STRING", mode="NULLABLE"),
            SchemaField(
                name="date_added",
                field_type="DATE",
                mode="NULLABLE",
                description="%E4Y-%m-%d",
            ),
        ),
    ),
]
