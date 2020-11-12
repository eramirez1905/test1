WITH invoices_agg_fees AS (
  SELECT
    invoice_fees.invoice_uuid,
    ARRAY_AGG(
      STRUCT(
        fees.uuid,
        fees.id,
        fees.is_on_separate_invoice,
        fees.is_tax_applicable,
        fees.description,
        fees.quantity,
        fees.category_type,
        fees.billing_frequency,
        fees.single_amount_local,
        fees.billing_end_at_utc,
        fees.billing_start_at_utc,
        fees.created_at_utc,
        fees.updated_at_utc,
        fees.dwh_last_modified_utc
      )
    ) AS fees
  FROM `{project_id}.pandata_intermediate.bl_invoice_fees` AS invoice_fees
  INNER JOIN `{project_id}.pandata_intermediate.bl_fees` AS fees
          ON invoice_fees.fee_uuid = fees.uuid
  GROUP BY invoice_fees.invoice_uuid
),

invoices_agg_orders AS (
  SELECT
    invoice_uuid,
    ARRAY_AGG(
      STRUCT(
        uuid,
        id,
        pd_order_code,
        pd_delivery_provider_type,
        pd_order_status_code,
        payment_type,
        invoice_number,
        invoice_tax,
        already_received_amount_local,
        balance_local,
        foodpanda_discount,
        vendor_discount_local,
        service_tax_local,
        vat_local,
        online_payment_charge_local,
        container_charge_local,
        delivery_fee_local,
        service_fee_local,
        restaurant_revenue_local,
        total_commission_local,
        total_price_local,
        product_price_local,
        ordered_at_utc,
        created_at_utc,
        dwh_last_modified_utc
      )
    ) AS orders
  FROM `{project_id}.pandata_intermediate.bl_invoice_order_data`
  GROUP BY invoice_uuid
),

commissions_agg_tiers AS (
  SELECT
    commission_uuid,
    ARRAY_AGG(
      STRUCT(
        uuid,
        id,
        lower_bound,
        standard_commission_value_percentage,
        standard_commission_value_fixed_amount_local,
        created_at_utc,
        updated_at_utc
      )
    ) AS tiers
  FROM `{project_id}.pandata_intermediate.bl_commission_tiers`
  GROUP BY commission_uuid
)

SELECT
  invoices.uuid,
  invoices.id,
  invoices.rdbms_id,
  invoices.bill_run_uuid,
  invoices.bill_run_id,
  invoices.client_uuid,
  invoices.client_id,
  invoices.email_message_id,
  invoices.amount_local,
  invoices.invoice_number,
  invoices.calculated_values,
  invoices.status,
  invoices.invoice_details_file_path,
  invoices.invoice_generation_type,
  invoices.is_invoice_generation_type_automatic,
  invoices.is_invoice_generation_type_manual,

  TIMESTAMP(DATETIME(invoices.invoicing_at_utc, countries.timezone)) AS invoicing_at_local,
  invoices.invoicing_at_utc,
  TIMESTAMP(DATETIME(invoices.from_utc, countries.timezone)) AS from_local,
  invoices.from_utc,
  TIMESTAMP(DATETIME(invoices.until_utc, countries.timezone)) AS until_local,
  invoices.until_utc,
  DATE(invoices.created_at_utc) AS created_date_utc,
  invoices.created_at_utc,
  invoices.updated_at_utc,
  invoices.dwh_last_modified_utc,

  invoices_agg_fees.fees,
  invoices_agg_orders.orders,
  STRUCT(
    commissions.uuid,
    commissions.id,
    commissions.commission_base_type,
    commissions.restaurant_revenue_type,
    commissions.tier_base_type,
    commissions.tier_calculation_type,
    commissions.invoicing_period,
    commissions.payment_charge_amount_local,
    commissions.payment_charge_percentage,
    commissions.start_date_utc,
    commissions.start_at_utc,
    commissions.end_date_utc,
    commissions.end_at_utc,
    commissions.created_at_utc,
    commissions.updated_at_utc,
    commissions.dwh_last_modified_utc,
    commissions_agg_tiers.tiers
  ) AS commissions
FROM `{project_id}.pandata_intermediate.bl_invoices` AS invoices
LEFT JOIN invoices_agg_fees
       ON invoices.uuid = invoices_agg_fees.invoice_uuid
LEFT JOIN invoices_agg_orders
       ON invoices.uuid = invoices_agg_orders.invoice_uuid
LEFT JOIN `{project_id}.pandata_intermediate.bl_commissions` AS commissions
       ON invoices.commission_uuid = commissions.uuid
LEFT JOIN commissions_agg_tiers
       ON commissions.uuid = commissions_agg_tiers.commission_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON countries.rdbms_id = invoices.rdbms_id
