WITH accounting_agg_vat_groups AS (
  SELECT
    accounting_uuid,
    ARRAY_AGG(
      STRUCT(
        amount_local,
        base_amount_local,
        percentage
      )
    ) AS vat_groups
  FROM `{project_id}.pandata_intermediate.pd_accounting_vat_groups`
  GROUP BY accounting_uuid
)

SELECT
  accounting.uuid,
  accounting.id,
  accounting.rdbms_id,
  accounting.order_uuid,
  accounting.order_id,
  accounting.charity_local,
  accounting.charity_plus_vat_local,
  accounting.container_charges_local,
  accounting.container_charges_plus_vat_local,
  accounting.delivery_fee_local,
  accounting.delivery_fee_plus_vat_local,
  accounting.delivery_fee_adjustment_local,
  accounting.delivery_fee_adjustment_plus_vat_local,
  accounting.difference_to_minimum_order_value_local,
  accounting.difference_to_minimum_order_value_plus_vat_local,
  accounting.discount_local,
  accounting.discount_plus_vat_local,
  accounting.gross_total_local,
  accounting.joker_commission_base_local,
  accounting.joker_commission_base_plus_vat_local,
  accounting.joker_fee_local,
  accounting.joker_fee_plus_vat_local,
  accounting.products_local,
  accounting.products_vat_local,
  accounting.products_plus_vat_local,
  accounting.rider_tip_local,
  accounting.rider_tip_plus_vat_local,
  accounting.service_fee_local,
  accounting.service_fee_plus_vat_local,
  accounting.vouchers_local,
  accounting.vouchers_plus_vat_local,
  accounting.voucher_ratio_foodpanda,
  accounting.discount_ratio_foodpanda,
  accounting.created_at_utc,
  accounting.created_date_utc,
  accounting.updated_at_utc,
  accounting.dwh_last_modified_at_utc,
  accounting_agg_vat_groups.vat_groups,
FROM `{project_id}.pandata_intermediate.pd_accounting` AS accounting
LEFT JOIN accounting_agg_vat_groups
       ON accounting.uuid = accounting_agg_vat_groups.accounting_uuid
