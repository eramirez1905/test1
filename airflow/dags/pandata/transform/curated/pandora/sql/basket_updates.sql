WITH basket_update_products_agg_toppings AS (
  SELECT
    basket_update_product_uuid,
    ARRAY_AGG(
      STRUCT(
        topping_template_product_uuid,
        topping_template_product_id,
        title,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS toppings
  FROM `{project_id}.pandata_intermediate.pd_basket_update_product_toppings`
  GROUP BY basket_update_product_uuid
),

basket_updates_agg_products AS (
  SELECT
    basket_update_products.basket_update_uuid,
    ARRAY_AGG(
      STRUCT(
        basket_update_products.product_uuid,
        basket_update_products.product_id,
        basket_update_products.product_variation_uuid,
        basket_update_products.product_variation_id,
        basket_update_products.action,
        basket_update_products.original_quantity,
        basket_update_products.reason,
        basket_update_products.title,
        basket_update_products.variation_title,
        basket_update_products.packaging_fee_gross_local,
        basket_update_products.subtotal_gross_local,
        basket_update_products.created_at_utc,
        basket_update_products.updated_at_utc,
        basket_update_products.dwh_last_modified_at_utc,
        basket_update_products_agg_toppings.toppings
      )
    ) AS products
  FROM `{project_id}.pandata_intermediate.pd_basket_update_products` AS basket_update_products
  LEFT JOIN basket_update_products_agg_toppings
         ON basket_update_products.uuid = basket_update_products_agg_toppings.basket_update_product_uuid
  GROUP BY basket_update_products.basket_update_uuid
)

SELECT
  basket_updates.uuid,
  basket_updates.id,
  basket_updates.rdbms_id,
  orders.uuid AS order_uuid,
  orders.id AS order_id,

  basket_updates.meta_source,
  basket_updates.meta_user_type,
  basket_updates.created_at_utc,
  basket_updates.created_date_utc,
  basket_updates.updated_at_utc,
  basket_updates.dwh_last_modified_at_utc,
  basket_updates_agg_products.products,
FROM `{project_id}.pandata_intermediate.pd_basket_updates` AS basket_updates
LEFT JOIN basket_updates_agg_products
       ON basket_updates.uuid = basket_updates_agg_products.basket_update_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_orders` AS orders
       ON basket_updates.rdbms_id = orders.rdbms_id
      AND basket_updates.order_code = orders.code
