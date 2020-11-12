WITH products_agg_product_variations AS (
  SELECT
    product_variations.product_uuid,
    ARRAY_AGG(
      STRUCT(
        product_variations.uuid,
        product_variations.id,
        product_variations.code,
        product_variations.title,
        product_variations.price_local,
        product_variations.sponsorship_price_local,
        product_variations.container_price_local,
        product_variations.is_deleted,
        product_variations.created_at_utc,
        product_variations.updated_at_utc,
        product_variations.dwh_last_modifed_at_utc
      )
    ) AS variations
  FROM `{project_id}.pandata_intermediate.pd_product_variations` AS product_variations
  LEFT JOIN `{project_id}.pandata_intermediate.pd_products` AS products
         ON product_variations.product_uuid = products.uuid
  GROUP BY product_variations.product_uuid
)

SELECT
  products.uuid,
  products.id,
  products.rdbms_id,
  products.menu_category_id,
  products.code,
  products.title,
  products.description,
  products.image_pathname,
  option_values.vat_rate,
  products.is_active,
  products.is_deleted,
  products.is_dish_information_excluded,
  products.has_dish_information,
  products.has_topping,
  products.is_express_item,
  products.is_prepacked_item,
  products.created_at_utc,
  products.updated_at_utc,
  products.dwh_last_modifed_at_utc,
  products_agg_product_variations.variations,
FROM `{project_id}.pandata_intermediate.pd_products` AS products
LEFT JOIN products_agg_product_variations
       ON products.uuid = products_agg_product_variations.product_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_option_values` AS option_values
       ON products.option_value_vat_uuid = option_values.uuid
