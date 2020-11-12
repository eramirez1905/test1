SELECT
  menu_items.uuid,
  menu_items.id,
  menu_items.vci_vendor_uuid,
  menu_items.vci_vendor_id,
  menu_items.country_code,
  menu_items.source,
  menu_items.item_name,
  menu_items.price_local,
  menu_items.discounted_price_local,
  menu_items.is_alcohol,
  menu_items.is_popular,
  menu_items.description,
  menu_items.image_url,
  STRUCT(
    categories.uuid,
    categories.id,
    categories.name,
    categories.description,
    categories.created_at_utc
  ) AS category,
  menu_items.created_at_utc,
  DATE(menu_items.created_at_utc) AS created_date_utc,
  menu_items._ingested_at_utc,
FROM `{project_id}.pandata_intermediate.vci_menu_items` AS menu_items
LEFT JOIN `{project_id}.pandata_intermediate.vci_categories` AS categories
       ON menu_items.vci_category_uuid = categories.uuid
