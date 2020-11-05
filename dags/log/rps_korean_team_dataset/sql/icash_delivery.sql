SELECT
   delivery_timestamp AS created_at
  , EXTRACT(DATE FROM DATETIME(delivery_timestamp, 'Asia/Seoul')) as created_date_local
  , DATETIME(delivery_timestamp, 'Asia/Seoul') AS created_at_local
  , external_id AS platform_order_code
  , external_restaurant_id AS platform_vendor_code
  , accepted_timestamp
  , dispatched_timestamp
  , deliver_at AS eta_timestamp
FROM `dl.icash_delivery`
WHERE region = 'kr'
