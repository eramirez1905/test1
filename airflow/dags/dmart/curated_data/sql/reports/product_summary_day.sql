CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_dmart.product_summary_day`
PARTITION BY created_date
CLUSTER BY country_code, chain_id AS
WITH id_date_filter AS (
  WITH date_filter AS(
    -- the generated array contains date where data from catalog was first ingested
    -- since that date the consecutive dates are rising monotonically
    -- befor that date the dates are shredded/jittery
    SELECT * 
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-06-26', CURRENT_DATE())) AS created_date
  )
  SELECT date_filter.created_date report_date
    , vpp.global_id
    , vp.vendor_id
    , vp.country_code
  FROM date_filter
  JOIN `{{ params.project_id }}.dl_dmart.catalog_vendor_product_price` vpp
  ON date_filter.created_date >= vpp.created_date
  JOIN `{{ params.project_id }}.dl_dmart.catalog_vendor_product` vp
  ON vpp.vendor_product_id = vp.id
    AND vpp.country_code = vp.country_code
), vendor AS (
  SELECT v.id
    , v.country_code
    , v.category_tree_source
    , v.updated_date
    , vp.remote_vendor_id
    , v.global_id
  FROM `{{ params.project_id }}.hl_dmart.catalog_vendor` v
  JOIN `{{ params.project_id }}.dl_dmart.catalog_platform_vendor` vp
  ON v.id = vp.vendor_id 
    AND v.country_code = vp.country_code
  WHERE NOT v.deleted
    AND remote_vendor_id IS NOT NULL
), prices AS (
  WITH prices_events AS (
    SELECT country_code
      , vendor_product_id
      , global_id
      , DATE(updated_date) AS report_date
      , ARRAY_AGG(
          STRUCT(DATE(COALESCE(updated_date, creation_date)) AS change_date
            , COALESCE(updated_date, creation_date) AS updated_at
            , price
            , original_price
          )
        ) OVER(PARTITION BY country_code, vendor_product_id ORDER BY DATE(updated_date) ASC) AS prices
    FROM `{{ params.project_id }}.hl_dmart.catalog_vendor_product_price`
  ), close_price AS (
    SELECT country_code 
      , vendor_product_id
      , global_id
      , report_date
      , (SELECT AS STRUCT COALESCE(original_price,price) min_price_close
          , COALESCE(original_price,price) max_price_close
          , IF(original_price IS NOT NULL,price,NULL) min_discount_price_close
          , IF(original_price IS NOT NULL,price,NULL) max_discount_price_close
        FROM
        (SELECT AS STRUCT *
          , ROW_NUMBER() OVER(PARTITION BY country_code, vendor_product_id 
        ORDER BY DATE(updated_at) DESC) _row_number
        FROM UNNEST(pe.prices)) ranked
        WHERE ranked._row_number = 1) close_price
    FROM prices_events pe
  ), close_price_flatten AS (
    SELECT DISTINCT country_code 
      , vendor_product_id
      , global_id
      , report_date
      , close_price.*
    FROM close_price pe
  ), min_max_price AS (
    -- Part responsible for calculating change in price on a given day
    -- 
    -- If order date is the same as price change date the query should return 
    -- maximum and minimum available prices at that particular date.
    -- It is worth noting that prices can change multiple times during the day
    -- and during previous relevant event (the last time before a report date
    -- that the price was changed). The process needs to take into account all
    -- price changes on a report day and latest event from any previous change.
    SELECT country_code 
      , vendor_product_id
      , global_id
      , report_date
      , (SELECT AS STRUCT MIN(COALESCE(original_price,price)) min_price
          , MAX(COALESCE(original_price,price)) max_price
          , MIN(IF(original_price IS NOT NULL,price,NULL)) min_discount_price
          , MAX(IF(original_price IS NOT NULL,price,NULL)) max_discount_price
        FROM (
          SELECT AS STRUCT *
            , ROW_NUMBER() OVER(PARTITION BY country_code, vendor_product_id, _event_rank_date
          ORDER BY updated_at DESC) _event_rank_for_date
          FROM (
            SELECT AS STRUCT *
              , DENSE_RANK() OVER(PARTITION BY country_code, vendor_product_id 
            ORDER BY DATE(updated_at) DESC) _event_rank_date
            FROM UNNEST(pe.prices)
          )
        ) ranked
        WHERE ranked._event_rank_date = 1 
          OR (ranked._event_rank_date = 2 
            AND ranked._event_rank_for_date = 1)) mm_price
    FROM prices_events pe
  ), min_max_price_flatten AS (
    SELECT distinct country_code 
      , vendor_product_id
      , global_id
      , report_date
      , mm_price.*
    FROM min_max_price 
  )
  SELECT cpr.*
    , mmpr.min_price
    , mmpr.max_price
    , mmpr.min_discount_price
    , mmpr.max_discount_price   
  FROM close_price_flatten cpr
  JOIN min_max_price_flatten mmpr
  ON cpr.country_code = mmpr.country_code 
    AND cpr.vendor_product_id = mmpr.vendor_product_id
    AND cpr.report_date = mmpr.report_date
), orders_data AS (
  SELECT DATE(po.placed_at) AS day_start
    , pp.external_id
    , po.vendor_external_id
    , sum(pp.quantity) AS units_sold
    , sum(pp.total_price) AS revenue
  FROM `{{ params.project_id }}.dl_dmart.pelican_orders` po
  JOIN `{{ params.project_id }}.dl_dmart.pelican_product` pp
  ON po.id = pp.order_id
  GROUP BY day_start
    , pp.external_id
    , po.vendor_external_id
), campaigns AS (
  SELECT vp.country_code
    , vp.id
    , ARRAY_AGG(t.name) AS campaigns
  FROM `{{ params.project_id }}.dl_dmart.catalog_vendor_product` vp
  LEFT JOIN `{{ params.project_id }}.dl_dmart.catalog_vendor_product_tag` vpt
  ON vp.country_code = vpt.country_code
    AND vp.id = vpt.vendor_product_id 
  LEFT JOIN `{{ params.project_id }}.dl_dmart.catalog_tag` t
  ON vpt.country_code = t.country_code
    AND vpt.tag_id = t.id
  WHERE t.type = 'campaign'
    AND t.name != 'campaign-offer'
  GROUP BY vp.country_code
    , vp.id
), brands AS (
  SELECT vp.id
    , vp.country_code
    , cp.chain_id
    , cp.sku
    , ARRAY_AGG(COALESCE(b.name, '')) brand
  FROM `{{ params.project_id }}.dl_dmart.catalog_vendor_product` vp
  LEFT JOIN `{{ params.project_id }}.dl_dmart.catalog_chain_product` cp
  ON vp.country_code = cp.country_code
    AND vp.chain_product_id = cp.id
  LEFT JOIN `{{ params.project_id }}.dl_dmart.catalog_master_product` mp
  ON mp.country_code = cp.country_code
    AND mp.id = cp.master_product_id
  LEFT JOIN `{{ params.project_id }}.dl_dmart.catalog_brand` b
  ON mp.country_code = b.country_code
    AND mp.brand_id = b.id
  GROUP BY vp.id
    , vp.country_code
    , cp.chain_id
    , cp.sku
), category_hashes AS (
  WITH vendor_hashes AS (
    SELECT vp.id
      , vp.country_code
      , ARRAY_AGG(c.hash) category_hashes
    FROM `{{ params.project_id }}.dl_dmart.catalog_vendor_product` vp
    JOIN `{{ params.project_id }}.dl_dmart.catalog_vendor_product_category` vpc
    ON vp.id = vpc.vendor_product_id
      AND vp.country_code = vpc.country_code
    JOIN `{{ params.project_id }}.dl_dmart.catalog_category` c
    ON vpc.category_id = c.id
      AND vpc.country_code = c.country_code
    GROUP BY vp.id
      , vp.country_code
  ), chain_hashes AS (
    SELECT vp.id
      , vp.country_code
      , ARRAY_AGG(cc.hash) category_hashes
    FROM `{{ params.project_id }}.dl_dmart.catalog_vendor_product` vp
    JOIN `{{ params.project_id }}.dl_dmart.catalog_chain_product` cp
    ON vp.chain_product_id  = cp.id
      AND vp.country_code = cp.country_code
    JOIN `{{ params.project_id }}.dl_dmart.catalog_chain_product_chain_category` cpcc
    ON cp.id = cpcc.chain_product_id
      AND cp.country_code = cpcc.country_code
    JOIN `{{ params.project_id }}.dl_dmart.catalog_chain_category` cc
    ON cpcc.chain_category_id = cc.id
      AND cpcc.country_code = cc.country_code
    GROUP BY vp.id
      , vp.country_code
  )
  SELECT COALESCE(vh.id, ch.id) AS id
    , COALESCE(vh.country_code, ch.country_code) AS country_code
    , vh.category_hashes vendor_categories
    , ch.category_hashes chain_categories
  FROM vendor_hashes vh
  FULL JOIN chain_hashes ch
  ON ch.country_code = vh.country_code
    AND ch.id = vh.id
), orders_catalog_events AS (
  SELECT idf.report_date created_date
    , idf.global_id external_id
    , v.remote_vendor_id
    , od.units_sold
    , od.revenue
    , p.*
    , v.category_tree_source
    , v.global_id vendor_global_id
    , ROW_NUMBER() OVER(PARTITION BY idf.report_date, v.remote_vendor_id, idf.global_id
      ORDER BY p.report_date DESC) AS _prices_event_dates
    , ROW_NUMBER() OVER(PARTITION BY idf.report_date, v.remote_vendor_id, idf.global_id
        ORDER BY DATE(v.updated_date) DESC) AS _vendor_event_dates
  FROM id_date_filter idf
  LEFT JOIN orders_data od
  ON idf.global_id = od.external_id
    AND idf.report_date = od.day_start
  JOIN vendor v
  ON idf.vendor_id = v.id
    AND idf.country_code = v.country_code
    AND DATE(v.updated_date) <= idf.report_date
  JOIN prices p
  ON idf.global_id  = p.global_id
    AND p.report_date <= idf.report_date
)
SELECT e.created_date
  , e.external_id vendor_product_global_id
  , e.vendor_global_id
  , TO_JSON_STRING(IF(e.category_tree_source = 'CHAIN', h.chain_categories, h.vendor_categories)) category_hashes
  , b.brand
  , IF(e.created_date = e.report_date, e.min_price, e.min_price_close) min_price
  , IF(e.created_date = e.report_date, e.max_price, e.max_price_close) max_price
  , IF(e.created_date = e.report_date, e.min_discount_price, e.min_discount_price_close) min_discount_price
  , IF(e.created_date = e.report_date, e.max_discount_price, e.max_discount_price_close) max_discount_price
  , TO_JSON_STRING(IFNULL(c.campaigns, [])) campaigns
  , e.revenue
  , e.units_sold
  , e.country_code
  , b.chain_id
  , b.sku
FROM orders_catalog_events e
LEFT JOIN campaigns c
ON e.country_code = c.country_code
  AND e.vendor_product_id = c.id
LEFT JOIN brands b
ON e.country_code = b.country_code
  AND e.vendor_product_id = b.id
LEFT JOIN category_hashes h
ON e.country_code = h.country_code
  AND e.vendor_product_id = h.id
WHERE _prices_event_dates = 1
AND _vendor_event_dates = 1
