CREATE OR REPLACE TABLE il.businesses
PARTITION BY created_date AS
  SELECT d.country_code
    , d.vendor_id AS business_id
    , d.vendor_code
    , d.vendor_name
    , CAST(d.created_at AS DATE) AS created_date
    , d.created_at
    , d.updated_at
    , SAFE.ST_Y(location) AS business_lat
    , SAFE.ST_X(location) AS business_lng
    , walk_in_time
    , walk_out_time
  FROM `{{ params.project_id }}.cl.vendors` d
;
