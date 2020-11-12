CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.delivery_areas_issues_flag`
CLUSTER BY entity_id, vendor_code AS
WITH vendor AS (
  SELECT v.entity_id
    , v.vendor_code
    , delivery_provider
    , v.vehicle_profile
    , da.id AS deliveryarea_id
    , his.active_from
    , his.active_to
    , his.end_transaction_id
    , his.drive_time
    , IF(his.drive_time IS NOT NULL, da.id, NULL) AS deliveryarea_w_dt
    , his.shape AS deliveryarea_shape
    , ST_AREA(his.shape) AS deliveryarea_area
    --get the most recenly delivery area by his.active_from if one delivery_area has multiple active record in da.history
    , ROW_NUMBER() OVER (PARTITION BY v.entity_id, vendor_code, delivery_provider, da.id ORDER BY his.active_from DESC) AS most_recent
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST (delivery_provider) delivery_provider ON delivery_provider IN ('OWN_DELIVERY', 'VENDOR_DELIVERY')
  LEFT JOIN UNNEST (delivery_areas) da ON da.is_deleted IS FALSE
  LEFT JOIN UNNEST (da.history) his ON his.operation_type IN ('updated', 'created')
  WHERE v.is_active IS TRUE
    --we only get active DA using active_to = NULL.
    AND his.active_to IS NULL
    AND delivery_provider IS NOT NULL
), vendor_most_recent AS (
  SELECT vendor.* EXCEPT (most_recent)
    --get the count of distinct active delivery areas
    , COUNT(DISTINCT deliveryarea_id) OVER (PARTITION BY entity_id, vendor_code) AS deliveryarea_count
    --get the count of distinct active delivery areas with drive_time
    , COUNT(DISTINCT deliveryarea_w_dt) OVER (PARTITION BY entity_id, vendor_code) AS deliveryarea_w_dt_count
  FROM vendor
  WHERE most_recent = 1
    --for some entities, there are duplicate DAs as of wrong active_to, but use workaround his.end_transaction_id to eliminate those
    AND end_transaction_id IS NULL
), outlier_range AS (
  SELECT DISTINCT entity_id
    , vehicle_profile
    , drive_time
    , PERCENTILE_CONT(deliveryarea_area, 0.003) OVER (PARTITION BY entity_id, vehicle_profile, drive_time) AS outlier_range_lowerfence
    , PERCENTILE_CONT(deliveryarea_area, 0.50) OVER (PARTITION BY entity_id, vehicle_profile, drive_time) AS outlier_range_median
    , PERCENTILE_CONT(deliveryarea_area, 0.997) OVER (PARTITION BY entity_id, vehicle_profile, drive_time) AS outlier_range_upperfence
  FROM vendor_most_recent
  --drive_time IS NULL should not get any outlier_range
  WHERE drive_time IS NOT NULL
), zone_shape_agg AS (
  SELECT p.entity_id
    , ST_UNION_AGG(zo.shape) AS country_zone_shape
  FROM `{{ params.project_id }}.cl.countries`
  LEFT JOIN UNNEST (platforms) p
  LEFT JOIN UNNEST (cities) ci
  LEFT JOIN UNNEST (zones) zo
  WHERE zo.shape IS NOT NULL
    AND zo.is_active IS TRUE
  GROUP BY 1
), vendor_flag AS (
  SELECT v.* EXCEPT (deliveryarea_shape)
    , o.outlier_range_lowerfence
    , o.outlier_range_median
    , o.outlier_range_upperfence
    --get the area size of the part of DA, which is not in the intersection of country_zone_shape
    , ST_AREA(ST_DIFFERENCE (deliveryarea_shape, ST_INTERSECTION(deliveryarea_shape, country_zone_shape))) AS outofzone_area
    --the flag for no delivery area vendor
    , IF(deliveryarea_count = 0, TRUE, FALSE) AS no_deliveryarea
    --the flag for no drive time DA, nice to have but not one of the issue criterion
    , IF(v.drive_time IS NULL, TRUE, FALSE) AS if_no_drivetime
    --the flag for out of zone DA (IF THE DA IS COVERED BY THE ENTITY LEVEL ZONE AND SWITCH TRUE/FALSE)
    , NOT(ST_COVEREDBY(deliveryarea_shape, z.country_zone_shape)) AS if_outofzone
    --the flag for out of lower fence DA (only when drive_time IS NOT NULL)
    , CASE
        WHEN v.drive_time IS NULL THEN NULL
        ELSE IF (v.deliveryarea_area < o.outlier_range_lowerfence, TRUE, FALSE)
      END AS if_below_lowerfence
    --the flag for out of upper fence DA (only when drive_time IS NOT NULL)
    , CASE
        WHEN v.drive_time IS NULL THEN NULL
        ELSE IF (v.deliveryarea_area > o.outlier_range_upperfence, TRUE, FALSE)
      END AS if_above_upperfence
  FROM vendor_most_recent v
  LEFT JOIN zone_shape_agg z ON z.entity_id = v.entity_id
  LEFT JOIN outlier_range o ON v.entity_id = o.entity_id
    AND v.vehicle_profile = o.vehicle_profile
    AND v.drive_time = o.drive_time
), vendor_summary_flag AS (
  SELECT entity_id
    , vendor_code
    , delivery_provider
    , no_deliveryarea
    , deliveryarea_count
    --use LOGICAL_OR to see IF ANY DA of the vendor is flagged
    , LOGICAL_OR(if_outofzone) OVER (PARTITION BY entity_id, vendor_code) AS if_any_outofzone
    , LOGICAL_OR(if_below_lowerfence) OVER (PARTITION BY entity_id, vendor_code) AS if_any_below_lowerfence
    , LOGICAL_OR(if_above_upperfence) OVER (PARTITION BY entity_id, vendor_code) AS if_any_above_upperfence
    , deliveryarea_id
    , vehicle_profile
    , drive_time
    , if_no_drivetime
    , if_outofzone
    , if_below_lowerfence
    , if_above_upperfence
    , ROUND(deliveryarea_area, 4) AS deliveryarea_area
    --keep the outofzone_area decimal to 4 just for verification when the ratio is rounded to 0.0
    , ROUND(outofzone_area, 4) AS outofzone_area
    , ROUND(100 * SAFE_DIVIDE(outofzone_area, deliveryarea_area), 2) AS outofzone_ratio
    , ROUND(outlier_range_lowerfence, 2) AS outlier_range_lowerfence
    , ROUND(outlier_range_median, 2) AS outlier_range_median
    , ROUND(outlier_range_upperfence, 2) AS outlier_range_upperfence
  FROM vendor_flag
)
SELECT entity_id
  , vendor_code
  , delivery_provider
  , no_deliveryarea
  --the summary flag if any of the issue flags(except if_no_drivetime) is TRUE THEN TRUE, otherwise FALSE
  , COALESCE((no_deliveryarea OR if_any_outofzone OR if_any_below_lowerfence OR if_any_above_upperfence OR if_any_above_upperfence)
      , FALSE)AS has_deliveryarea_issues
  , deliveryarea_count
  , ARRAY_AGG(STRUCT(deliveryarea_id AS id
      , vehicle_profile
      , drive_time
      , if_no_drivetime
      , if_outofzone
      , if_below_lowerfence
      , if_above_upperfence
      , deliveryarea_area AS area
      , outofzone_area
      , outofzone_ratio
      , outlier_range_lowerfence
      , outlier_range_median
      , outlier_range_upperfence
  )) AS delivery_areas
FROM vendor_summary_flag
GROUP BY 1, 2, 3, 4, 5, 6
