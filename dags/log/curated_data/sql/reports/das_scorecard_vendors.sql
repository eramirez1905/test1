CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.das_scorecard_vendors` AS
SELECT region
  , country_code
  , country_name
  , city_name
  , platform
  , COUNT(DISTINCT IF(delivery_provider = 'OWN_DELIVERY', vendor_code, NULL)) AS od_vendors
  , COUNT(DISTINCT IF(delivery_provider = 'VENDOR_DELIVERY', vendor_code, NULL)) AS vd_vendors
  , COUNT(DISTINCT vendor_code) AS all_vendors
  -- A: Percent active “OD” vendors with delivery areas
  , COUNT(DISTINCT
      IF(delivery_provider = 'OWN_DELIVERY' AND count_deliveryareas > 0, vendor_code, NULL)
    ) AS od_vendor_w_deliveryareas_nominator
  -- B: Percent active “VD” vendors with delivery areas
  , COUNT(DISTINCT
      IF(delivery_provider = 'VENDOR_DELIVERY' AND count_deliveryareas > 0, vendor_code, NULL)
    ) AS vd_vendor_w_deliveryareas_nominator
  -- C: Percent delivery areas that are based on drive time polygons
  , COUNT(DISTINCT IF(delivery_provider = 'OWN_DELIVERY' AND has_drivetime, vendor_code, NULL)) AS od_vendors_based_on_drive_time_nominator
  -- D: Percent drive time polygons that are based on custom profiles
  , COUNT(DISTINCT
        IF(vehicle_profile NOT IN ('car', 'bicycle', 'default')
          AND vehicle_profile IS NOT NULL, vendor_code, NULL)
    ) AS vendors_cust_vehicle_profiles_nominator
  , COUNT(DISTINCT IF(vehicle_profile IS NOT NULL, vendor_code, NULL)) AS vendors_cust_vehicle_profiles_denominator
  -- E: Percent vendors that have multiple (dynamic) delivery fees
  , COUNT(DISTINCT IF(delivery_provider = 'OWN_DELIVERY' AND has_dynamic_pricing, vendor_code, NULL)) AS od_vendors_w_dynamic_delivery_fee_nominator
  -- F: Percent “OD” vendors that are out of any zone
  , COUNT(DISTINCT
      IF(delivery_provider = 'OWN_DELIVERY' AND vendor_in_zone IS FALSE, vendor_code, NULL)
    ) AS od_vendors_out_of_zone_nominator
  -- G: Percent delivery areas that are cut at zone borders
  , COUNT(DISTINCT IF(delivery_provider = 'OWN_DELIVERY' AND deliveryarea_in_zone, vendor_code, NULL)) AS od_vendors_with_delivery_area_fully_cointained_nominator
  -- H: Percent vendors that are out of their delivery areas
  , COUNT(DISTINCT IF(delivery_provider = 'OWN_DELIVERY' AND vendor_in_deliveryarea IS FALSE, vendor_code, NULL)) AS od_vendors_outside_deliveryarea_nominator
   --EXTRA
  , COUNT(DISTINCT IF(delivery_provider = 'OWN_DELIVERY' AND deliveryarea_not_in_zone, vendor_code, NULL)) AS vendors_with_deliveryarea_out_of_zone_nominator
  , SUM(count_deliveryareas) AS total_deliveryareas
  , SUM(IF(delivery_provider = 'OWN_DELIVERY', count_deliveryareas, NULL)) AS total_OD_deliveryareas
FROM `{{ params.project_id }}.rl.das_scorecard_report`
GROUP BY 1,2,3,4,5
