WITH rider_kpi_agg_deliveries_by_entity AS (
  SELECT
    rider_kpi.created_date_local,
    rider_kpi.country_code,
    rider_kpi.city_id,
    rider_kpi.zone_id,
    rider_kpi.rider_id,
    rider_kpi.batch_number,
    rider_kpi.vehicle_name,
    ARRAY_AGG(
      STRUCT(
        deliveries.entity,
        deliveries.entity_id,
        deliveries.completed_deliveries AS completed_deliveries_count,
        deliveries.cancelled_deliveries AS cancelled_deliveries_count,
        deliveries.cancelled_deliveries_no_customer AS cancelled_deliveries_no_customer_count,
        deliveries.cancelled_deliveries_after_pickup AS cancelled_deliveries_after_pickup_count,
        deliveries.picked_up_deliveries AS picked_up_deliveries_count,
        deliveries.rider_notified AS rider_notified_count,
        deliveries.rider_accepted AS rider_accepted_count,
        deliveries.undispatched_after_accepted AS undispatched_after_accepted_count,
        deliveries.effective_delivery_time_count,
        deliveries.at_vendor_time_count,
        deliveries.at_customer_time_count,
        deliveries.dropoff_distance_count,
        deliveries.pickup_distance_count,
        deliveries.reaction_time_sec_count,
        deliveries.to_vendor_time_count,
        deliveries.to_customer_time_count,
        deliveries.last_delivery_over_10_count,
        deliveries.at_customer_time_under_5_count,
        deliveries.at_customer_time_5_10_count,
        deliveries.at_customer_time_10_15_count,
        deliveries.at_customer_time_over_15_count,
        deliveries.effective_delivery_time_sum AS effective_delivery_time_sum_in_seconds,
        deliveries.at_vendor_time_sum AS at_vendor_time_sum_in_seconds,
        deliveries.at_customer_time_sum AS at_customer_time_sum_in_seconds,
        deliveries.dropoff_distance_sum AS dropoff_distance_sum_in_seconds,
        deliveries.pickup_distance_sum AS pickup_distance_sum_in_seconds,
        deliveries.reaction_time_sec_sum AS reaction_time_sec_sum_in_seconds,
        deliveries.to_vendor_time_sum AS to_vendor_time_sum_in_seconds,
        deliveries.to_customer_time_sum AS to_customer_time_sum_in_seconds
      )
    ) AS deliveries_by_entity
  FROM `fulfillment-dwh-production.curated_data_shared.rider_kpi` AS rider_kpi
  CROSS JOIN UNNEST (rider_kpi.deliveries) AS deliveries
  WHERE rider_kpi.created_date_local IS NOT NULL
    AND rider_kpi.rider_id IS NOT NULL
  GROUP BY
    rider_kpi.created_date_local,
    rider_kpi.country_code,
    rider_kpi.city_id,
    rider_kpi.zone_id,
    rider_kpi.rider_id,
    rider_kpi.batch_number,
    rider_kpi.vehicle_name
)

SELECT
  TO_HEX(
    SHA256(
      rider_kpi.created_date_local || '_'
      || rider_kpi.country_code || '_'
      || IFNULL(CAST(rider_kpi.city_id AS STRING), '_') || '_'
      || IFNULL(CAST(rider_kpi.zone_id AS STRING), '_') || '_'
      || rider_kpi.rider_id || '_'
      || IFNULL(CAST(rider_kpi.batch_number AS STRING), '_') || '_'
      || IFNULL(rider_kpi.vehicle_name, '_')
    )
  ) AS uuid,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(rider_kpi.country_code, rider_kpi.city_id),
    rider_kpi.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(rider_kpi.country_code, rider_kpi.city_id) AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_kpi.zone_id, rider_kpi.country_code) AS lg_zone_uuid,
  rider_kpi.zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_kpi.rider_id, rider_kpi.country_code) AS lg_rider_uuid,
  rider_kpi.rider_id AS lg_rider_id,

  rider_kpi.contract_type,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(rider_kpi.country_code) AS country_code,
  rider_kpi.country_name,
  rider_kpi.city_name,
  rider_kpi.zone_name,
  rider_kpi.rider_name,
  rider_kpi.email,
  rider_kpi.batch_number,
  rider_kpi.current_batch_number,
  rider_kpi.contract_name,
  rider_kpi.job_title,
  rider_kpi.rider_contract_status,
  rider_kpi.captain_name,
  rider_kpi.vehicle_profile,
  rider_kpi.vehicle_name,

  rider_kpi.tenure_in_weeks,
  rider_kpi.tenure_in_years,
  rider_kpi.time_to_street AS start_and_first_shift_diff_in_days,

  rider_kpi.shifts_done AS shifts_done_count,
  rider_kpi.late_shifts AS late_shift_count,
  rider_kpi.all_shifts AS all_shift_count,
  rider_kpi.no_shows AS no_show_count,
  rider_kpi.unexcused_no_shows AS unexcused_no_show_count,
  rider_kpi.weekend_shifts AS weekend_shift_count,

  rider_kpi.transition_working_time AS transition_working_time_in_seconds,
  rider_kpi.transition_busy_time AS transition_busy_time_in_seconds,
  rider_kpi.peak_time AS peak_time_in_seconds,
  rider_kpi.working_time AS working_time_in_seconds,
  rider_kpi.planned_working_time AS planned_working_time_in_seconds,
  rider_kpi.break_time AS break_time_in_seconds,
  rider_kpi.swaps_accepted AS swaps_accepted_count,
  rider_kpi.swaps_pending_no_show AS swaps_pending_no_show_count,
  rider_kpi.swaps_accepted_no_show AS swaps_accepted_no_show_count,

  rider_kpi.created_date_local,
  rider_kpi.contract_start_date_local,
  rider_kpi.contract_end_date_local,
  rider_kpi.contract_creation_date_local,
  rider_kpi.hiring_date_local,
  rider_kpi_agg_deliveries_by_entity.deliveries_by_entity,
FROM `fulfillment-dwh-production.curated_data_shared.rider_kpi` AS rider_kpi
LEFT JOIN rider_kpi_agg_deliveries_by_entity
       ON rider_kpi.created_date_local = rider_kpi_agg_deliveries_by_entity.created_date_local
      AND rider_kpi.country_code = rider_kpi_agg_deliveries_by_entity.country_code
      AND rider_kpi.city_id = rider_kpi_agg_deliveries_by_entity.city_id
      AND rider_kpi.zone_id = rider_kpi_agg_deliveries_by_entity.zone_id
      AND rider_kpi.rider_id = rider_kpi_agg_deliveries_by_entity.rider_id
      AND rider_kpi.batch_number = rider_kpi_agg_deliveries_by_entity.batch_number
      AND rider_kpi.vehicle_name = rider_kpi_agg_deliveries_by_entity.vehicle_name
WHERE rider_kpi.created_date_local IS NOT NULL
  AND rider_kpi.rider_id IS NOT NULL
