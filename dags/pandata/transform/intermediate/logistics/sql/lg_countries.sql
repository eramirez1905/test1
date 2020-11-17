WITH zones_agg_default_delivery_area_settings AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    zones.id AS zone_id,
    ARRAY_AGG(
      STRUCT(
        STRUCT(
          default_delivery_area_settings.delivery_fee.amount AS amount_local,
          default_delivery_area_settings.delivery_fee.percentage
        ) AS delivery_fee,
        default_delivery_area_settings.delivery_time AS delivery_time_in_minutes,
        default_delivery_area_settings.municipality_tax,
        default_delivery_area_settings.municipality_tax_type,
        default_delivery_area_settings.tourist_tax,
        default_delivery_area_settings.tourist_tax_type,
        default_delivery_area_settings.status,
        default_delivery_area_settings.minimum_value AS minimum_value_local,
        default_delivery_area_settings.drive_time AS drive_time_in_minutes,
        default_delivery_area_settings.vehicle_profile,
        default_delivery_area_settings.cut_on_zone_border AS is_cut_on_zone_border,
        default_delivery_area_settings.priority,
        default_delivery_area_settings.filters
      )
    ) AS default_delivery_area_settings
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.zones) AS zones
  CROSS JOIN UNNEST (zones.default_delivery_area_settings) AS default_delivery_area_settings
  GROUP BY
    country_code,
    city_id,
    zone_id
),

zones_agg_starting_points AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    zones.id AS zone_id,
    ARRAY_AGG(
      STRUCT(
        starting_points.id,
        starting_points.name,
        starting_points.is_active,
        starting_points.demand_distribution,
        starting_points.created_at AS created_at_utc,
        starting_points.updated_at AS updated_at_utc,
        STRUCT(
          starting_points.starting_point_shape.geojson,
          starting_points.starting_point_shape.wkt,
          starting_points.shape AS geo
        ) AS shape
      )
    ) AS starting_points
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.zones) AS zones
  CROSS JOIN UNNEST (zones.starting_points) AS starting_points
  GROUP BY
    country_code,
    city_id,
    zone_id
),

zones_agg_events AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    zones.id AS zone_id,
    ARRAY_AGG(
      STRUCT(
        events.id,
        events.action,
        events.activation_threshold,
        events.deactivation_threshold,
        events.title,
        events.value,
        events.message,
        events.vertical_type,
        events.shape AS shape_geo,
        events.is_halal,
        events.is_shape_in_sync,
        events.starts_at AS start_time_local,
        events.ends_at AS end_time_local,
        events.updated_at AS updated_at_utc,
        events.delivery_provider AS delivery_providers,
        events.cuisines,
        events.tags,
        events.characteristics
      )
    ) AS events
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.zones) AS zones
  CROSS JOIN UNNEST (zones.events) AS events
  GROUP BY
    country_code,
    city_id,
    zone_id
),

zones_agg_opening_times AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    zones.id AS zone_id,
    ARRAY_AGG(
      STRUCT(
        opening_times.weekday,
        opening_times.starts_at AS start_time_local,
        opening_times.ends_at AS end_time_local
      )
    ) AS opening_times
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.zones) AS zones
  CROSS JOIN UNNEST (zones.opening_times) AS opening_times
  GROUP BY
    country_code,
    city_id,
    zone_id
),

zones_agg_goal_delivery_time_history AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    zones.id AS zone_id,
    ARRAY_AGG(
      STRUCT(
        goal_delivery_time_history.goal_delivery_time AS goal_delivery_time_in_minutes,
        goal_delivery_time_history.updated_at AS updated_at_utc
      )
    ) AS goal_delivery_time_history
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.zones) AS zones
  CROSS JOIN UNNEST (zones.goal_delivery_time_history) AS goal_delivery_time_history
  GROUP BY
    country_code,
    city_id,
    zone_id
),

cities_agg_goal_delivery_time_history AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    ARRAY_AGG(
      STRUCT(
        goal_delivery_time_history.goal_delivery_time AS goal_delivery_time_in_minutes,
        goal_delivery_time_history.updated_at AS updated_at_utc
      )
    ) AS goal_delivery_time_history
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.goal_delivery_time_history) AS goal_delivery_time_history
  GROUP BY
    country_code,
    city_id
),

cities_agg_zones AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS city_id,
    ARRAY_AGG(
      STRUCT(
        zones.id,
        zones.name,
        zones.geo_id,
        zones.fleet_id,
        zones.is_active,
        zones.is_embedded,
        zones.has_default_delivery_area_settings,
        zones.delivery_types,
        zones.vehicle_types,
        zones.distance_cap AS distance_cap_in_meters,
        zones.created_at AS created_at_utc,
        zones.updated_at AS updated_at_utc,
        zones.area AS area_in_km_square,
        zones.boundaries,
        zones.embedding_zone_ids,
        STRUCT(
          zone_shape.geojson,
          zone_shape.wkt,
          zones.shape AS geo,
          zones.zone_shape_updated_at AS updated_at_utc
        ) AS shape,
        zones_agg_default_delivery_area_settings.default_delivery_area_settings,
        zones_agg_starting_points.starting_points,
        zones_agg_events.events,
        zones_agg_opening_times.opening_times,
        zones_agg_goal_delivery_time_history.goal_delivery_time_history
      )
    ) AS zones
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  CROSS JOIN UNNEST (cities.zones) AS zones
  LEFT JOIN zones_agg_default_delivery_area_settings
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = zones_agg_default_delivery_area_settings.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = zones_agg_default_delivery_area_settings.city_id
        AND zones.id = zones_agg_default_delivery_area_settings.zone_id
  LEFT JOIN zones_agg_starting_points
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = zones_agg_starting_points.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = zones_agg_starting_points.city_id
        AND zones.id = zones_agg_starting_points.zone_id
  LEFT JOIN zones_agg_events
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = zones_agg_events.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = zones_agg_events.city_id
        AND zones.id = zones_agg_events.zone_id
  LEFT JOIN zones_agg_opening_times
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = zones_agg_opening_times.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = zones_agg_opening_times.city_id
        AND zones.id = zones_agg_opening_times.zone_id
  LEFT JOIN zones_agg_goal_delivery_time_history
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = zones_agg_goal_delivery_time_history.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = zones_agg_goal_delivery_time_history.city_id
        AND zones.id = zones_agg_goal_delivery_time_history.zone_id
  GROUP BY
    country_code,
    city_id
),

countries_agg_cities AS (
  SELECT
    `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS country_code,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) AS id,
        cities.name,
        cities.is_active,
        cities.order_value_limit AS order_value_limit_local,
        cities.timezone,
        cities.created_at AS created_at_utc,
        cities.updated_at AS updated_at_utc,
        cities_agg_zones.zones,
        cities_agg_goal_delivery_time_history.goal_delivery_time_history
      )
    ) AS cities
  FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
  CROSS JOIN UNNEST (countries.cities) AS cities
  LEFT JOIN cities_agg_zones
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = cities_agg_zones.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = cities_agg_zones.city_id
  LEFT JOIN cities_agg_goal_delivery_time_history
         ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = cities_agg_goal_delivery_time_history.country_code
        AND `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(countries.country_code, cities.id) = cities_agg_goal_delivery_time_history.city_id
  GROUP BY country_code
)

SELECT
  countries.region,
  countries.region_short_name,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) AS code,
  countries.country_iso AS iso,
  countries.country_name AS name,
  countries.currency_code,
  countries.platforms,
  countries_agg_cities.cities,
FROM `fulfillment-dwh-production.curated_data_shared.countries` AS countries
LEFT JOIN countries_agg_cities
       ON `{project_id}`.pandata_intermediate.EXCLUDE_T2(countries.country_code) = countries_agg_cities.country_code
WHERE countries.country_code != 't2'
