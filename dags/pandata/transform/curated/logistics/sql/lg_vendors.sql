WITH lg_entities AS (
  SELECT
    platforms.entity_id AS id,
    lg_countries.code AS country_code,
    lg_countries.iso,
  FROM `{project_id}.pandata_intermediate.lg_countries` AS lg_countries
  CROSS JOIN UNNEST (lg_countries.platforms) AS platforms
)
SELECT
  lg_vendors.uuid,
  lg_vendors.lg_entity_id,
  lg_vendors.code,
  lg_entities.country_code,
  pd_countries.rdbms_id,
  pd_vendors.id AS pd_vendor_id,

  lg_vendors.name,
  lg_vendors.vehicle_profile,
  lg_vendors.location_geo,
  lg_vendors.last_provided_location_geo,

  lg_vendors.location_history_geos,
  lg_vendors.hurrier,
  lg_vendors.rps,
  lg_vendors.delivery_areas,
  lg_vendors.delivery_area_locations,
  lg_vendors.porygon,
  lg_vendors.dps,
  lg_vendors.time_buckets,
FROM `{project_id}.pandata_intermediate.lg_vendors` AS lg_vendors
LEFT JOIN lg_entities
       ON lg_vendors.lg_entity_id = lg_entities.id
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS pd_countries
       ON lg_entities.iso = pd_countries.iso
LEFT JOIN `{project_id}.pandata_intermediate.pd_vendors` AS pd_vendors
       ON pd_countries.rdbms_id = pd_vendors.rdbms_id
      AND lg_vendors.code = pd_vendors.code
