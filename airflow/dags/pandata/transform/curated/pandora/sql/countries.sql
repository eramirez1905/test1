-- A self join is required because area_id points to the parent area so it's natural for
-- this table to be nested rather than flattened to preserve the structure

-- Any child area is excluded from the parent areas to ensure that any area
-- either appeared in the parent area or child area so there's no duplicate,
WITH child_areas AS (
  SELECT
    area_uuid AS parent_area_uuid,
    ARRAY_AGG(
      STRUCT(
        uuid,
        id,
        name,
        is_active,
        latitude,
        longitude,
        postcode,
        zoom,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS areas
  FROM `{project_id}.pandata_intermediate.pd_areas`
  WHERE area_uuid IS NOT NULL  -- has parent area
  GROUP BY area_uuid
),

areas AS (
  SELECT
    areas.uuid,
    areas.id,
    areas.rdbms_id,
    areas.city_id,
    areas.name,
    areas.is_active,
    areas.latitude,
    areas.longitude,
    areas.postcode,
    areas.zoom,
    child_areas.areas AS subareas,
    areas.created_at_utc,
    areas.updated_at_utc,
    areas.dwh_last_modified_at_utc
  FROM `{project_id}.pandata_intermediate.pd_areas` AS areas
  LEFT JOIN child_areas
         ON areas.uuid = child_areas.parent_area_uuid
  WHERE areas.area_uuid IS NULL  -- has no parent area
),

countries_agg_areas AS (
  SELECT
    countries.id,
    ARRAY_AGG(
      STRUCT(
        areas.id,
        areas.city_id,
        areas.name,
        areas.is_active,
        areas.latitude,
        areas.longitude,
        areas.postcode,
        areas.zoom,
        areas.subareas,
        areas.created_at_utc,
        areas.updated_at_utc,
        areas.dwh_last_modified_at_utc
      )
    ) AS areas
  FROM `{project_id}.pandata_intermediate.pd_countries` AS countries
  LEFT JOIN areas
         ON countries.rdbms_id = areas.rdbms_id
  GROUP BY countries.id
),

countries_agg_cities AS (
  SELECT
    countries.id,
    ARRAY_AGG(
      STRUCT(
        cities.uuid,
        cities.id,
        cities.name,
        cities.is_active,
        cities.is_deleted,
        cities.timezone,
        option_values.vat_rate,
        cities.created_at_utc,
        cities.updated_at_utc,
        cities.dwh_last_modified_at_utc
      )
    ) AS cities
  FROM `{project_id}.pandata_intermediate.pd_countries` AS countries
  LEFT JOIN `{project_id}.pandata_intermediate.pd_cities` AS cities
         ON cities.rdbms_id = countries.rdbms_id
  LEFT JOIN `{project_id}.pandata_intermediate.pd_option_values` AS option_values
         ON cities.option_value_vat_uuid = option_values.uuid
  GROUP BY countries.id
)


SELECT
  countries.id,
  countries.rdbms_id,
  countries.lg_rdbms_id,
  countries.company_id,
  countries.entity_id,
  countries.backend_url,
  countries.common_name,
  countries.company_name,
  countries.iso,
  countries.currency_code,
  countries.domain,
  countries.is_live,
  countries.region,
  countries.three_letter_iso_code,
  countries.timezone,
  countries.vat,
  countries.venture_url,
  countries_agg_cities.cities,
  countries_agg_areas.areas
FROM `{project_id}.pandata_intermediate.pd_countries` AS countries
LEFT JOIN countries_agg_cities
       ON countries.id = countries_agg_cities.id
LEFT JOIN countries_agg_areas
       ON countries.id = countries_agg_areas.id
