WITH foodpanda_countries AS (
  SELECT
    fp_countries.dwh_country_id AS id,
    fp_countries.rdbms_id,
    fp_countries.dwh_company_id AS company_id,
    fp_countries.dwh_source_code AS entity_id,
    fp_countries.backend_url,
    fp_countries.common_name,
    fp_countries.company_name,
    fp_countries.country_iso,
    fp_countries.currency_code,
    fp_countries.domain,
    SAFE_CAST(fp_countries.live AS BOOLEAN) AS is_live,
    fp_countries.region,
    fp_countries.three_letter_country_iso_code,
    fp_countries.timezone,
    SAFE_CAST(fp_countries.vat AS NUMERIC) AS vat,
    fp_countries.venture_url
  FROM `{project_id}.pandata_raw_il_backend_latest.v_dim_countries` AS fp_countries
  WHERE LOWER(fp_countries.company_name) = "foodpanda"
    AND fp_countries.dwh_source_code != "UH_TH2"
),

logistics_countries AS (
  SELECT
    lg_countries.rdbms_id AS lg_rdbms_id,
    lg_countries.country_iso,
  FROM `{project_id}.pandata_raw_il_backend_latest.v_dim_countries` AS lg_countries
  WHERE LOWER(lg_countries.company_name) = "usehurrier"
    AND lg_countries.dwh_source_code != "UH_TH2"
)

SELECT
  foodpanda_countries.id,
  foodpanda_countries.rdbms_id,
  logistics_countries.lg_rdbms_id,
  LOWER(foodpanda_countries.country_iso) AS lg_country_code,
  foodpanda_countries.company_id,
  foodpanda_countries.entity_id,
  foodpanda_countries.backend_url,
  foodpanda_countries.common_name,
  foodpanda_countries.company_name,
  foodpanda_countries.country_iso AS iso,
  foodpanda_countries.currency_code,
  foodpanda_countries.domain,
  foodpanda_countries.is_live,
  foodpanda_countries.region,
  foodpanda_countries.three_letter_country_iso_code AS three_letter_iso_code,
  foodpanda_countries.timezone,
  foodpanda_countries.vat,
  foodpanda_countries.venture_url
FROM foodpanda_countries
LEFT JOIN logistics_countries
       ON foodpanda_countries.country_iso = logistics_countries.country_iso
-- We ignore this entity and just consider everything `UH_TH` instead
-- to avoid duplicated entries unecessarily
