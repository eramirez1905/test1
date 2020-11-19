-- create partitioned legacy event table from imported data
CREATE OR REPLACE TABLE forecasting.events_pandora_legacy
PARTITION BY created_date
CLUSTER BY country_code AS
SELECT
	country_code,
	event_type,
	CAST(event_id AS STRING) AS event_id,
	TIMESTAMP_TRUNC(starts_at, SECOND) AS starts_at,
	TIMESTAMP_TRUNC(ends_at, SECOND) AS ends_at,
	value,
	geo_object_type,
	geo_object_id,
	SAFE.ST_GEOGFROMTEXT(geom) AS geom,
	DATE(starts_at) AS created_date
FROM `dh-logistics-data-science.imports.forecasting_events_pandora_legacy_json`
ORDER BY 1,2,3
