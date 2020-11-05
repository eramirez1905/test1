CREATE OR REPLACE TABLE rl.scorecard_report_zones AS
WITH zone_weighted_scores AS (
  SELECT country_code
    , city_id
    , zone_id
    , report_week_local
    , segment
    , variable
    , SUM((weight * 10) * score) AS weighted_kpi_score
    , SUM(weight * 10) AS sum_weights
  FROM (
    SELECT *
    FROM rl.scorecard_final_zones_table
  )
  GROUP BY 1, 2, 3, 4, 5, 6
), zone_segment_scores AS (
  SELECT country_code
    , city_id
    , zone_id
    , report_week_local
    , segment
    , ROUND(SAFE_DIVIDE(SUM(weighted_kpi_score), SUM(sum_weights)), 1) AS zone_segment_score
  FROM zone_weighted_scores
  GROUP BY 1, 2, 3, 4, 5
), entities AS (
  SELECT co.country_code
    -- add a concatenation of all the platform in each country for visualization purposes.
    , ARRAY_TO_STRING(ARRAY_AGG(p.display_name IGNORE NULLS), ' / ') AS entities
  FROM cl.countries co
  LEFT JOIN UNNEST (co.platforms) p
  -- remove legacy display name from report, as there is no data under it since 2017, however it may cause confusion to the user.
  WHERE p.display_name NOT IN ('FD - Bahrain')
  GROUP BY  1
)
SELECT ft.country_code
  , co.country_name
  , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, en.entities) AS entity_display_name
  , co.region
  , ft.city_id
  , ci.name AS city_name
  , ft.zone_id
  , z.name AS zone_name
  , CASE
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', '{{ next_ds }}')
        THEN 'current_week'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK))
        THEN '1_week_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 2 WEEK))
        THEN '2_weeks_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 3 WEEK))
        THEN '3_weeks_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK))
        THEN '4_weeks_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK))
        THEN '5_weeks_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK))
        THEN '6_weeks_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 7 WEEK))
        THEN '7_weeks_ago'
      WHEN ft.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK))
        THEN '8_weeks_ago'
      ELSE ft.report_week_local
    END AS week_relative
  , ft.report_week_local
  , ft.segment
  , ft.variable AS kpi
  , ft.score AS zone_kpi_score
  , zis.zone_segment_score
FROM rl.scorecard_final_zones_table ft
LEFT JOIN zone_segment_scores zis USING(country_code, city_id, zone_id, report_week_local, segment)
LEFT JOIN cl.countries co ON ft.country_code = co.country_code
LEFT JOIN UNNEST(co.cities) ci ON ft.city_id = ci.id
LEFT JOIN UNNEST(ci.zones) z ON ft.zone_id = z.id
LEFT JOIN entities en ON ft.country_code = en.country_code
;
