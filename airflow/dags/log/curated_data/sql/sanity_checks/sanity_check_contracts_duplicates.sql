WITH riders_contracts AS (
  SELECT country_code
    , rider_id
    , co.id
    , co.created_at
    , count(*) AS count
  FROM `{{ params.project_id }}.cl.riders`
  LEFT JOIN UNNEST(contracts) co
  WHERE co.status = 'VALID'
    AND '{{ next_ds }}' BETWEEN CAST(co.start_at AS DATE) AND CAST(co.end_at AS DATE)
  GROUP BY 1,2,3,4
)
SELECT COUNTIF(count != 1) = 0 AS duplicates_count
FROM riders_contracts
