CREATE OR REPLACE TABLE il.issues
PARTITION BY created_date AS
  SELECT i.country_code
    , ci.id AS city_id
    , i.id AS issue_id
    , o.id AS order_id
    , i.delivery_id AS delivery_id
    , i.courier_id
    , c.user_id AS rider_id
    , o.confirmation_number AS order_code
    , CASE
      WHEN o.platform IN ('MJAM', 'ONLINE_PIZZA_SE')
        THEN NULL
      ELSE o.unn_order_id
      END AS legacy_order_id
    , i.type AS issue_type
    , i.category AS issue_category
    , i.notes AS issue_notes
    , i.created_at AS issue_created_at
    , i.dismissed_at AS issue_dismissed_at
    , i.resolved_at AS issue_resolved_at
    , i.updated_at  AS issue_updated_at
    , i.created_date
    , ci.timezone
  FROM `{{ params.project_id }}.ml.hurrier_issues` i
  JOIN `{{ params.project_id }}.ml.hurrier_deliveries` d ON d.id = i.delivery_id
    AND d.country_code = i.country_code
  JOIN `{{ params.project_id }}.ml.hurrier_addresses` a ON a.id = COALESCE(d.dropoff_address_id, d.pickup_address_id)
    AND a.country_code = d.country_code
  JOIN `{{ params.project_id }}.ml.hurrier_cities` ci ON ci.id = a.city_id
    AND ci.country_code = a.country_code
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_orders` o ON d.order_id = o.id
    AND d.country_code = o.country_code
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_couriers` c ON d.courier_id = c.id
    AND d.country_code = c.country_code
  WHERE type NOT IN ('CourierIssue')

  UNION ALL

  SELECT i.country_code
    , ci.id AS city_id
    , i.id AS issue_id
    , NULL AS disp_order_id
    , NULL AS delivery_id
    , i.courier_id
    , c.user_id AS rider_id
    , NULL AS order_code
    , NULL AS order_id
    , i.type AS issue_type
    , i.category AS issue_category
    , i.notes AS issue_notes
    , i.created_at AS issue_created_at
    , i.dismissed_at AS issue_dismissed_at
    , i.resolved_at AS issue_resolved_at
    , i.updated_at AS issue_updated_at
    , i.created_date
    , ci.timezone
  FROM `{{ params.project_id }}.ml.hurrier_issues` i
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_couriers` c ON c.country_code = i.country_code
    AND c.id = i.courier_id
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_zones` z ON c.country_code = z.country_code
    AND z.id = c.zone_id
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_cities` ci ON z.city_id = ci.id
    AND z.country_code = ci.country_code
  WHERE type IN ('CourierIssue')
;
