WITH subscriptions_agg_payments AS (
  SELECT
    subscription_payments.subscription_uuid,
    ARRAY_AGG(
      STRUCT(
        subscription_payments.uuid,
        subscription_payments.id,
        subscription_payments.gateway_reference,
        subscription_payments.http_status,
        subscription_payments.internal_reference,
        subscription_payments.total_amount_local,
        subscription_payments.vat_amount_local,
        subscription_payments.net_amount_local,
        subscription_payments.status,
        subscription_payments.is_status_success,
        subscription_payments.is_status_failed,
        subscription_payments.is_status_pending,
        TIMESTAMP(DATETIME(subscription_payments.paid_at_utc, countries.timezone)) AS paid_at_local,
        subscription_payments.paid_at_utc,
        TIMESTAMP(DATETIME(subscription_payments.subscription_start_at_utc, countries.timezone)) AS subscription_start_at_local,
        subscription_payments.subscription_start_at_utc,
        TIMESTAMP(DATETIME(subscription_payments.subscription_end_at_utc, countries.timezone)) AS subscription_end_at_local,
        subscription_payments.subscription_end_at_utc,
        TIMESTAMP(DATETIME(subscription_payments.created_at_utc, countries.timezone)) AS created_at_local,
        subscription_payments.created_at_utc,
        TIMESTAMP(DATETIME(subscription_payments.updated_at_utc, countries.timezone)) AS updated_at_local,
        subscription_payments.updated_at_utc,
        subscription_payments.dwh_last_modified_at_utc
      )
    ) AS payments
  FROM `{project_id}.pandata_intermediate.sb_subscription_payments` AS subscription_payments
  LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
         ON countries.entity_id = subscription_payments.global_entity_id
  GROUP BY subscription_payments.subscription_uuid
)

SELECT
  subscriptions.uuid,
  countries.rdbms_id,
  subscriptions.id,
  subscriptions.global_entity_id,
  customers.uuid AS customer_uuid,
  customers.id AS customer_id,
  subscriptions.code,
  subscriptions.plan_code,
  subscriptions.customer_code,
  subscriptions.status,
  subscriptions.payment_token,
  TIMESTAMP(DATETIME(subscriptions.started_at_utc, countries.timezone)) AS started_at_local,
  TIMESTAMP(DATETIME(subscriptions.ended_at_utc, countries.timezone)) AS ended_at_local,
  TIMESTAMP(DATETIME(subscriptions.last_renewal_at_utc, countries.timezone)) AS last_renewal_at_local,
  TIMESTAMP(DATETIME(subscriptions.next_renewal_at_utc, countries.timezone)) AS next_renewal_at_local,
  TIMESTAMP(DATETIME(subscriptions.created_at_utc, countries.timezone)) AS created_at_local,
  TIMESTAMP(DATETIME(subscriptions.updated_at_utc, countries.timezone)) AS updated_at_local,
  DATE(subscriptions.created_at_utc, countries.timezone) AS created_date_local,
  subscriptions.started_at_utc,
  subscriptions.ended_at_utc,
  subscriptions.last_renewal_at_utc,
  subscriptions.next_renewal_at_utc,
  subscriptions.created_at_utc,
  subscriptions.updated_at_utc,
  DATE(subscriptions.created_at_utc) AS created_date_utc,
  subscriptions.dwh_last_modified_utc,
  subscriptions_agg_payments.payments,
FROM `{project_id}.pandata_intermediate.sb_subscriptions` AS subscriptions
LEFT JOIN subscriptions_agg_payments
       ON subscriptions.uuid = subscriptions_agg_payments.subscription_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON countries.entity_id = subscriptions.global_entity_id
LEFT JOIN `{project_id}.pandata_intermediate.pd_customers` AS customers
       ON countries.rdbms_id = customers.rdbms_id
      AND subscriptions.customer_code = customers.code
