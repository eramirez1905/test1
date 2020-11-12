WITH customers_agg_newsletter_users AS (
  SELECT
    customer_uuid,
    LOGICAL_OR(is_active) AS has_at_least_one_active_user,
  FROM `{project_id}.pandata_intermediate.pd_newsletter_users`
  WHERE customer_uuid IS NOT NULL
  GROUP BY customer_uuid
),

customers_agg_social_logins AS (
  SELECT
    customer_uuid,
    ARRAY_AGG(
      STRUCT(
        uuid,
        id,
        platform,
        dwh_last_modified_at_utc
    )
  ) AS social_logins
  FROM `{project_id}.pandata_intermediate.pd_social_logins`
  GROUP BY customer_uuid
)

SELECT
  customers.uuid,
  customers.id,
  customers.rdbms_id,
  customers.created_by_user_uuid,
  customers.created_by_user_id,
  customers.updated_by_user_uuid,
  customers.updated_by_user_id,
  customers.loyalty_program_name_uuid,
  customers.loyalty_program_name_id,
  customers.code,
  customers.is_deleted,
  customers.is_guest,
  customers.is_mobile_verified,
  customers_agg_newsletter_users.has_at_least_one_active_user AS is_newsletter_user,
  customers.email,
  customers.first_name,
  customers.last_name,
  customers.internal_comment,
  customers.landline,
  customers.mobile,
  customers.mobile_confirmation_code,
  customers.mobile_country_code,
  customers.mobile_verified_attempts,
  customers.source,
  languages.title AS language_title,
  customers.last_user_agent,
  customers.last_login_at_local,
  customers.created_at_utc,
  customers.updated_at_utc,
  customers.dwh_last_modified_at_utc,
  customers_agg_social_logins.social_logins,
FROM `{project_id}.pandata_intermediate.pd_customers` AS customers
LEFT JOIN customers_agg_social_logins
       ON customers.uuid = customers_agg_social_logins.customer_uuid
LEFT JOIN customers_agg_newsletter_users
       ON customers.uuid = customers_agg_newsletter_users.customer_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_languages` AS languages
       ON customers.language_uuid = languages.uuid
