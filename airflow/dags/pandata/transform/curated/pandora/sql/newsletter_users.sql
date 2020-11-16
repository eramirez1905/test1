SELECT
  uuid,
  id,
  rdbms_id,
  customer_id,
  mailing_id,
  area_id,
  city_id,
  created_by_user_id,
  updated_by_user_id,
  email,
  first_name,
  last_name,
  language,
  mailing_name,
  is_active,
  subscribed_at_local,
  unsubscribed_at_local,
  dwh_last_modified_at_utc,
FROM `{project_id}.pandata_intermediate.pd_newsletter_users`