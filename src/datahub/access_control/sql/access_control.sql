CREATE OR REPLACE VIEW `{{ params.project_id }}.{{ params.acl_dataset }}.access_control` AS
SELECT countries, entities
FROM `{{ params.project_id }}.{{ params.acl_dataset }}.users`
WHERE REGEXP_EXTRACT(SESSION_USER(), r'@(.+)') = email
  OR SESSION_USER() = email
ORDER BY weight DESC
LIMIT 1
