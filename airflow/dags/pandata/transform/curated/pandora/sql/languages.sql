WITH parent_languages_agg_child_languages AS (
  SELECT
    parent_languages.uuid AS language_uuid,
    ARRAY_AGG(
      STRUCT(
        child_languages.uuid,
        child_languages.id,
        child_languages.title,
        child_languages.code,
        child_languages.google_map_language,
        child_languages.locale,
        child_languages.is_active,
        child_languages.is_deleted,
        child_languages.is_text_direction_right_to_left,
        child_languages.created_at_utc,
        child_languages.updated_at_utc,
        child_languages.dwh_last_modified_at_utc
      )
    ) AS child_languages
  FROM `{project_id}.pandata_intermediate.pd_languages` AS parent_languages
  LEFT JOIN `{project_id}.pandata_intermediate.pd_languages` AS child_languages
         ON parent_languages.uuid = child_languages.parent_uuid
  WHERE parent_languages.parent_uuid IS NULL
  GROUP BY parent_languages.uuid
)

SELECT
  languages.uuid,
  languages.id,
  languages.rdbms_id,

  languages.title,
  languages.code,
  languages.google_map_language,
  languages.locale,
  languages.is_active,
  languages.is_deleted,
  languages.is_text_direction_right_to_left,
  languages.created_at_utc,
  languages.updated_at_utc,
  languages.dwh_last_modified_at_utc,
  parent_languages_agg_child_languages.child_languages,
FROM parent_languages_agg_child_languages
LEFT JOIN `{project_id}.pandata_intermediate.pd_languages` AS languages
       ON parent_languages_agg_child_languages.language_uuid = languages.uuid
