WITH companies_agg_departments AS (
  SELECT
    company_uuid,
    ARRAY_AGG(
      STRUCT(
        name,
        state,
        is_state_active,
        is_state_deleted,
        created_at_utc,
        updated_at_utc
      )
    ) AS departments,
  FROM `{project_id}.pandata_intermediate.cp_company_departments`
  GROUP BY company_uuid
)

SELECT
  companies.uuid,
  companies.id,
  companies.rdbms_id,
  companies.city,
  companies.name,
  companies.industry,
  companies.postal_code,
  companies.purpose,
  companies.setting,
  companies.state,
  companies.street,
  companies.is_state_active,
  companies.is_state_deleted,
  companies.is_state_new,
  companies.is_state_suspended,
  companies.is_allowance_enabled,
  companies.is_requested_demo,
  companies.is_self_signup,
  companies.is_account_linking_required,
  companies.created_at_utc,
  companies.updated_at_utc,
  companies.dwh_last_modified_utc,
  companies_agg_departments.departments,
FROM `{project_id}.pandata_intermediate.cp_companies` AS companies
LEFT JOIN companies_agg_departments
       ON companies.uuid = companies_agg_departments.company_uuid
