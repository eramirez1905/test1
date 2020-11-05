WITH clients_agg_contacts AS (
  SELECT
    client_uuid,
    ARRAY_AGG(
      STRUCT(
        uuid,
        id,
        is_email_notifiable,
        is_sms_notifiable,
        name,
        email,
        phone_number,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_utc
      )
    ) AS contacts
  FROM `{project_id}.pandata_intermediate.bl_contacts`
  GROUP BY client_uuid
)

SELECT
  clients.uuid,
  clients.id,
  clients.rdbms_id,
  clients.client_code,
  clients.is_active,
  clients.is_automatically_invoiceable,
  clients.is_setup_confirmed,
  clients.bank_account_name,
  clients.bank_account_number,
  clients.bank_branch_name,
  clients.bank_information,
  clients.bank_name,
  clients.billing_address,
  clients.cheque_in_favour_of,
  clients.cheque_in_favour_of_bank_account_number,
  clients.cheque_in_favour_of_bank_name,
  clients.cheque_in_favour_of_mailing_address,
  clients.invoice_language,
  clients.name,
  clients.invoice_method_type,
  clients.payment_mode_type,
  clients.created_at_utc,
  clients.updated_at_utc,
  clients.dwh_last_modified_utc,
  STRUCT(
    tax.title,
    tax.is_client_level,
    tax.percentage,
    tax.created_at_utc,
    tax.updated_at_utc
  ) AS tax
FROM `{project_id}.pandata_intermediate.bl_clients` AS clients
LEFT JOIN clients_agg_contacts
       ON clients.uuid = clients_agg_contacts.client_uuid
-- seems like a 1:1 but not 100% sure due to the structure
LEFT JOIN `{project_id}.pandata_intermediate.bl_client_tax` AS client_tax
       ON clients.uuid = client_tax.client_uuid
LEFT JOIN `{project_id}.pandata_intermediate.bl_tax` AS tax
       ON client_tax.tax_uuid = tax.uuid
