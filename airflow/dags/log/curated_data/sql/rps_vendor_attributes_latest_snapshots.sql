SELECT CAST('{{ next_ds }}' AS DATE) AS captured_date
  , CAST('{{ execution_date }}' AS TIMESTAMP) AS captured_at
  , entity_id
  , vendor_code
  , STRUCT(
      created_date
      , vertical_type
      , vendor_grade
      , is_new_vendor
      , is_dormant_vendor
      , is_inoperative_vendor
      , is_unengaged_vendor
      , total_orders
      , successful_orders
      , is_latest_record
      , is_vm_uo_offlining
      , is_vm_uo_offlining_disabled_reason
      , is_vm_uo_autocall
      , is_vm_uo_autocall_disabled_reason
      , updated_date
      , updated_date_local
    ) AS captured_content
FROM `{{ params.project_id }}.rl.rps_vendor_attributes_latest`
