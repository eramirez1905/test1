# Purchase Order

This table contains the Purchase Order level data

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER` | [NULL] |
| access_token | `STRING` | Security Token |
| message_main_attachment_id | `INTEGER` | Main Attachment |
| name | `STRING` | Order Reference |
| origin | `STRING` | Source Document |
| partner_ref | `STRING` | Vendor Reference |
| date_order | `TIMESTAMP` | Order Date |
| date_approve | `DATE` | Approval Date |
| partner_id | `INTEGER` | Vendor |
| dest_address_id | `INTEGER` | Drop Ship Address |
| currency_id | `INTEGER` | Currency |
| state | `STRING` | Status |
| notes | `STRING` | Terms and Conditions |
| invoice_count | `INTEGER` | Bill Count |
| invoice_status | `STRING` | Billing Status |
| date_planned | `TIMESTAMP` | Scheduled Date |
| amount_untaxed | `NUMERIC` | Untaxed Amount |
| amount_tax | `NUMERIC` | Taxes |
| amount_total | `NUMERIC` | Total |
| fiscal_position_id | `INTEGER` | Fiscal Position |
| payment_term_id | `INTEGER` | Payment Terms |
| incoterm_id | `INTEGER` | Incoterm |
| user_id | `INTEGER` | Purchase Representative |
| company_id | `INTEGER` | Company |
| create_uid | `INTEGER` | Created by |
| create_date | `TIMESTAMP` | Created on |
| write_uid | `INTEGER` | Last Updated by |
| write_date | `TIMESTAMP` | Last Updated on |
| picking_count | `INTEGER` | Picking count |
| picking_type_id | `INTEGER` | Deliver To |
| group_id | `INTEGER` | Procurement Group |
| owner_id | `INTEGER` | Owner |
| return_state | `STRING` | Return State |
| received_final_amount | `NUMERIC` | Received Final Amount |
| returned_final_amount | `NUMERIC` | Returned Final Amount |
| returned_final_subtotal | `NUMERIC` | Returned Final Amount Untaxed |
| received_final_subtotal | `NUMERIC` | Received Final Amount Untaxed |
| received_final_tax_amount | `NUMERIC` | Tax |
| returned_final_tax_amount | `NUMERIC` | Tax |
| uuid | `STRING` | Purchase order UUID |
| publish_on_confirm | `BOOL` | This PO's confirmation was published to PubSub |
