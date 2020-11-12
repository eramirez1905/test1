# Account Invoice

This table contains the account invoice level data

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER` | [NULL] |
| access_token | `STRING` | Security Token |
| message_main_attachment_id | `INTEGER` | Main Attachment |
| name | `STRING` | Reference/Description |
| origin | `STRING` | Source Document |
| type | `STRING` | Type |
| refund_invoice_id | `INTEGER` | Invoice for which this invoice is the credit note |
| number | `STRING` | Number |
| move_name | `STRING` | Journal Entry Name |
| reference | `STRING` | Payment Ref. |
| comment | `STRING` | Additional Information |
| state | `STRING` | Status |
| sent | `BOOL` | Sent |
| date_invoice | `DATE` | Invoice Date |
| date_due | `DATE` | Due Date |
| partner_id | `INTEGER` | Partner |
| vendor_bill_id | `INTEGER` | Vendor Bill |
| payment_term_id | `INTEGER` | Payment Terms |
| date | `DATE` | Accounting Date |
| account_id | `INTEGER` | Account |
| move_id | `INTEGER` | Journal Entry |
| amount_untaxed | `NUMERIC` | Untaxed Amount |
| amount_untaxed_signed | `NUMERIC` | Untaxed Amount in Company Currency |
| amount_tax | `NUMERIC` | Tax |
| amount_total | `NUMERIC` | Total |
| amount_total_signed | `NUMERIC` | Total in Invoice Currency |
| amount_total_company_signed | `NUMERIC` | Total in Company Currency |
| currency_id | `INTEGER` | Currency |
| journal_id | `INTEGER` | Journal |
| company_id | `INTEGER` | Company |
| reconciled | `BOOL` | Paid/Reconciled |
| partner_bank_id | `INTEGER` | Bank Account |
| residual | `NUMERIC` | Amount Due |
| residual_signed | `NUMERIC` | Amount Due in Invoice Currency |
| residual_company_signed | `NUMERIC` | Amount Due in Company Currency |
| user_id | `INTEGER` | Salesperson |
| fiscal_position_id | `INTEGER` | Fiscal Position |
| commercial_partner_id | `INTEGER` | Commercial Entity |
| cash_rounding_id | `INTEGER` | Cash Rounding Method |
| incoterm_id | `INTEGER` | Incoterm |
| source_email | `STRING` | Source Email |
| vendor_display_name | `STRING` | Vendor Display Name |
| create_uid | `INTEGER` | Created by |
| create_date | `TIMESTAMP` | Created on |
| write_uid | `INTEGER` | Last Updated by |
| write_date | `TIMESTAMP` | Last Updated on |
| purchase_id | `INTEGER` | Add Purchase Order |
| vendor_bill_purchase_id | `INTEGER` | Auto-Complete |
| team_id | `INTEGER` | Sales Team |
| partner_shipping_id | `INTEGER` | Delivery Address |
| incoterms_id | `INTEGER` | Incoterms |
