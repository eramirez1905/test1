# Purchase Order Line

This table contains the Purchase Order Line level data

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER` | [NULL] |
| name | `STRING` | Description |
| sequence | `INTEGER` | Sequence |
| product_qty | `NUMERIC` | Quantity |
| product_uom_qty | `undefined` | Total Quantity |
| date_planned | `TIMESTAMP` | Scheduled Date |
| product_uom | `INTEGER` | Product Unit of Measure |
| product_id | `INTEGER` | Product |
| price_unit | `NUMERIC` | Unit Price |
| price_subtotal | `NUMERIC` | Subtotal |
| price_total | `NUMERIC` | Total |
| price_tax | `undefined` | Tax |
| order_id | `INTEGER` | Order Reference |
| account_analytic_id | `INTEGER` | Analytic Account |
| company_id | `INTEGER` | Company |
| state | `STRING` | Status |
| qty_invoiced | `NUMERIC` | Billed Qty |
| qty_received | `NUMERIC` | Received Qty |
| partner_id | `INTEGER` | Partner |
| currency_id | `INTEGER` | Currency |
| create_uid | `INTEGER` | Created by |
| create_date | `TIMESTAMP` | Created on |
| write_uid | `INTEGER` | Last Updated by |
| write_date | `TIMESTAMP` | Last Updated on |
| orderpoint_id | `INTEGER` | Orderpoint |
| sale_order_id | `INTEGER` | Sale Order |
| sale_line_id | `INTEGER` | Origin Sale Item |
| discount | `NUMERIC` | Discount (%) |
| price_supplier | `NUMERIC` | Unit Price |
| product_cost | `NUMERIC` | Cost price snapshot |
| return_state | `STRING` | Return State |
| qty_returned | `NUMERIC` | Returned Qty |
| received_final_amount | `NUMERIC` | Received Final Amount |
| returned_final_amount | `NUMERIC` | Returned Final Amount |
| received_final_subtotal | `NUMERIC` | Received Final Amount Before Tax |
| returned_final_subtotal | `NUMERIC` | Returned Final Amount Before Tax |
| received_final_tax_amount | `NUMERIC` | Received Tax Amount |
| returned_final_tax_amount | `NUMERIC` | Returned Tax Amount |
