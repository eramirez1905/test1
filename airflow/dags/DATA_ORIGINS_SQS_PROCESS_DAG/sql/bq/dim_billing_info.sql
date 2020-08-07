DELETE FROM peyabi.testing.dim_billing_info 
WHERE billing_info_id IN (
	SELECT billing_info_id
	FROM s3.sqs_billing_info
);

INSERT INTO peyabi.testing.dim_billing_info (billing_info_id
       , commission
       , company_address
       , company_name
       , company_number
       , reference_name
       , type
       , address_id
       , email
       , country_id
       , date_created
       , last_updated
       , payment_mode
       , tax_category_id
       , tax_category_code
       , tax_category_name
       , sap_id
       , active
       , business_type
       , commercial_activity
       , commission_includes_delivery_cost
       , credit_card_commission_includes_delivery_cost
       , date
       , food_tax_id
       , generate_final_user_documents
       , is_debtor
       , mobile_white_label_commission
       , notes
       , online_payment
       , single_commission
       , single_payment
       , widget_commission
) 
(
	SELECT billing_info_id
	       , commission
	       , company_address
	       , company_name
	       , company_number
	       , reference_name
	       , type
	       , address_id
	       , email
	       , country_id
	       , date_created
	       , last_updated
	       , pm.id as payment_mode
	       , tax_category_id
	       , tax_category_code
	       , tax_category_name
	       , sap_id
	       , active
	       , business_type
	       , commercial_activity
	       , commission_includes_delivery_cost
	       , credit_card_commission_includes_delivery_cost
	       , date
	       , food_tax_id
	       , generate_final_user_documents
	       , is_debtor
	       , mobile_white_label_commission
	       , notes
	       , online_payment
	       , single_commission
	       , single_payment
	       , widget_commission
	FROM s3.sqs_billing_info x
	LEFT JOIN imports.bicoretech_payment_mode pm 
	ON pm.name = payment_mode
);