# Salesforce Cases

|Column | Type | Description|
| :--- | :--- | :--- | 
|source_id | INTEGER | Unique identifier of the entity (country + brand)|
|global_entity_id | STRING | N/A|
|management_entity | STRING | N/A|
|case_id | STRING | Unique identifier of the case in Salesforce|
|case_number | INTEGER | Unique identifier of the case in Salesforce Frontend|
|brand | STRING | Brand|
|contact_center | STRING | Contact Center|
|region | STRING | Region|
|channel | STRING | Channel through which the case originated|
|stakeholder | STRING | Stakeholder from which the case originated|
|direction | STRING | Inound/ Outbound|
|country | STRING | Country|
|language | STRING | Language|
|department | STRING | department (Customer, Partner or Rider Service)|
|team | STRING | team (Email, Calls...)|
|ticket_created_at | TIMESTAMP | Timestamp on which the case was created in Salesforce|
|ticket_solved_at | TIMESTAMP | Timestamp on which the case was resolved in Salesforce|
|ticket_full_resolution_time_min | INTEGER | Total time between creation and resolution of the case|
|after_chat_time_sec | INTEGER | For Chat Cases, time spent wrapping up the chat case after the chat was closed|
|chat_active_time_sec | INTEGER | N/A|
|owner_id | STRING | Last agent assigned to the case|
|ccr1 | STRING | Customer/Partner Contact Reason Level 1|
|ccr2 | STRING | Customer/Partner Contact Reason Level 2|
|ccr3 | STRING | Customer/Partner Contact Reason Level 3|
|refund_type | STRING | Refund Type|
|refund_amount | FLOAT | Refund Amount (local currency)|
|handling_time | FLOAT | Handlng Time of the case|
|incoming_cases | INTEGER | Number of incoming cases|
|reopen_cases | INTEGER | Number of Reopened cases|
|resolved_cases | INTEGER | Number of Resolved Cases|
|customer_email | STRING | Email of the stakeholder from which the case originated|
|order_code | STRING | Related order code or order id in case the case is order-related|
|alan_identifier | STRING | Related unique Help Center / Alan identifier in case this case originated from the helpcenter|
|working_item_l1_escalated | INTEGER | Number of Escalated Level 1 Work Items in that case|
|working_item_l1_close | INTEGER | Number of Closed Level 1 Work Items in that case|
|working_item_l2 | INTEGER | Number of Level 2 Work Items in that case|
|handling_time_l1_escalated | FLOAT | Combined handle time of the Level 1 Escalated Work items of that case|
|handling_time_l1_close | FLOAT | Combined handle time of the Level 1 Closed Work items of that case|
|handling_time_l2 | FLOAT | Combined handle time of the Level 2 Work items of that case|
|escalated_to | STRING | Department (record_type_name) to which the Case was escalated|
|parent_id | STRING | Case id of the Parent case|
|ticket_first_reply_time_l1_escalated | FLOAT | First Reply TIme of the first Level 1 Escalated Work Item of that case|
|ticket_first_reply_time_l1_close | FLOAT | First Reply TIme of the first Level 1 Closed Work Item of that case|
|ticket_first_reply_time_l2 | FLOAT | First Reply TIme of the first Level 2 Work Item of that case|
|count_within_sla_l1_escalated | INTEGER | 1 if the First Reply TIme of the first Level 1 Escalated Work Item of that case was less than 24hrs|
|count_within_sla_l1_close | INTEGER | 1 if the First Reply TIme of the first Level 1 Closed Work Item of that case was less than 24hrs|
|count_sla_l1_escalated | INTEGER | Case included in the base for calculation of SLA L1 Escalated|
|count_sla_l1_close | INTEGER | Case included in the base for calculation of SLA L1 Closed|
|count_within_sla_l2 | INTEGER | 1 if the First Reply TIme of the first Level 2 Work Item of that case was less than 24hrs|
|count_sla_l2 | INTEGER | Case included in the base for calculation of SLA L2|