# Salesforce Work Items

|Column | Type | Description|
| :--- | :--- | :--- | 
|case_id | STRING | unique case_id|
|case_number | INTEGER | unique case number (visible in Salesforce frontend)|
|case_type | STRING | case_type|
|work_item_id | STRING | unique id of work item within a case (format: case_id::1, case_id::2)|
|owner_id | STRING | agent id in charge of the work item|
|brand | STRING | brand|
|channel | STRING | channel (chat, email..)|
|stakeholder | STRING | Customer, Partner...|
|direction | STRING | direction (inbound...)|
|country | STRING | country|
|language | STRING | N/A|
|contact_center | STRING | contact_center|
|region | STRING | region|
|global_entity_id | STRING | global standard id of the entity (e.g TA_AE)|
|management_entity | STRING | management_entity (e.g. foodpanda APAC)|
|department | STRING | department|
|team | STRING | team (e.g. Calls and Order Processing)|
|customer_email | STRING | customer_email|
|alan_identifier | STRING | related Helpcenter identifier (e.g. hc-FP_SG-xxx )|
|parent_id | STRING | case id of the parent case, only relevant for Level 2 cases (child case)|
|order_code | STRING | order_code|
|work_item_escalated | BOOLEAN | escalated work item of that case (1 if the work item is escalated)|
|work_item_created_at | TIMESTAMP | timestamp when work_item is created: field IN ('created') or new value of the status IN ('Reopen','New Answer',New')|
|work_item_assigned_at | TIMESTAMP | timestamp when work_item is assigned to an agent: field IN ('Owner')|
|work_item_resolved_at | TIMESTAMP | timestamp when work_item is resolved: new value of the status IN ('Resolved')|
|work_item_closed_at | TIMESTAMP | timestamp when work_item is closed: new value of the status IN ('closed')|
|count_within_sla_work_item | INTEGER | 1 if the work item is within sla (24h) based on the first reply time|
|count_within_sla_agent_work_item | INTEGER | N/A|
|ccr1 | STRING | Customer/Partner Contact Reason Level 1|
|ccr2 | STRING | Customer/Partner Contact Reason Level 2|
|ccr3 | STRING | Customer/Partner Contact Reason Level 3|
|refund_type | STRING | refund_type (card, cash...)|
|refund_amount | FLOAT | Refund Amount (local currency)|
|work_item_first_reply_time | FLOAT | time from start of the work item until it's resolved (in minutes)|
|work_item_agent_first_reply_time | FLOAT | N/A|
|work_item_handling_time | FLOAT | time from the assignment timestamp until it's resolved (in minutes). Does not include time spent on the customer's side|
|work_item_queue_time | FLOAT | time from the start of a work item untill it gets assigned (in minutes)|
|working_item_l1_close | INTEGER | work item closed Level 1 (not escalated)|
|working_item_l2 | INTEGER | work item level 2 (child case)|
|working_item_l1_escalated | INTEGER | escalated work item Level 1 (parent)|
|alligned_with_qa | INTEGER | N/A|