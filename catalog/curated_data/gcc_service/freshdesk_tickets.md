# Freshdesk Tickets

|Column | Type | Description|
| :--- | :--- | :--- | 
|country | STRING | Country|
|region | STRING | Region|
|global_entity_id | STRING | N/A|
|management_entity | STRING | N/A|
|chat_created_at | TIMESTAMP | Related chat creation timestamp|
|ticket_created_at | TIMESTAMP | Freshdesk Ticket creation timestamp|
|dispatch_center | STRING | Dispatch Center|
|contact_reason_l1 | STRING | Rider Contact Reason Level 1|
|contact_reason_l2 | STRING | Rider Contact Reason Level 2|
|interaction_id | STRING | Related chat unique identifier|
|ticket_id | INTEGER | Unique Identifier of the Freshdesk Ticket|
|rider_contract_type | STRING | il_global_ccn.freshdesk_fct_tickets|
|rider_city | STRING | Rider's city|
|order_id | STRING | Order for which the freshdesk ticket was created|
|logistics_team_feedback | STRING | Boolean indicating if the agent sent an escalation/feedback to the Logistics department upon chat resolution|
|cs_team_feedback | STRING | Boolean indicating if the agent sent an escalation/feedback to the CS department upon chat resolution|
|ps_team_feedback | STRING | Boolean indicating if the agent sent an escalation/feedback to the PS department upon chat resolution|