# Help Center Session Events

|Column | Type | Description|
| :--- | :--- | :--- | 
|source | STRING | Product source : Help Center or Alan|
|global_entity_id | STRING | Global Entity ID - Unique identifier of the entity (country + brand) - e.g. FP_SG|
|region | STRING | Region|
|management_entity | STRING | N/A|
|event_id | STRING | Unique identifier of the event|
|flow_session_id | STRING | Unique identifier of the session|
|ordinal | INTEGER | Event ordinal in the session|
|event_type | STRING | Type of event, e.g. TICKET_CREATED|
|event_category | STRING | N/A|
|view_id | STRING | Leaf Name on which the event is happening|
|view_type | STRING | N/A|
|event_timestamp | TIMESTAMP | Timestmap on which the event started|
|agent_id | STRING | Agent who was assigned to the created contact|
|agent_name | STRING | ID of the Agent who was assigned to the created contact|
|platform | STRING | Platform on which the chat/case was created (e.g. Salesforce)|
|chat_id | STRING | Unique identifier of the chat created during that event|
|message | STRING | N/A|
|file | STRING | N/A|
|chat_ended_reason | STRING | N/A|
|position | INTEGER | N/A|
|external_chat_id | STRING | Unique identifier of the chat created during that event, to be matched with alan_identifier in Salesforce|
|department_id | STRING | N/A|
|new_department_id | STRING | N/A|
|ticket_offered | BOOLEAN | true if a Case was created during this event|
|type | STRING | N/A|
|priority | STRING | N/A|
|contact_reason_level_1 | STRING | Customer/Partner Contact Reason Level 1|
|contact_reason_level_2 | STRING | Customer/Partner Contact Reason Level 2|
|contact_reason_level_3 | STRING | Customer/Partner Contact Reason Level 3|
|form_field_name_1 | STRING | Name of the first field of the form displayed during this event|
|form_field_value_1 | STRING | Value entered by visitor in the first field of the form displayed during this event|
|form_field_name_2 | STRING | Name of the second field of the form displayed during this event|
|form_field_value_2 | STRING | Value entered by visitor in the second field of the form displayed during this event|
|order_id | STRING | Related order Id tracked in this event|
|exception | STRING | N/A|
|user_description | STRING | N/A|
|help_access_point | STRING | N/A|
|button_id | STRING | N/A|
|feedback | STRING | N/A|
|data | STRING | N/A|
|ticket_id | STRING | Unique identifier of the Case created during that event, to be matched with alan_identifier in Salesforce|
|api_name | STRING | N/A|
|api_response | STRING | N/A|
|api_data | STRING | N/A|
|report_date | DATE | Date (YYYY-MM--DD) at which the event happened|
|is_leaf | INTEGER | Determines if the event is a Leaf or not [Condition: event_type = 'NAVIGATION' and contact_reason_level_3 is not null]|
|dropped_chat | INTEGER | Related Chat created during the event had a Queue Time of less than 30s and was missed|
|dropped_chat_5s | INTEGER | Related Chat created during the event had a Queue Time of less than 5s and was missed ( Accidental Chats)|
|missed_chat | INTEGER | Related Chat created during the event was missed|
|ht_served | INTEGER | N/A|
|served_chats | INTEGER | N/A|