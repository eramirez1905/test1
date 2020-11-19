# Freshchat Chats

|Column | Type | Description|
| :--- | :--- | :--- | 
|instance_id | INTEGER | Freshchat instance id|
|instance_name | STRING | Freshchat instance name|
|country | STRING | Country|
|region | STRING | Region|
|dispatch_center | STRING | Dispatch Center|
|global_entity_id | STRING | N/A|
|management_entity | STRING | N/A|
|resolution_label_subcategory | STRING | N/A|
|resolution_label | STRING | N/A|
|language | STRING | Language|
|interaction_id | STRING | Unique identifier of the Freshchat interaction|
|conversation_id | INTEGER | Unique identifier of the Freshchat conversation to which the chats belongs|
|channel | STRING | Channel in which the chat is routed (bears the country code)|
|resolution_group | STRING | Group in which the chat is resolved (bears the language code)|
|dispatcher_email | STRING | Email of the Agent who got assigned to the chat|
|first_assigned_group | STRING | Group in which the chat is routed (bears the language code)|
|chat_created_at | TIMESTAMP | Timestamp of the creation of the chat|
|first_assigned_timestamp | TIMESTAMP | Timestamp of the chat first assignment to an agent|
|first_response_timestamp | TIMESTAMP | Timestamp of the chat first response by an agent|
|time_to_first_response_from_create | INTEGER | Time between creation and first response (in seconds)|
|avg_response_time_secs | INTEGER | Average response time between 2 messages|
|resolution_timestamp | TIMESTAMP | Timestamp of the chat resolution|
|time_to_resolve_from_create | INTEGER | Time between creation and chat resolution (in seconds)|
|response_count | INTEGER | Number of messages exchanged during the chat|
|csat_rating | INTEGER | CSAT Score of the chat by the rider|
|csat_response | STRING | Text response by the rider for the CSAT survey|
|email | STRING | Email of the Rider|
|identifier | STRING | ID of the Rider in Freshchat|
|first_name | STRING | First name of the Rider|
|last_name | STRING | Last name of the Rider|
|chat_within_sla_5 | INTEGER | Chat within SLA 5 sec (First Reply happened in less than 5 sec)|
|chat_within_sla_10 | INTEGER | Chat within SLA 10 sec (First Reply happened in less than 10 sec)|
|chat_within_sla_30 | INTEGER | Chat within SLA 30 sec (First Reply happened in less than 30 sec)|
|chat_within_sla_50 | INTEGER | Chat within SLA 50 sec (First Reply happened in less than 50 sec)|
|chat_within_sla_60 | INTEGER | Chat within SLA 60 sec (First Reply happened in less than 60 sec)|
|handling_time | FLOAT | Handling time of the chat|
|dropped_within_30_sec | INTEGER | Chat was not answered in the first 30sec of queue time|
|first_reply_time | FLOAT | Time between first assignment and first repl|
|queue_time | FLOAT | Time between creation and first assignment|
|incoming_chat | INTEGER | N/A|
|work_item_chat | INTEGER | N/A|
|missed_chat | INTEGER | Chat that was not answered in less than 5 minutes|
|good_rating | INTEGER | Chat with a CSAT Rating of 4 or 5|
|chat_rated | INTEGER | Chat with a CSAT Rating|
|automated_chat | INTEGER | N/A|
|dwh_created_at | TIMESTAMP | N/A|