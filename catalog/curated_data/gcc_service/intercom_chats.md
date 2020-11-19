# Intercom Chats

|Column | Type | Description|
| :--- | :--- | :--- | 
|conversation_id | INTEGER | id of the partner's thread in which the chats happend|
|chat_id | STRING | unique chat id (conversation_id ::ordinal)|
|queue_id | STRING | identifier of the queue to which the chat was allocated|
|queue_name | STRING | name of the queue to which the chat was allocated (e.g. PS TW)|
|country_code | STRING | 2 letter code of the country from which the chat originated|
|country | STRING | country from which the chat originated|
|brand | STRING | default to Foodpanda|
|department_code | STRING | PS - Partner Service (ftaken from the queue name (e.g. PS TW)|
|agent_id | STRING | identifier in intercom system of the agent who served the chat|
|agent_email | STRING | email of the agent who served the chat|
|created_timestamp | TIMESTAMP | timestamp of the creation of the chat (after the bot time) (local time)|
|assigned_queue_timestamp | TIMESTAMP | timestamp of the assignment to the Queue (local time)|
|assigned_agent_timestamp | TIMESTAMP | timestamp of the assignment to the Agent (local time)|
|first_reply_timestamp | TIMESTAMP | timestamp of the First Reply of the Agent (local time)|
|closed_timestamp | TIMESTAMP | timestamp of the closure of the chat (local time)|
|bot_chat | INTEGER | 1 if the chat was handled only by a bot (agent id is null)|
|tag_id | STRING | id of the tag applied during the chat|
|tag_name | STRING | label of the tag applied during the chat|
|tag_applied_at | TIMESTAMP | timestamp at which the tag was applied (local time)|
|global_pcr_code | STRING | global Partner Contact Reason code, related to the tag applied|
|chat_queue_time | INTEGER | time difference in seconds between chat creation and assignment to agent (chat error =0)|
|chat_first_reply_time | INTEGER | time difference in seconds between assignment to agent and first reply of the agent (chat error =0)|
|chat_agent_first_reply_time | INTEGER | time difference in seconds between assignment to agent and first reply of the agent (chat error =0)|
|chat_handling_time | INTEGER | time difference in seconds between assignment to agent and closure (chat error =0)|
|user_id | STRING | unique identifier in Intercom system of the Partner|
|user_browser_language_code | STRING | N/A|
|language_code | STRING | N/A|
|user_browser_language | STRING | language used by the partner in his browser|
|management_entity | STRING | N/A|
|global_entity_id | STRING | global entity id of the country/brand from which the chat originated|
|region | STRING | N/A|
|incoming_chats | INTEGER | N/A|
|work_items | INTEGER | N/A|
|served_chats | INTEGER | when chat is not missed|
|missed_chats | INTEGER | chat is missed when it's closed before partner replies, or when the partner replies more than 2 hours after|
|chat_within_sla_5 | INTEGER | chat where first relpy time < 5 sec (chat error =0)|
|chat_within_sla_10 | INTEGER | chat where first relpy time < 10 sec (chat error =0)|
|chat_within_sla_30 | INTEGER | chat where first relpy time < 30 sec (chat error =0)|
|chat_within_sla_50 | INTEGER | chat where first relpy time < 50 sec (chat error =0)|
|chat_within_sla_60 | INTEGER | chat where first relpy time < 60 sec (chat error =0)|
|chat_error | INTEGER | flag for chats with unexpected worklow which creates metrics unreliability. 0 when chat without issue|
|single_chat_conv | INTEGER | chat that belongs to a conversation that was not reopened.|