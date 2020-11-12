# Pandacare Chats

|Column | Type | Description|
| :--- | :--- | :--- | 
|chat_id | STRING | unique id of the chat in Panda Care|
|country | STRING | country|
|language | STRING | language|
|brand | STRING | brand (Foodpanda only fornow)|
|queue_name | STRING | name of the queue|
|department | STRING | department in which the chat happened ( e.g Customer Service)|
|hc_identifier | STRING | session id of the Helpcenter session that created this chat|
|hc_leaf | STRING | leaf id on which this chat was created|
|hc_deflection_id | STRING | deflection id of the leaf on which this chat was created|
|customer_email | STRING | email of the ustomer who requested the chat|
|order_id | STRING | order id related to the chat|
|agent_id | STRING | id of the agent who served got assigned to the chat|
|email | STRING | email of the agent who served the chat|
|missed_chat | INTEGER | if the customer left the chat before agent replied, it's a missed chat|
|served_chat | INTEGER | when the chat is not missed|
|global_ccr_code | STRING | N/A|
|queue_time | INTEGER | queue time of the chat - from Creation to First Assignment|
|chat_first_reply_time | INTEGER | first response time by an agent - from First Assignment to First Reply (in seconds)|
|agent_first_reply_time | INTEGER | time that the agent spent serving the chat - From First Reply to Closed (in seconds)|
|handling_time | INTEGER | time that the agent spent serving the chat - From First Reply to Closed (in seconds)|
|creation_timestamp | TIMESTAMP | time creation of the chat (local time)|
|first_assignment_timestamp | TIMESTAMP | time of first assignment of the chat to an agent (local time)|
|closed_timestamp | TIMESTAMP | timestamp when chat is closed (local time)|
|management_entity | STRING | N/A|
|region | STRING | N/A|
|global_entity_id | STRING | global entity id of the country/brand from which the chat originated|