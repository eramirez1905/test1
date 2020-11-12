# Qualtrics Csat Responses

|Column | Type | Description|
| :--- | :--- | :--- | 
|source_id | INTEGER | unique entity identifier from DH|
|global_entity_id | STRING | unique entity identifier (global)|
|country | STRING | country|
|management_entity | STRING | N/A|
|region | STRING | region|
|brand | STRING | brand|
|response_id | STRING | unique identifier of the response|
|survey_id | STRING | related survey|
|chat_id | STRING | Helpcenter chat id pushed to Qualtrics by the helpcenter|
|start_date | TIMESTAMP | datetime when the user started filling the response|
|response_date | DATE | date when the user finished filling the response|
|end_date | TIMESTAMP | datetime when the user finished filling the response|
|recorded_date | TIMESTAMP | N/A|
|user_language | STRING | language of the user|
|type | STRING | takeholder|
|finished | BOOLEAN | true if the user completed the survey entirely|
|status | INTEGER | N/A|
|duration | INTEGER | time spent by the user filling up the response|
|recipient_email | STRING | email of the user that filled the response|
|distribution_channel | STRING | N/A|
|global_ccr_code | STRING | global Customer Contact Reason sent by the Helpcenter|
|csat | INTEGER | value selected to the csat question|
|case_number | STRING | Salesforce case number in case the channel is Email|
|device | STRING | operating system of the device used by the customer|
|locale | STRING | language and country code (e.g. en-PK) pushed from the Helpcenter|
|order_id | STRING | order id pushed from the Helpcenter|
|origin | STRING | channel|
|progress | INTEGER | percentage of completion of the survey|
|qstar_13 | INTEGER | N/A|
|qnegative | STRING | in case of negative csat (4,5) , these are the attributes chosen|
|qpositive | STRING | in case of positive csat (4,5) , these are the attributes chosen|
|a1 | STRING | The conversation was long and complicated|
|a2 | STRING | The agent took long to respond|
|a3 | STRING | My question was not answered|
|a4 | STRING | The agent didn't explain things clearly|
|a5 | STRING | The agent's language skills were poor|
|a6 | STRING | The agent was unfriendly|
|a7 | STRING | The agent didn't understand my question|
|a8 | STRING | The agent was unprofessional|
|a9 | STRING | The agent lacked knowledge|
|a10 | STRING | The agent was difficult to understand|
|a11 | STRING | The outcome wasn't what I expected|
|a12 | STRING | This isn't my first time contacting support about this issue|
|a13 | STRING | It took long to get through to an agent|
|a14 | STRING | The Help Center was difficult to use|
|qagent | STRING | agent question (yes/no)|
|qcomment | STRING | free text comment|
|q6 | STRING | N/A|
|q7 | STRING | N/A|
|q8 | STRING | N/A|