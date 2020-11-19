# users

|Column | Type | Description|
| :--- | :--- | :--- | 
|email | STRING | Agent's work email|
|bamboo_id | STRING | Agent's Bamboo User ID|
|bamboo_active | BOOLEAN | Account active or not in Bamboo|
|zendesk_id | INTEGER | Agent's Zendesk User ID|
|zendesk_active | BOOLEAN | Account active or not in Zendesk|
|zopim_id | INTEGER | Agent's Zopim User ID|
|zopim_active | BOOLEAN | Account active or not in Zopim|
|shyftplan_id | INTEGER | Agent's Shyftplan User ID|
|shyftplan_active | BOOLEAN | Account active or not in Shyftplan|
|shyftplan_employment_id | INTEGER | Agent's employement ID in Shyftplan|
|shyftplan_company_id | INTEGER | Company ID in Shyftplan in which the agent is registered|
|rooster_id | INTEGER | N/A|
|rooster_active | BOOLEAN | N/A|
|rooster_instance_id | INTEGER | N/A|
|playvox_id | STRING | Agent's Playvox User ID|
|playvox_active | BOOLEAN | Account active or not in Playvox|
|playvox_status | STRING | Agent's status in Playvox|
|playvox_last_login_date | TIMESTAMP | Agent's last login date in Playvox|
|playvox_user_type | STRING | Agent's user type (role) in Playvox|
|playvox_instance | STRING | Instance ID in Playvox in which the agent is registered|
|playvox_team_description | STRING | Description of the Team in which the agent is registered|
|nvm_id | INTEGER | Agent's NVM User ID (all instances of NVM)|
|nvm_active | BOOLEAN | Account active or not in NVM|
|salesforce_pd_id | STRING | Agent's Pandora Salesforce User ID|
|salesforce_pd_active | BOOLEAN | Account active or not in Pandora Salesforce|
|salesforce_id | STRING | Agent's DH Salesforce User ID|
|salesforce_active | BOOLEAN | Account active or not in DH Salesforce|
|salesforce_profile_name | STRING | Profile name of the Agent in DH Salesforce|
|salesforce_user_role_name | STRING | User Role name of the Agent in DH Salesforce|
|salesforce_last_login | TIMESTAMP | Agent's last login date in DH Salesforce|
|salesforce_q_case | BOOLEAN | True if and only if the agent is in one or more Case queue type in DH Salesforce|
|salesforce_q_chat | BOOLEAN | True if and only if the agent is in one or more Chat queue type in DH Salesforce|
|freshchat_active | BOOLEAN | Account active or not in Freshchat (at least 1 interaction in the last 3 months)|
|elastix_id | INTEGER | N/A|
|intercom_id | STRING | N/A|
|avaya_chat_id | INTEGER | N/A|
|avaya_call_id | STRING | N/A|
|pandacare_id | STRING | N/A|
|hire_date | DATE | Bamboo hire date of the Agent|
|contract_end_date | DATE | Bamboo Contract End date of the Agent|
|division | STRING | Bamboo Division of the Agent|
|brand | STRING | Bamboo brand for which the Agent is working for (based on Division)|
|bpo | STRING | Bamboo BPO/BPO name for which the Agent is working for (based on Division)|
|reporting_to | STRING | Bamboo Team Lead of the Agent|
|idam_access_status | STRING | N/A|
|number_of_agents | INTEGER | Bamboo number of agents in the team of the Agent (reporting to the same Team Lead)|
|cc_effective_date | DATE | Bamboo Contract information Effective Date of the Agent, from the CCI section|
|cc_department | STRING | Bamboo department (Customer, Partner or Rider Service) from the CCI section|
|cc_team | STRING | Bamboo Team (Chat, Calls and Order Processing...) of the Agent from the CCI section|
|cc_working_language | STRING | Bamboo Working Language of the Agent from the CCI section|
|cc_working_country | STRING | Bamboo Working Country of the Agent from the CCI section|
|cc_level | FLOAT | Bamboo Level the Agent from the CCI section|
|cc_contract_min_weekly_hours | FLOAT | Bamboo Minimun working hours per week of the Agent from the CCI section|
|cc_contract_max_weekly_hours | FLOAT | Bamboo Maximum working hours per week of the Agent from the CCI section|
|name | STRING | Bamboo full name of the Agent|
|place_of_work | STRING | Bamboo place of work of the Agent|
|employment_status | STRING | Bamboo Employement Status of the Agent|
|center | STRING | Contact Center to which the agent belongs (mapped which email extension)|
|management_entity_group | STRING | Group of entities (for Tableau permissions) (mapped with email extension)|
|region | STRING | Region to which the agent belongs (mapped which email extension)|
|management_entity | STRING | N/A|