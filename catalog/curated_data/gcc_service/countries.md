# Countries

|Column | Type | Description|
| :--- | :--- | :--- | 
|dh_source_id | INTEGER | Unique identifier of the entity (country + brand)|
|rdbms_id | INTEGER | Pandora unique identifier of the entity (country + brand)|
|main_language | STRING | Main language spoken in the country|
|main_language_iso | STRING | Language identifier (iso code with 3 letters)|
|common_name | STRING | Country name|
|country_iso | STRING | Country identifier (iso code with 2 letters)|
|aggregated_countries | STRING | N/A|
|number_of_aggregated_countries | INTEGER | N/A|
|region | STRING | Region|
|brand | STRING | Brand name|
|brand_code | STRING | Brand identifier (code with 2 letters)|
|dispatch_rdbms_id | INTEGER | Unique backend identifier for Logistics backend (1-to-1 relationship with log_country_code)|
|telephone_code | INTEGER | Telephone prefix of the country|
|contact_center | STRING | Contact Center|
|dispatch_center | STRING | Dispatch Center|
|cs_ps_location_country_iso | STRING | Geographical location of the Contact Center|
|dp_location_country_iso | STRING | Geographical location of the Dispatch Center|
|management_entity_group | STRING | Group of entities (for Tableau permissions)|
|management_entity | STRING | Management Entity to which the entity is attached to legally|
|dp_instance_id | INTEGER | Freshchat instance id|
|dp_main_brand | STRING | N/A|
|is_pandora | INTEGER | Identifies if the entity belongs to Pandora platform (1) or not (0)|
|live | STRING | Identifies if the entity is live or not, reporting-wise|
|cs_setup | STRING | Country (CB) or Language-based (LB) setup of the entity for Customer Service|
|ps_setup | STRING | Country (CB) or Language-based (LB) setup of the entity for Partner Service|
|dp_disp_setup | STRING | Country (CB) or Language-based (LB) setup of the entity for Rider Service - Communicators team|
|dp_coms_setup | STRING | Country (CB) or Language-based (LB) setup of the entity for Rider Service - Dispatchers team|
|location_timezone | STRING | Timezone of the location of the entity (Local Time)|
|cs_ps_location_timezone | STRING | Timezone of the location of the contact center (CC Time)|
|dp_location_timezone | STRING | Timezone of the location of the dispatch center (DP Time)|
|dwh_source_code | STRING | Global Entity ID - Unique identifier of the entity (country + brand) - e.g. FP_SG|
|global_entity_id | STRING | N/A|
|gcc_region | STRING | N/A|
|dh_region | STRING | N/A|
|dwh_created_at | TIMESTAMP | N/A|