# Rider KPI

This table displays the principal information from riders, and calculates the main KPIs of the rider's performance. The information is aggregate on a daily rider level, and the 'DATE' type columns are already in local time according to the delivery's country. Time-related fields are displayed in seconds.

| Column | Type | Description | How to use it
| :--- | :--- | :--- | :--- |
| created_date_local | `DATE` | The date when the delivery was completed or when the shift starts. (Partition column) | |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. | |
| country_name | `STRING` | The name of the country in English. | |
| city_id | `INTEGER` | The numeric identifier assigned to the city. This id is only unique within the same country. | |
| city_name | `STRING` | Name of the city in English. | |
| zone_id | `INTEGER` | The numeric identifier assigned to the drop-off zone. This id is only unique within the same country. | |
| zone_name | `STRING` | The name of the zone. | |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. | |
| rider_name | `STRING` | Rider's name and surname used by Rooster. | |
| email | `STRING` | Rider's email used by Rooster. | |
| batch_number | `INTEGER` | Batch assigned to the rider during a specific timeframe. | |
| current_batch_number | `INTEGER` | Group assigned by biweekly scoring run in Rooster (current status). | |
| contract_type | `STRING` | Contract type used by Rooster, for example {"PART_TIME", "FULL_TIME", "FREELANCER"}. | |
| contract_name | `STRING` | Name of the contract in Rooster. | |
| job_title | `STRING` | Rider's job title as assigned in Rooster. | |
| contract_start_date_local | `DATE` | The date the contract started in local time. | |
| contract_end_date_local | `DATE` | The date the contract ended in local time. | |
| contract_creation_date_local | `DATE` | The date and time the contract was created in Rooster in local time. | |
| hiring_date_local | `DATE` | The date when the rider has been created in Rooster in local time. | |
| rider_contract_status | `STRING` | The current status of the rider's contract, could be 'inactive' or 'active'. | |
| captain_name | `STRING` | Rider_name of the rider to whom rider is reporting to. | |
| vehicle_profile | `STRING` | The google profile used to estimate the driving times to the vendor and the customer. | |
| vehicle_name | `STRING` | The name of the vehicle with which the rider made the deliveries. | |
| tenure_in_weeks | `FLOAT` | The contract duration in weeks. | |
| tenure_in_years | `FLOAT` | The contract duration in years. | |
| time to street | `INTEGER` | Time from the contract start date until the first shift is completed, in days within the period under consideration. | |
| shifts_done | `INTEGER` | The number of evaluated shifts. | |
| working_time | `INTEGER` | Difference in seconds of the actual end time of the shift and actual start time of the shift. | |
| planned_working_time | `INTEGER` | Difference in seconds of the planned shift end time and the planned shift start time. | |
| break_time | `INTEGER` | The time in seconds the rider was put on break by a dispatcher or automatically. Break durations are a non paid status. | Break Time Ratio =  break_time / (break_time + working_time) |
| late_shifts | `INTEGER` | The number of shifts which the rider showed up late to. | Late Shifts Ratio = late_shifts / shifts_done |
| all_shifts | `INTEGER` | The number of shifts. | |
| no_shows | `INTEGER` | Number of shifts which the rider applied for but didn’t show up to. | |
| unexcused_no_shows | `INTEGER` | Number of shifts which the rider applied for but didn't show up either requested an absence. | |
| peak_time | `INTEGER` | Time in seconds spent by rider on a shift during a time of great demand. Peak times vary from country to country. | Peak Time Ratio = peak_time / working_time |
| weekend_shifts | `INTEGER` | Number of shifts completed (at least partly) during the weekend. Weekends stretch from Friday to Sunday. | |
| transition_working_time | `FLOAT` | The time in seconds the rider spent in working statuses. | Idle Time Ratio = (transition_working_time - transition_busy_time) / transition_working_time |
| transition_busy_time | `FLOAT` | The time in seconds the rider was not in break time. | Busy Time Ratio = transition_busy_time / transition_working_time |
| swaps_accepted | `INTEGER` | Number of shift swaps accepted by a rider. | |
| swaps_pending_no_show | `INTEGER` | Number of shift swaps accepted by a rider and then don't show up. | |
| swaps_accepted_no_show | `INTEGER` | Number of shift swaps on pending status that the rider don't show up. | |
| [deliveries](#deliveries) | `ARRAY<RECORD>` | Details of deliveries at entity level. | |

### Deliveries

| Column | Type | Description | How to use it
| :--- | :--- | :--- | :--- |
| entity | `STRING` | Display name of the local operational entity corresponding to the orders. | |
| entity_id | `STRING` | The code of the local operational entity representing the country. This code is made out of two codes : the two-letter code of the venture, and the country iso. | |
| completed_deliveries | `INTEGER` | Number of completed deliveries made by the rider. | Rider UTR = count_deliveries / (working_time/3600) |
| cancelled_deliveries | `INTEGER` | Number of cancelled deliveries taken by the rider. | |
| cancelled_deliveries_no_customer | `INTEGER` | Number of cancelled deliveries because the rider didn't find the customer. | |
| cancelled_deliveries_after_pickup | `INTEGER` | Number of cancelled deliveries after the rider picked up them. | |
| picked_up_deliveries | `INTEGER` | Number of picked up deliveries. | |
| rider_notified | `INTEGER` | Number of times the rider was notified about a delivery. | Rider Acceptance Rate = rider_accepted / rider_notified |
| rider_accepted | `INTEGER` | Number of times the rider accepted to make a delivery. | |
| undispatched_after_accepted | `INTEGER` | Number of orders that where accepted by the rider to be delivered but then these were undispatched. | Undispatched After Accepted = undispatched_after_accepted / rider_notified |
| effective_delivery_time_sum | `INTEGER` | The total time in seconds of the actual delivery time of the deliveries. | Rider Delivery Time = (effective_delivery_time_sum / 60) / effective_delivery_time_count |
| effective_delivery_time_count | `INTEGER` | Number of completed deliveries. | |
| at_vendor_time_sum | `INTEGER` | The total time in seconds difference between rider arrival at vendor and the rider picking up the food. | At vendor time = at_vendor_time_sum / at_vendor_time_count |
| at_vendor_time_count | `INTEGER` | The total records counted. | |
| at_customer_time_sum | `INTEGER` | The time difference between rider arrival at customer and the pickup time. | At customer time = at_customer_time_sum / at_customer_time_count |
| at_customer_time_count | `INTEGER` | The total records counted. | |
| dropoff_distance_sum | `FLOAT` | Distance in meters calculated from rider left pickup location until rider near customer location as in https://en.wiktionary.org/wiki/Manhattan_distance | Dropoff Distance = dropoff_distance_sum / dropoff_distance_count |
| dropoff_distance_count | `INTEGER` | The total records counted. | |
| pickup_distance_sum | `FLOAT` | Distance in meters calculated from rider accepted location until rider near pickup location as in https://en.wiktionary.org/wiki/Manhattan_distance | Pickup Distance = pickup_distance_sum / pickup_distance_count |
| pickup_distance_count | `INTEGER` | The total records counted. | |
| reaction_time_sec_sum | `INTEGER` | The total time between rider notification and rider acceptance. In case multiple riders accepted the delivery, the last event is taken. | Reaction Time (in seconds) = reaction_time_sec_sum / reaction_time_sec_count |
| reaction_time_sec_count | `INTEGER` | The total records counted. | |
| to_vendor_time_sum | `INTEGER` | The total time between rider acceptance and arriving at the restaurant. | |
| to_vendor_time_count | `INTEGER` | The total records counted. | |
| to_customer_time_sum | `INTEGER` | The total time difference between rider arrival at customer and the pickup time. | |
| to_customer_time_count | `INTEGER` | The total records counted. | |
| last_delivery_over_10_count | `INTEGER` | Total the work days in which the rider spent more than 10 minutes at_customer_time in the last delivery of the day. | |
| at_customer_time_under_5_count | `INTEGER` | Total deliveries in which the rider spent less than 5 minutes at_customer_time. | |
| at_customer_time_5_10_count | `INTEGER` | Total deliveries in which the rider spent between 5 and 10 minutes at_customer_time. | |
| at_customer_time_10_15_count | `INTEGER` | Total deliveries in which the rider spent between 10 and 15 minutes at_customer_time. | |
| at_customer_time_over_15_count | `INTEGER` | Total deliveries in which the rider spent more than 15 minutes at_customer_time. | |

### Examples:

Since in a country a rider can work for multiple entities and being entity an order attribute (and not a rider attribute), rider specific metrics (such as working_hours) need to be calculated based on all dimensions except entity.
```sql
WITH aggregation AS (
  SELECT created_date_local
    , country_name
    , zone_id
    , rider_id
    , d.entity_id
    , d.completed_deliveries
    , d.cancelled_deliveries
    , d.cancelled_deliveries_no_customer
    , d.effective_delivery_time_sum
    , d.effective_delivery_time_count
    , d.at_customer_time_sum
    , d.at_customer_time_count
    , d.at_vendor_time_sum
    , d.at_vendor_time_count
    , d.dropoff_distance_sum
    , d.dropoff_distance_count
    , d.pickup_distance_sum
    , d.pickup_distance_count
    , d.rider_accepted
    , d.rider_notified
    , d.undispatched_after_accepted
    -- Metrics calculated excluding entity dimension
    , SUM(d.completed_deliveries) OVER (PARTITION BY created_date_local, country_code, city_id, rider_id, batch_number, vehicle_name, zone_id, current_batch_number, job_title) AS country_level_completed_deliveries
    , SUM(working_time / 3600) OVER (PARTITION BY created_date_local, country_code, city_id, rider_id, batch_number, vehicle_name, zone_id, current_batch_number, job_title) AS country_level_working_hours
    , SUM(late_shifts) OVER (PARTITION BY created_date_local, country_code, city_id, rider_id, batch_number, vehicle_name, zone_id, current_batch_number, job_title) AS late_shifts
    , SUM(shifts_done) OVER (PARTITION BY created_date_local, country_code, city_id, rider_id, batch_number, vehicle_name, zone_id, current_batch_number, job_title) AS shifts_done 
  FROM `fulfillment-dwh-production.curated_data_shared.rider_kpi` r 
  LEFT JOIN UNNEST(r.deliveries) d
  WHERE country_code = 'ae'
    AND created_date_local = '2020-09-05'
)
SELECT created_date_local
  , country_name
  , zone_id
  , rider_id
  , entity_id
  , ROUND(SUM(country_level_working_hours), 2) AS working_hours
  , SUM(completed_deliveries) AS completed_deliveries
  , ROUND(SAFE_DIVIDE(SUM(country_level_completed_deliveries), SUM(country_level_working_hours)), 2) AS utr
  , SUM(cancelled_deliveries) AS cancelled_deliveries
  , SUM(cancelled_deliveries_no_customer) AS cancelled_deliveries_no_customer
  , ROUND(SAFE_DIVIDE(SUM((effective_delivery_time_sum / 60)), SUM(effective_delivery_time_count)), 2) AS rider_delivery_time
  , ROUND(SAFE_DIVIDE(SUM(at_customer_time_sum / 60), SUM(at_customer_time_count)), 2) AS at_customer_time
  , ROUND(SAFE_DIVIDE(SUM(at_vendor_time_sum / 60), SUM(at_vendor_time_count)), 2) AS at_vendor_time
  , ROUND(SAFE_DIVIDE(SUM((dropoff_distance_sum / 1000)), SUM(dropoff_distance_count)), 2) AS dropoff_distance
  , ROUND(SAFE_DIVIDE(SUM((pickup_distance_sum / 1000)), SUM(pickup_distance_count)), 2) AS pickup_distance
  , ROUND(SAFE_DIVIDE(SUM(rider_accepted), SUM(rider_notified)), 2) AS rider_acceptance_rate
  , ROUND(SAFE_DIVIDE(SUM(undispatched_after_accepted), SUM(rider_notified)), 2) AS undispatched_after_accepted_rate
  , ROUND(SAFE_DIVIDE(SUM(late_shifts), SUM(shifts_done)), 2) AS late_shift_ratio
FROM aggregation
GROUP BY 1, 2, 3, 4, 5
```
