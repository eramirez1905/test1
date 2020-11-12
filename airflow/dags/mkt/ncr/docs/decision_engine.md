# NCR Decision Engine

The goal of NCR decision engine is to decide which users to target with which NCR campaign.

To do so, it takes as in input

- the currently active NCR Vendor deals from `{{ brand_code }}_ncr_midas_campaign_bookings` tables
- the user campaign eligibility from `{{ brand_code }}_campaign_eligibility` tables

The mapping criteria for the decision engine are

- Every user should be assign to maximum one campaign.
- Send out the maximum number of campaigns across all vendors
- No more than `left_notifications` should be send out per vendor

## Ranking logic

At it's core, the decision engine uses a simple ranking factor to assign users to restaurants.

- For every **restaurant** we consider the total notifications we want to send (call it `X`)
- For every **restaurant** we compute the total number of reachable (eligible) users (call it Y`)
- Define decision factor is defined as `X / Y`
- For every **user** we assign them to be targeted by the restaurant with the **highest** decision factor

A decision factor of 0.5 means the restaurant has twice as many targetable users than `left_notifications`.
A decision factor of 2.0 means the restaurant will only be able to send at maximum half of their `left_notifications`, because no more than that number of users are targettable.
The decision factor is a measure of how "desperately" a restaurant needs to target the users.

## Example

Consider the following (simplified) example to understand ...

1. Why a decision logic is needed
2. How the decision engine finds a "good" mapping

<table>
<tr>
    <th>user_vendor_campaign_eligibility </th>
    <th>ncr_vendor_campaigns</th></tr>
<tr>
    <td>

| restaurant_id | user_id |
| ------------- | ------- |
| 1             | A       |
| 1             | B       |
| 1             | C       |
| 1             | D       |
| 1             | E       |
| 1             | F       |
| 2             | C       |
| 2             | D       |
| 2             | E       |
| 3             | E       |

</td>
<td>

| restaurant_id  | left_notifications |
| -------------- | ------------------ |
| 1              | 3                  |
| 2              | 2                  |
| 3              | 1                  |
| 4              | 4                  |

</td>
</tr>
</table>

Here is a representation of this matching problem as a directed acyclic graph (DAG).

- Source (S) and Target (T) nodes are only added for the visualization
- Nodes on the left side represent a vendor (1, 2, 3, 4)
- Nodes on the right side represent a user (A, B, C, D, E, F)
- Edges between (S) and vendor represent the `left_notifications` for that vendor
- And edge between a vendor and a user represents an eligible match

![sample user vendor mapping](images/user_vendor_mapping_raw.svg?raw=true&sanitize=true "Sample user vendor mapping")

## What we want to avoid

This examples illustrates an not-ideal solution and tries to justify why the decision engine is needed.
If (for whatever reason) we assign vendor (1) to send it's three notifications to users (C, D, E) we cannot send any further notifications.
This is because vendor (2) could only target users (C, D, E) but these are already occupied (Remember that every users can only receive notification from at most one vendor).
Likewise, Vendor (3) could only target (E) which is already occupied.

![sample user vendor mapping unideal solution](images/user_vendor_mapping_unideal.svg?raw=true&sanitize=true "Sample user vendor mapping - unideal solution")

As a result, the total number of notification (across all vendors) in this solution is **3**.

## Finding the ideal solution

First we compute the decision factor per **restaurant** as described above.

| restaurant_id | user_ids | targetable_customers | left_notifications | decision_factor |
| ------------- | -------- | -------------------- | ------------------ | --------------- |
| 1             | ABCDEF   | 6                    | 3                  | 0.5             |
| 2             | CDE      | 3                    | 2                  | 0.6666          |
| 3             | E        | 1                    | 1                  | 1               |

Then we rank restaurants for each **user** based on decision factor.
Highlighting the interesting cases:

| user_id | restaurant_id | targetable_customers | left_notifications | decision_factor | rank |
| ------- | ------------- | -------------------- | ------------------ | --------------- | ---- |
| A       | 1             | 6                    | 3                  | 0.5             | 1    |
| B       | 1             | 6                    | 3                  | 0.5             | 1    |
| C       | 1             | 6                    | 3                  | 0.5             | 2    |
| C       | 2             | 3                    | 2                  | **0.6666**      | 1    |
| D       | 1             | 6                    | 3                  | 0.5             | 2    |
| D       | 2             | 3                    | 2                  | **0.6666**      | 1    |
| E       | 1             | 6                    | 3                  | 0.5             | 3    |
| E       | 2             | 3                    | 2                  | 0.6666          | 2    |
| E       | 3             | 1                    | 1                  | **1**           | 1    |
| F       | 1             | 6                    | 3                  | 0.5             | 1    |

Considering only the highest decision_factor per user (rank=1) yields a mapping that is unique per user and represents our final matching.

| user_id | restaurant_id | targetable_customers | left_notifications | decision_factor |
| ------- | ------------- | -------------------- | ------------------ | --------------- |
| A       | 1             | 6                    | 3                  | 0.5             |
| B       | 1             | 6                    | 3                  | 0.5             |
| C       | 2             | 3                    | 2                  | 0.6666          |
| D       | 2             | 3                    | 2                  | 0.6666          |
| E       | 3             | 1                    | 1                  | 1               |
| F       | 1             | 6                    | 3                  | 0.5             |

![sample user vendor mapping ideal solution](images/user_vendor_mapping_ideal.svg?raw=true&sanitize=true "Sample user vendor mapping - ideal solution")

*Note:* If the number of users per restaurant is higher than `left_notifications` we take a random set of the targetable users limited by the total number of `left_notifications`.

The total number of notification (across all vendors) in this solution is **6**, which is the highest number theoretically possible 🎉.
