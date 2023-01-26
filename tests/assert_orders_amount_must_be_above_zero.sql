{{ config(store_failures = true) }}

-- notes: quarantine records and isolate them if the total sales amount is negative -- 
select 
 user_id,
 sum(amount) as total_amount 
from {{ref('t2_silver_orders')}}
group by 1 
having not (total_amount >= 0)