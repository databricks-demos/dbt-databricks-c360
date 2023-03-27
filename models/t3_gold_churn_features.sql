{{
 config(materialized = 'table', file_format = 'delta')
}}

-- notes: final user table with all information for Analysis / ML -- 
with 
    -- block 1 -- 
    churn_orders_stats as 
    (select user_id, 
            count(*) as order_count, 
            sum(amount) as total_amount, 
            sum(item_count) as total_item, 
             max(creation_date) as last_transaction
      from {{ref('t2_silver_orders')}} 
      group by user_id
    ),  
    -- block 2 -- 
    churn_app_events_stats as 
    (
      select first(platform) as platform, 
             user_id, 
             count(*) as event_count, 
             count(distinct session_id) as session_count, 
             max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
       from {{ref('t2_silver_events')}} 
       group by user_id
    )

select *, 
       datediff(now(), creation_date) as days_since_creation,
       datediff(now(), last_activity_date) as days_since_last_activity,
       datediff(now(), last_event) as days_last_event
from {{ref('t2_silver_users')}} 
inner join churn_orders_stats using (user_id)
inner join churn_app_events_stats using (user_id)

         
         
         
         