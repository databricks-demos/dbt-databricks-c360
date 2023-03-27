{{
 config(materialized = 'table', 
        file_format = 'delta')
}}

select 
  user_id,
  session_id,
  event_id,
  `date`,
  platform,
  action,
  url
from mchan_dbt_demo_db.t1_bronze_events