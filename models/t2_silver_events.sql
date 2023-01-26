{{
 config(materialized = 'table', file_format = 'delta')
}}

select 
  user_id,
  session_id,
  event_id,
  `date`,
  platform,
  action,
  url
from field_eng_dbt_demo.dbt_c360.t1_bronze_events