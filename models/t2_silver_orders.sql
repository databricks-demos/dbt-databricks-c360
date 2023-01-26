{{
 config(materialized = 'table', file_format = 'delta')
}}

--notes: order data cleaned and anonymized for analysis -- 
select
  cast(amount as int),
  `id` as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date
from field_eng_dbt_demo.dbt_c360.t1_bronze_orders