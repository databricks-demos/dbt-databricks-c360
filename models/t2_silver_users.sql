{{
 config(materialized = 'table', file_format = 'delta')
}}

-- notes: user data cleaned and anonymized for analysis -- 
select
  id as user_id,
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  initcap(firstname) as firstname, 
  initcap(lastname) as lastname, 
  address, 
  canal, 
  country,
  cast(gender as int),
  cast(age_group as int), 
  cast(churn as int) as churn
from field_eng_dbt_demo.dbt_c360.t1_bronze_users