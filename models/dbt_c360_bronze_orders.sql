{{
    config(
        materialized='streaming_table'
    )
}}
select
    *
from stream read_files('/Volumes/dbdemos/dbt-retail/orders', format=>'json')
