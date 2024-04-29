{{
    config(
        materialized='streaming_table'
    )
}}
select
    *
from stream read_files('/dbdemos/dbt-retail/users', format=>'json')