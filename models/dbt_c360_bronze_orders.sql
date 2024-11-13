{{
    config(
        materialized='streaming_table'
    )
}}
select
    *
from stream read_files('/Volumes//{{target.database}}/{{target.schema}}/raw_data/orders', format=>'json')
