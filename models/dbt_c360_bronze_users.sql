{{
    config(
        materialized='streaming_table'
    )
}}
select
    *
from stream read_files('/Volumes/{{ var("catalog") }}/{{ var("schema") }}/users', format=>'json')