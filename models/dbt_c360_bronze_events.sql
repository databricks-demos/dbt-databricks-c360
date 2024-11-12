{{
    config(
        materialized='streaming_table'
    )
}}
select
    *
from stream read_files('/Volumes/{{ var("catalog") }}/{{ var("schema") }}/events', format=>'csv')
