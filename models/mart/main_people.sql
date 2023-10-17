{{
    config(
        materialized="incremental",
        unique_key="person_key",
        incremental_strategy="merge",
    )
}}

with people as (select * from {{ ref("datastore_people") }})
select *
from people
